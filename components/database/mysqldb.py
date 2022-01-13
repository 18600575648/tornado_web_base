#!/usr/bin/python
# -*- coding: utf-8 -*-
from collections import defaultdict
import re
import ujson as json
import time
import aiomysql
import asyncio
from components.database.mysqlpool import create_pool, Pool
from components.utils.log import ExtraLog
from tornado.log import app_log
from components.utils.misc import escape_string

class DBProxyException(Exception):
    pass

class DBProxyRuntimeException(DBProxyException):
    def __init__(self, message):
        super(DBProxyRuntimeException, self).__init__(message)
        self.message = message

    def __str__(self):
        return "DBProxyRuntimeException: %s" % self.message

class DBDisconnect(DBProxyException):
    def __init__(self, message, error_code):
        super(DBDisconnect, self).__init__(message)
        self.error_code = error_code
        self.message = message

    def __str__(self):
        return "MSSQLDBDisconnect: error_code:%d %s" % (self.error_code, self.message)

class MySqlDB(object):
    """The base class for loma DB handle classes.
    This base class will define the interface and realize the common function.
    Current support two DB libraries: pymssql(FreeTDS), pyodbc(Microsoft ODBC Driver 11 for Linux)."""

    CONN_DISCONNECT_ERR_CODE_LIST = (20003,20004,)
    LOCK_RETRY_ERR_CODE_LIST = (1205,1222)
    """ Need to retry when the list code occurs.
    http://technet.microsoft.com/en-us/library/aa258770(v=SQL.80).aspx
    1205
    Transaction (Process ID %d) was deadlocked on {%Z} resources with another process and has been chosen as the deadlock victim.
    Rerun the transaction.

    1222
    An error occurred in Service Broker internal activation while trying to scan the user queue 'Queue_name' for its status.
    Error: 1222, State: 51. Lock request time out period exceeded. This is an informational message only. No user action is required
    """

    """Default print executed SQL length."""
    SQL_PRINT_LEN = 500
    GL_opendb_conns = 0

    @staticmethod
    def set_retry_err_code_list(err_code_list=None):
        """If the exception error code is in err_code_list the action will be retried.
        The latest value will be retruned.
        """
        if not err_code_list:
            return MySqlDB.LOCK_RETRY_ERR_CODE_LIST
        MySqlDB.LOCK_RETRY_ERR_CODE_LIST = err_code_list
        return MySqlDB.LOCK_RETRY_ERR_CODE_LIST

    @staticmethod
    def set_sql_print_len(print_len=0):
        """The length of the SQL to be printed.
        The latest value will be return.
        """
        if print_len <= 0:
            return MySqlDB.SQL_PRINT_LEN
        MySqlDB.SQL_PRINT_LEN = print_len
        return MySqlDB.SQL_PRINT_LEN

    @staticmethod
    def parse_sqlalchemy_dburl(db_uri, query_timeout=600, login_timeout=40):
        """Analyze the `db_uri` into configure dictionary which in sqlalchemy style."""
        db_dict = defaultdict(str,{})
        if not db_uri:
            return db_dict
        tmp_list = db_uri.split(':')
        tmp1_list = tmp_list[2].split('@')
        db_dict['username'] = tmp_list[1][2:]
        db_dict['password'] = tmp1_list[0]
        db_dict['host'] = tmp1_list[1]
        tmp2_list = tmp_list[3].split('/')
        db_dict['port'] = tmp2_list[0]
        db_dict['db'] = tmp2_list[1]
        db_dict['charset'] = 'utf8mb4'
        db_dict['timeout'] = query_timeout
        db_dict['login_timeout'] = login_timeout
        return db_dict

    @staticmethod
    def mssql_escape_string(value):
        if not value:
            return value
        if isinstance(value, (str,bytes)):
            if value.find("''")>0:
                return value
            return value.replace("'", "''")
        return value

    @staticmethod
    def mysql_escape_string(value,escape_quot=False):
        if not value:
            return value
        if isinstance(value, (str,bytes)):
            if not escape_quot and value.find("''")>0:
                return escape_string(value)
            return escape_string(value.replace("'", "''"))
        return value

    @staticmethod
    def val2SqlVal(val):
        type_name = type(val).__name__
        if val is None:
            return "null"
        elif type_name in ('bool','integer','int'):
            return '%d' % val
        elif type_name in ('float','real','double'):
            return "%f" % val
        escape_str = val
        if type_name == 'dict':
            escape_str = json.dumps(val)

        if type_name in ('str', 'unicode', 'dict'):            
            escape_str = MySqlDB.mysql_escape_string(escape_str)
            return "'%s'" % escape_str
        return "'%s'" % str(escape_str)

    @staticmethod
    def composeColValueSql(cols, vals, type='insert', makeupnull=False):
        hold_list = []
        if type == 'insert':
            for col in cols:
                if col in vals:
                    hold_list.append(MySqlDB.val2SqlVal(vals[col]))
                elif makeupnull:
                    hold_list.append(MySqlDB.val2SqlVal(None))
        if type == 'update':
            for col in cols:
                if col in vals:
                    hold_list.append("%s=%s" % (col, MySqlDB.val2SqlVal(vals[col])))
                elif makeupnull:
                    hold_list.append("%s=%s" % (col, MySqlDB.val2SqlVal(None)))
        return ",".join(hold_list)

    _conn_pool = None
    _condition = asyncio.Condition()
    async def _ensure_pool(self):
        """获得或者初始化连接池"""
        async with MySqlDB._condition:
            if MySqlDB._conn_pool:
                return MySqlDB._conn_pool
            if not self._config:
                raise DBProxyRuntimeException('not provide correct config / uri to create db connection')
            # config: host/port/user/password/db/charset/autocommit
            self._config['charset'] = self._config.get('charset', 'utf8mb4')
            self._config['autocommit'] = True if not 'autocommit' in self._config else self._config['autocommit']
            # 创建连接池，并确保有最小数量（缺省为1）的可用连接
            MySqlDB._conn_pool = await create_pool(
                                        maxsize=self._config.get('pool_max_size', 10),
                                        minsize=self._config.get('pool_min_size', 1),
                                        pool_recycle=self._config.get('pool_recycle_time',-1),
                                        **self._config)
            return MySqlDB._conn_pool

    async def get_conn(self):
        if not self._conn_pool:
            self._conn_pool = await self._ensure_pool()

        # 配置没有改变，使用已有的连接
        if self._conn and not self._conn.closed and self._conn_pool.config_key(self._conn) == self._conn_pool.config_key(**self._config):
            return self._conn

        # 更换连接，归还到连接池
        if self._conn:
            self._conn_pool.release(self._conn)
            self._conn = None
        
        # 获取新的连接
        self._conn = await self._conn_pool.acquire(**self._config)
        return self._conn

    def __init__(self, config={}, uri=""):
        """`uri`: the connect string to the Configure Server Database.
        pymssql example: mssql+pymssql://test:test@ipaloma1inner.sqlserver.rds.aliyuncs.com:3221/op
        pyodbc example : Driver={ODBC Driver 11 for SQL Server};Server=192.168.1.122,3389;Database=master;Uid=test;Pwd=test;
        ATTENTION: pyodbc connect string default ues the 'master' database, you shuold call 'USE [DATABASE]' to change the target database.
                Do NOT use the target database directly else the ODBC driver will throw out 'Segmentation fault'
                when you try make second same database connection, specailly when SQL Server use 'mirror' backup method.
        `config`: dict version of the connection uri: host/port/user/password/database/charset/autocommit
        """        
        self.config(config, uri)
        self.log = ExtraLog(self, app_log)
        self._conn = None
        self._conn_pool = None        

    def config(self, config={}, uri=""):
        """`config`: json for connection config: host/port/user/password/db
        `uri`: odbc connection string
        """
        self._config = config if config else MySqlDB.parse_sqlalchemy_dburl(uri)
        
    def change_db(self, db):
        """`db` the next db to be used"""
        self._config['db'] = db

    def __del__(self):
        pass

    def format_params(self, param_list):
        return [ MySqlDB.val2SqlVal(parameter, None) for parameter in param_list ]

    async def process_exception(self, conn, e):
        if await conn.ping():
            raise DBDisconnect("DB链接已经断开, %s" % (e.message), e.number)
        else:
            raise DBProxyRuntimeException("查询失败(ERROR:%s)" % e)

    async def exec_sql(self, sql, params=None, commit=False):
        """Execute the SQL without fetch the result"""
        affect_rows = 0
        
        start_point = time.time()
        conn = await self.get_conn()
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            try:
                affect_rows = await cursor.execute(sql, params)
                if commit:
                    await conn.commit()
            except Exception as e:
                self.log.exception('exec_modify_sql_no_fetch error:%s %s\tSQL TIME USAGE:%.3fs'
                                    %(sql, e, time.time()-start_point))
                await self.process_exception(conn,e)
            self.log.info('%s\tSQL TIME USAGE:%.3fs affect_rows:%d' % (sql[:MySqlDB.SQL_PRINT_LEN], time.time()-start_point, affect_rows))
        return

    async def exec_select(self, sql, fetch_result=True):
        """Execute a SQL and return a list contains the result.
        `sql`: sql statement(s)
        `fetch_result`: whether fetch the result set if False the empty list will be return.
        the row will be rebuild into dictionary;        
        """        
        fetch_object_method_time_usage, execute_object_method_time_usage, start_point = 0, 0, time.time()
        
        conn = await self.get_conn()
        async with conn.cursor(aiomysql.DictCursor) as cursor:                
            rc = []
            try:
                await cursor.execute(sql)                    
                execute_object_method_time_usage = time.time() - start_point
                start_point = time.time()
                
                if fetch_result:
                    # rc = cursor.fetchall()
                    rc = []
                    while True:
                        many = await cursor.fetchmany(1000)
                        if not many:
                            break
                        rc.extend(many)
                        await asyncio.sleep(0)
                    if len(rc)>20000:
                        self.log.info("allow_large_data--Surch result count more than 20000")
                fetch_object_method_time_usage = time.time() - start_point
            except Exception as e:
                self.log.exception('exec_select error:%s %s\tSQL TIME USAGE:%.3fs'
                                    %(sql, e, time.time()-start_point))
                await self.process_exception(conn, e)

        self.log.info('%s\tTIME USAGE: SQL Execute:%.3fs Fetch Result:%.3fs'
            %(sql[:MySqlDB.SQL_PRINT_LEN], execute_object_method_time_usage, fetch_object_method_time_usage))
        return rc

    async def call_procedure(self, proc_name, parameters=[], fetch_result=True,allow_large_data=True):
        """Call a store procedure and return a list contains the results.
        `as_dict_flag`: the row will be rebuild into dictionary;
        `fetch_result`: whether fetch the result set if False the empty list will be return.
        """
        fetch_object_method_time_usage, execute_object_method_time_usage, start_point = 0, 0, time.time()
        async with (await self.get_conn()) as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:        
                rc = []
                try:
                    self.change_database(cursor)
                    if allow_large_data:
                        await conn.set_limit_count(0)
                    else:
                        await conn.set_limit_count(50000)
                    result_args = await cursor.callproc(proc_name, parameters)
                    if await conn.get_out_of_limit_count_status():
                        raise DBProxyRuntimeException("allow_large_data--Surch result count more than 50000")
                    self.log.warning("%s result_args:%s" % (proc_name, result_args))
                    execute_object_method_time_usage = time.time() - start_point
                    start_point = time.time()
                    rc = []
                    while True:
                        many = await cursor.fetchmany(1000)
                        if not many:
                            break
                        rc.extend(many)
                        await asyncio.sleep(0)

                    if len(rc)>20000:
                        self.log.info("allow_large_data--Surch result count more than 20000")
                    if len(rc) == 1:
                        rc = rc[0]
                    if not fetch_result:
                        rc = []
                    fetch_object_method_time_usage = time.time() - start_point                    
                except Exception as e:
                    self.log.exception('callproc error:%s %s\tSQL TIME USAGE:%.3fs'
                                    %(proc_name, e, time.time()-start_point))
                    await self.process_exception(conn, e)

        self.log.info('exec %s %s\tTIME USAGE: SQL Execute:%.3fs Fetch Result:%.3fs'
            %(proc_name, parameters, execute_object_method_time_usage, fetch_object_method_time_usage))
        return rc

    async def executemany(self, oper_sql, sql_of_params, commit=False):
        """Call executemany() function.
        `as_dict_flag`: the row will be rebuild into dictionary;
        `fetch_result`: whether fetch the result set if False the empty list will be return.
        """
        execute_object_method_time_usage, start_point = 0, time.time()
        async with  (await self.get_conn()) as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:        
                try:
                    self.change_database(cursor)
                    await cursor.executemany(oper_sql, sql_of_params)
                    if commit:
                        await conn.commit()
                    execute_object_method_time_usage = time.time() - start_point                    
                except Exception as e:
                    
                    self.log.exception('executemany error:%s params:%s %s\tSQL TIME USAGE:%.3fs'
                                    %(oper_sql, sql_of_params, e, time.time()-start_point))
                    await self.process_exception(conn, e)

        self.log.info('%s params len=%d\tTIME USAGE: SQL Execute:%.3fs' %
            (oper_sql, len(sql_of_params), execute_object_method_time_usage))
