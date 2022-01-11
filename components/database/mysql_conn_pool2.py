#!/usr/bin/python
# -*- coding: utf-8 -*-
import re
import sys
from aiomysql.connection import connect
import ujson as json
import time
import aiomysql
import asyncio

import six

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

class DBMySql(object):
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
    conn_pool = None

    @staticmethod
    def set_retry_err_code_list(err_code_list=None):
        """If the exception error code is in err_code_list the action will be retried.
        The latest value will be retruned.
        """
        if not err_code_list:
            return DBProxyBase.LOCK_RETRY_ERR_CODE_LIST
        DBProxyBase.LOCK_RETRY_ERR_CODE_LIST = err_code_list
        return DBProxyBase.LOCK_RETRY_ERR_CODE_LIST

    @staticmethod
    def set_sql_print_len(print_len=0):
        """The length of the SQL to be printed.
        The latest value will be return.
        """
        if print_len <= 0:
            return DBProxyBase.SQL_PRINT_LEN
        DBProxyBase.SQL_PRINT_LEN = print_len
        return DBProxyBase.SQL_PRINT_LEN

    @staticmethod
    def parse_sqlalchemy_dburl(db_uri, query_timeout=600, login_timeout=40):
        """Analyze the `db_uri` into configure dictionary which in sqlalchemy style."""
        db_dict = {}
        tmp_list = db_uri.split(':')
        tmp1_list = tmp_list[2].split('@')
        db_dict['user'] = tmp_list[1][2:]
        db_dict['password'] = tmp1_list[0]
        db_dict['server'] = tmp1_list[1]
        tmp2_list = tmp_list[3].split('/')
        db_dict['port'] = tmp2_list[0]
        db_dict['database'] = tmp2_list[1]
        db_dict['charset'] = 'UTF-8'
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
            escape_str = DBProxyBase.mysql_escape_string(escape_str)
            return "'%s'" % escape_str
        return "'%s'" % str(escape_str)

    @staticmethod
    def composeColValueSql(cols, vals, type='insert', makeupnull=False):
        hold_list = []
        if type == 'insert':
            for col in cols:
                if col in vals:
                    hold_list.append(DBProxyBase.val2SqlVal(vals[col]))
                elif makeupnull:
                    hold_list.append(DBProxyBase.val2SqlVal(None))
        if type == 'update':
            for col in cols:
                if col in vals:
                    hold_list.append("%s=%s" % (col, DBProxyBase.val2SqlVal(vals[col])))
                elif makeupnull:
                    hold_list.append("%s=%s" % (col, DBProxyBase.val2SqlVal(None)))
        return ",".join(hold_list)

    conn_pool = None
    @staticmethod
    async def init_conn_pool(config):
        result = await aiomysql.create_pool(host=config['host'], port=config['port'], user=config['user'],
                                      password=config['password'],
                                      db=config['database'],
                                      loop=asyncio.get_event_loop(), autocommit=True,
                                      maxsize=config['pool_max_size'],
                                      pool_recycle=config['pool_recycle'],
                                      charset='utf8mb4')
        global conn_pool
        conn_pool = result[0]
        

    @staticmethod
    def sql_readonly_check(checksql):
        '''检查sql字符串中是否包含修改行为的关键字'''
        keys = ('insert','update','delete','exec','call','set','shutdown','setuser','write',
            'alter','execute','restore','grant','revoke','writetext','lock','rename',
            'commit','begin','create','updatetext','drop','dump','truncate','replace')
        contain_keys = []
        for key in keys:
            if re.search(key+r'\s+', checksql, re.I):
                contain_keys.append(key)
        return contain_keys

    def check_nolock(self, sql):
        # with(nolock)
        final_sql = re.search(r'with\s*\(\s*nolock\s*\)',sql,flags=re.IGNORECASE)
        return final_sql!=None

    def __init__(self, db_name):
        """`config_db_uri`: the connect string to the Configure Server Database.
        pymssql example: mssql+pymssql://test:test@ipaloma1inner.sqlserver.rds.aliyuncs.com:3221/op
        pyodbc example : Driver={ODBC Driver 11 for SQL Server};Server=192.168.1.122,3389;Database=master;Uid=test;Pwd=test;
        ATTENTION: pyodbc connect string default ues the 'master' database, you shuold call 'USE [DATABASE]' to change the target database.
                Do NOT use the target database directly else the ODBC driver will throw out 'Segmentation fault'
                when you try make second same database connection, specailly when SQL Server use 'mirror' backup method.
        """        
        self._db_name = db_name
        self.log = ExtraLog(self, app_log)

    def __del__(self):
        pass

    def format_params(self, param_list):
        return [ DBProxyBase.val2SqlVal(parameter, None) for parameter in param_list ]

    async def process_exception(self, conn, e):
        if await conn.ping():
            raise DBDisconnect("DB链接已经断开, %s" % (e.message), e.number)
        else:
            raise DBProxyRuntimeException("查询失败(ERROR:%s)" % e)

    async def exec_modify_sql_no_fetch(self, sql):
        await self.exec_modify_sql_no_fetch(sql, params=None)

    async def change_database(self, cursor):
        sql = 'use `%s`;' % self._db_name
        await cursor.execute(sql)
        
    async def exec_modify_sql_no_fetch(self, sql, params=None):
        """Execute the SQL without fetch the result"""
        affect_rows = 0
        
        start_point = time.time()
        async with self.get_db_conn().acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                try:
                    self.change_database(cursor)
                    affect_rows = cursor.execute(sql, params)
                except Exception as e:
                    self.log.exception('exec_modify_sql_no_fetch error:%s %s\tSQL TIME USAGE:%.3fs'
                                        %(sql, e, time.time()-start_point))
                    await self.process_exception(conn,e)
                self.log.info('%s\tSQL TIME USAGE:%.3fs affect_rows:%d' % (sql[:DBProxyBase.SQL_PRINT_LEN], time.time()-start_point, affect_rows))
        return

    async def exec_select(self, sql, fetch_result=True, allow_large_data=True):
        """Execute a SQL and return a list contains the result.
        `as_dict_flag`: the row will be rebuild into dictionary;
        `fetch_result`: whether fetch the result set if False the empty list will be return.
        """        
        sql = self.sqlserver2mysql(sql)
        
        fetch_object_method_time_usage, execute_object_method_time_usage, start_point = 0, 0, time.time()
        
        async with self.get_db_conn().acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:                
                rc = []
                try:
                    self.change_database(cursor)
                    if allow_large_data:
                        await conn.set_limit_count(0)
                    else:
                        await conn.set_limit_count(50000)
                    cursor.execute(sql)
                    if await conn.get_out_of_limit_count_status():
                        raise DBProxyRuntimeException("allow_large_data--Surch result count more than 50000")
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
            %(sql[:DBProxyBase.SQL_PRINT_LEN], execute_object_method_time_usage, fetch_object_method_time_usage))
        return rc

    async def call_procedure(self, proc_name, parameters=[], fetch_result=True,allow_large_data=True):
        """Call a store procedure and return a list contains the results.
        `as_dict_flag`: the row will be rebuild into dictionary;
        `fetch_result`: whether fetch the result set if False the empty list will be return.
        """
        fetch_object_method_time_usage, execute_object_method_time_usage, start_point = 0, 0, time.time()
        async with self.get_db_conn().acquire() as conn:
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

    async def executemany(self, oper_sql, sql_of_params):
        """Call executemany() function.
        `as_dict_flag`: the row will be rebuild into dictionary;
        `fetch_result`: whether fetch the result set if False the empty list will be return.
        """
        execute_object_method_time_usage, start_point = 0, time.time()
        async with self.get_db_conn().acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:        
                try:
                    self.change_database(cursor)
                    await cursor.executemany(oper_sql, sql_of_params)
                    execute_object_method_time_usage = time.time() - start_point                    
                except Exception as e:
                    
                    self.log.exception('executemany error:%s params:%s %s\tSQL TIME USAGE:%.3fs'
                                    %(oper_sql, sql_of_params, e, time.time()-start_point))
                    await self.process_exception(conn, e)

        self.log.info('%s params len=%d\tTIME USAGE: SQL Execute:%.3fs' %
            (oper_sql, len(sql_of_params), execute_object_method_time_usage))
