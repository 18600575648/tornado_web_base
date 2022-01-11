import aiomysql
import asyncio


# config配置文件
# mysql
pool: aiomysql.Pool
MYSQL_HOST = "81.71.137.167"
MYSQL_PORT = 3306
MYSQL_USER = 'root'
MYSQL_DB = 'mall_demo02'
MYSQL_PASSWD = 'LgDk%zMG0x!lfc@C'
MYSQL_CONNECTION_MAXSIZE = 2
MYSQL_POOL_RECYCLE = 60
'''
异步连接池
'''

async def get_mysql_pool(config):
    return await aiomysql.create_pool(host=config.MYSQL_HOST, port=config.MYSQL_PORT, user=config.MYSQL_USER,
                                      password=config.MYSQL_PASSWD,
                                      db=config.MYSQL_DB,
                                      loop=asyncio.get_event_loop(), autocommit=False,
                                      maxsize=config.MYSQL_CONNECTION_MAXSIZE,
                                      pool_recycle=config.MYSQL_POOL_RECYCLE)
task = [
    asyncio.ensure_future(get_mysql_pool())
]
loop = asyncio.get_event_loop()
loop.run_until_complete(asyncio.wait(task))
pool = [t.result() for t in task]
config.pool = pool[0]




# 使用连接池来操作mysql
async def execute(sql: str, args: Union[tuple, list] = None) -> (int, list):
    conn: aiomysql.Connection
    cursor: aiomysql.DictCursor
    rows: int
    res: list

    async with config.pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            try:
                rows = await cursor.execute(sql, args)
                res = await cursor.fetchall()
                return rows, res
            except Exception as e:
                await conn.ping()
                rows = await cursor.execute(sql, args)
                res = await cursor.fetchall()
                return rows, res

async def execute_with_commit(sql: str, args: Union[tuple, list] = None) -> int:
    conn: aiomysql.Connection
    cursor: aiomysql.Cursor
    rows: int
    print(sql)

    async with config.pool.acquire() as conn:
        async with conn.cursor(aiomysql.Cursor) as cursor:
            try:
                rows = await cursor.execute(sql, args)
                await conn.commit()
                return rows
            except Exception as e:
                await conn.ping()
                await cursor.execute(sql, args)
                await conn.commit()
                return conn.affected_rows()