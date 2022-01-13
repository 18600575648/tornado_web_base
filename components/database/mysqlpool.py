# copied from aiopg
# https://github.com/aio-libs/aiopg/blob/master/aiopg/pool.py

import asyncio
import collections
from multiprocessing.synchronize import Condition
import warnings

from aiomysql.connection import connect
from aiomysql.utils import (_PoolContextManager, _PoolConnectionContextManager,
                    _PoolAcquireContextManager)


def create_pool(minsize=1, maxsize=10, echo=False, pool_recycle=-1,
                loop=None, **kwargs):
    coro = _create_pool(minsize=minsize, maxsize=maxsize, echo=echo,
                        pool_recycle=pool_recycle, loop=loop, **kwargs)
    return _PoolContextManager(coro)


async def _create_pool(minsize=1, maxsize=10, echo=False, pool_recycle=-1,
                       loop=None, **kwargs):
    if loop is None:
        loop = asyncio.get_event_loop()

    pool = Pool(minsize=minsize, maxsize=maxsize, echo=echo,
                pool_recycle=pool_recycle, loop=loop, **kwargs)
    # if minsize > 0:
    #     async with pool.condition(**kwargs) as cond:
    #         await pool._fill_free_pool(False, cond, **kwargs)
    return pool


class Pool(asyncio.AbstractServer):
    """Connection pool"""

    def __init__(self, minsize, maxsize, echo, pool_recycle, loop, **kwargs):
        if minsize < 0:
            raise ValueError("minsize should be zero or greater")
        if maxsize < minsize:
            raise ValueError("maxsize should be not less than minsize")
        self._minsize = minsize
        self._maxsize = maxsize
        self._loop = loop
        self._acquiring = {}
        self._free = {} 
        self._cond = asyncio.Condition()
        self._config_cond = {}
        self._used = {}
        self._terminated = {}
        self._closing = False
        self._closed = False
        self._echo = echo
        self._recycle = pool_recycle

    @property
    def echo(self):
        return self._echo

    @property
    def minsize(self):
        return self._minsize

    @property
    def maxsize(self):
        return self._maxsize

    def emptyIfNone(s):
        return '' if not s else s

    def config_key(self, conn = None, **_kwargs):
        if conn:
            keys = ['' if not conn.host else f'host-{conn.host}', 
                            '' if not conn.port else f'port-{conn.port}', 
                            '' if not conn.db else f'db-{conn.db}', 
                            '' if not conn.user else f'user-{conn.user}' ]

            return ' '.join([item for item in keys if item])

        return ' '.join ([ f'{k}-{v}' for k,v in _kwargs.items() if k in ('host', 'port', 'db','user')]) 

    async def condition(self, key='', **_kwargs):
        cond = None
        async with self._cond:
            key = self.config_key(**_kwargs) if not key else key
            cond = self._config_cond.get(key, asyncio.Condition())
            self._config_cond[key] = cond
        return cond
    
    def acquiring(self, key='', **_kwargs):
        return self._acquiring.get(self.config_key(**_kwargs) if not key else key, 0)

    def add_acquiring(self, key='', **_kwargs):
        key = self.config_key(**_kwargs) if not key else key
        self._acquiring[key] = self._acquiring.get(key, 0) + 1

    def release_acquiring(self, key='', **_kwargs):
        key = self.config_key(**_kwargs) if not key else key
        if self._acquiring.get(key, 0) > 0:
            self._acquiring[key] = self._acquiring[key] - 1

    def used(self, key='', **_kwargs):
        key = self.config_key(**_kwargs) if not key else key
        self._used[key] = self._used.get(key, set())
        return self._used[key]

    def free(self, key = '', **_kwargs):
        key = self.config_key(**_kwargs) if not key else key
        self._free[key] = self._free.get(key, collections.deque(maxlen=self.maxsize))
        return self._free[key]

    def terminated(self, key = '', **_kwargs):
        key = self.config_key(**_kwargs) if not key else key
        self._terminated[key] = self._terminated.get(key, set())
        return self._terminated[key]

    def size(self, key='', **_kwargs):
        key = self.config_key(**_kwargs) if not key else key
        return len(self.free(key=key)) + len(self.used(key=key)) + self.acquiring(key=key)

    async def clear(self):
        """Close all free connections in all pools."""
        async with self._cond:
            for queue in self._free.values():
                while queue:
                    conn = queue.popleft()
                    await conn.ensure_closed()
            self._cond.notify()

    async def clear(self, key= '', **_kwargs):
        """Close all free connections in spedified pool."""
        _cond = self.condition(key=key, **_kwargs)
        async with _cond:
            queue = self.free(key=key, **_kwargs)
            while queue:
                conn = queue.popleft()
                await conn.ensure_closed()
            _cond.notify()

    def close(self):
        """Close pool.

        Mark all pool connections to be closed on getting back to pool.
        Closed pool doesn't allow to acquire new connections.
        """
        if self._closed:
            return
        self._closing = True

    def terminate(self):
        """Terminate pool.

        Close pool with instantly closing all acquired connections also.
        """

        self.close()
        for key, used in self._used.items():
            for conn in used:
                conn.close()
                self.terminated(key=key).add(conn)

        self._used.clear()

    async def wait_closed(self):
        """Wait for closing all pool's connections."""

        if self._closed:
            return
        if not self._closing:
            raise RuntimeError(".wait_closed() should be called "
                               "after .close()")

        for queue in self._free.values():
            while queue:
                conn = queue.popleft()
                conn.close()

        for key in self._free.keys():
            cond = await self.condition(key)
            async with cond:
                while self.size(key) > self.freesize(key):
                    await cond.wait()

        self._closed = True

    def acquire(self, **kwargs):
        """Acquire free connection from the pool."""
        coro = self._acquire(**kwargs)
        return _PoolAcquireContextManager(coro, self)

    async def _acquire(self, **_kwargs):
        if self._closing:
            raise RuntimeError("Cannot acquire connection after closing pool")
        cond = await self.condition(**_kwargs)
        async with cond:
            while True:
                await self._fill_free_pool(True, cond=cond, **_kwargs)
                free = self.free(**_kwargs)
                if free:
                    conn = free.popleft()
                    assert not conn.closed, conn
                    used = self.used(**_kwargs)
                    assert conn not in used, (conn, used)
                    used.add(conn)
                    return conn
                else:
                    await cond.wait()

    async def _fill_free_pool(self, override_min, cond, **_kwargs):
        # iterate over free connections and remove timeouted ones
        free = self.free(**_kwargs)
        free_size = len(free)
        n = 0

        # 清理到指定数据库的连接
        while n < free_size:
            conn = free[-1]

            if conn._reader.at_eof() or conn._reader.exception():
                free.pop()
                conn.close()

            elif (self._recycle > -1 and
                  self._loop.time() - conn.last_usage > self._recycle):
                free.pop()
                conn.close()

            else:
                free.rotate()
 
            n += 1

        # make sure the current config has meet the minsize
        while self.size(**_kwargs) < self.minsize:
            self.add_acquiring()
            try:
                conn = await connect(echo=self._echo, loop=self._loop,
                                     **_kwargs)
                # raise exception if pool is closing
                free.append(conn)
                cond.notify()
            finally:
                self.release_acquiring()
        if free:
            return

        if override_min and self.size(**_kwargs) < self.maxsize:
            self.add_acquiring()
            try:
                conn = await connect(echo=self._echo, loop=self._loop,
                                     **_kwargs)
                # raise exception if pool is closing
                free.append(conn)
                cond.notify()
            finally:
                self.release_acquiring()

    async def _wakeup(self):
        async with self._cond:
            self._cond.notify()

    def release(self, conn):
        """Release free connection back to the connection pool.

        This is **NOT** a coroutine.
        """
        fut = self._loop.create_future()
        fut.set_result(None)

        terminated = self.terminated(key=self.config_key(conn=conn))
        if conn in terminated:
            assert conn.closed, conn 
            terminated.remove(conn)
            return fut

        used = self.used(key=self.config_key(conn=conn))
        assert used, (conn, self._used)
        used.remove(conn)

        if not conn.closed:
            in_trans = conn.get_transaction_status()
            if in_trans:
                conn.close()
                return fut
            if self._closing:
                conn.close()
            else:
                self.free(self.config_key(conn)).append(conn)
            fut = self._loop.create_task(self._wakeup())
        return fut

    def get(self):
        warnings.warn("pool.get deprecated use pool.acquire instead",
                      DeprecationWarning,
                      stacklevel=2)
        return _PoolConnectionContextManager(self, None)

    def __enter__(self):
        raise RuntimeError(
            '"yield from" should be used as context manager expression')

    def __exit__(self, *args):
        # This must exist because __enter__ exists, even though that
        # always raises; that's how the with-statement works.
        pass  # pragma: nocover

    def __iter__(self):
        # This is not a coroutine.  It is meant to enable the idiom:
        #
        #     with (yield from pool) as conn:
        #         <block>
        #
        # as an alternative to:
        #
        #     conn = yield from pool.acquire()
        #     try:
        #         <block>
        #     finally:
        #         conn.release()
        conn = yield from self.acquire()
        return _PoolConnectionContextManager(self, conn)

    def __await__(self):
        msg = "with await pool as conn deprecated, use" \
              "async with pool.acquire() as conn instead"
        warnings.warn(msg, DeprecationWarning, stacklevel=2)
        conn = yield from self.acquire()
        return _PoolConnectionContextManager(self, conn)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()
        await self.wait_closed()
