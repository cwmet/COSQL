import asyncio
import logging
import sqlite3
from functools import partial
from pathlib import Path
from queue import Empty, Queue, SimpleQueue
from threading import Thread
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Generator,
    Iterable,
    Literal,
    Optional,
    Tuple,
    Type,
    Union,
)

from warnings import warn
from .context import contextmanager
from .cursor import Cursor

__all__ = ["connect", "Connection", "Cursor"]
LOG = logging.getLogger("aiosqlite")
IsolationLevel = Optional[Literal["DEFERRED", "IMMEDIATE", "EXCLUSIVE"]]

def set_result(fut: asyncio.Future, result: Any) -> None:
    if not fut.done():
        fut.set_result(result)

def set_exception(fut: asyncio.Future, e: BaseException) -> None:
    if not fut.done():
        fut.set_exception(e)

_STOP_RUNNING_SENTINEL = object()

class Connection(Thread):
    def __init__(self, connector: Callable[[], sqlite3.Connection], iter_chunk_size: int, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        super().__init__()
        self._running = True
        self._connection: Optional[sqlite3.Connection] = None
        self._connector = connector
        self._tx: SimpleQueue[Tuple[asyncio.Future, Callable[[], Any]]] = SimpleQueue()
        self._iter_chunk_size = iter_chunk_size

        if loop is not None:
            warn(
                "aiosqlite.Connection no longer uses the `loop` parameter",
                DeprecationWarning,
            )

    def _stop_running(self):
        self._running = False
        self._tx.put_nowait(_STOP_RUNNING_SENTINEL)

    @property
    def _conn(self) -> sqlite3.Connection:
        if self._connection is None:
            raise ValueError("no active connection")

        return self._connection

    def run(self) -> None:
        while True:
            tx_item = self._tx.get()
            if tx_item is _STOP_RUNNING_SENTINEL:
                break
            future, function = tx_item

            try:
                LOG.debug("executing %s", function)
                result = function()
                LOG.debug("operation %s completed", function)
                future.get_loop().call_soon_threadsafe(set_result, future, result)
            except BaseException as e:
                LOG.debug("returning exception %s", e)
                future.get_loop().call_soon_threadsafe(set_exception, future, e)

    async def _execute(self, fn, *args, **kwargs):
        if not self._running or not self._connection:
            raise ValueError("Connection closed")

        function = partial(fn, *args, **kwargs)
        future = asyncio.get_event_loop().create_future()

        self._tx.put_nowait((future, function))

        return await future

    async def execute(
        self,
        sql: str,
        parameters: Optional[Iterable[Any]] = None,
        fetch: Literal[0, 1, 2] = 0,
    ) -> Union[sqlite3.Row, Iterable[sqlite3.Row], None]:
        if parameters is None:
            parameters = []

        def query_execution():
            cursor = self._conn.execute(sql, parameters)
            if fetch == 1:
                return cursor.fetchone()
            elif fetch == 2:
                return cursor.fetchall()
            return cursor

        return await self._execute(query_execution)

    async def executemany(self, sql: str, parameters: Iterable[Iterable[Any]]) -> None:
        await self._execute(self._conn.executemany, sql, parameters)

    async def executescript(self, sql_script: str) -> None:
        await self._execute(self._conn.executescript, sql_script)

    async def execute_insert(self, sql: str, parameters: Optional[Iterable[Any]] = None) -> Optional[sqlite3.Row]:
        if parameters is None:
            parameters = []

        def insert_execution():
            cursor = self._conn.execute(sql, parameters)
            cursor.execute("SELECT last_insert_rowid()")
            return cursor.fetchone()

        return await self._execute(insert_execution)

    async def _connect(self) -> "Connection":
        if self._connection is None:
            try:
                future = asyncio.get_event_loop().create_future()
                self._tx.put_nowait((future, self._connector))
                self._connection = await future
            except Exception:
                self._stop_running()
                self._connection = None
                raise

        return self

    def __await__(self) -> Generator[Any, None, "Connection"]:
        self.start()
        return self._connect().__await__()

    async def __aenter__(self) -> "Connection":
        return await self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    @contextmanager
    async def cursor(self) -> Cursor:
        return Cursor(self, await self._execute(self._conn.cursor))

    async def commit(self) -> None:
        await self._execute(self._conn.commit)

    async def rollback(self) -> None:
        await self._execute(self._conn.rollback)

    async def close(self) -> None:
        if self._connection is None:
            return
        try:
            await self._execute(self._conn.close)
        except Exception:
            LOG.info("exception occurred while closing connection")
            raise
        finally:
            self._stop_running()
            self._connection = None

    @contextmanager
    async def execute(self, sql: str, parameters: Optional[Iterable[Any]] = None) -> Cursor:
        if parameters is None:
            parameters = []
        cursor = await self._execute(self._conn.execute, sql, parameters)
        return Cursor(self, cursor)

    @contextmanager
    async def execute_insert(self, sql: str, parameters: Optional[Iterable[Any]] = None) -> Optional[sqlite3.Row]:
        if parameters is None:
            parameters = []
        return await self._execute(self._execute_insert, sql, parameters)

    @contextmanager
    async def execute_fetchone(self, sql: str, parameters: Optional[Iterable[Any]] = None) -> Iterable[sqlite3.Row]:
        if parameters is None:
            parameters = []
        return await self._execute(self._execute_fetchone, sql, parameters)
    
    @contextmanager
    async def execute_fetchall(self, sql: str, parameters: Optional[Iterable[Any]] = None) -> Iterable[sqlite3.Row]:
        if parameters is None:
            parameters = []
        return await self._execute(self._execute_fetchall, sql, parameters)
    
    @contextmanager
    async def executemany(self, sql: str, parameters: Iterable[Iterable[Any]]) -> Cursor:
        cursor = await self._execute(self._conn.executemany, sql, parameters)
        return Cursor(self, cursor)

    @contextmanager
    async def executescript(self, sql_script: str) -> Cursor:
        cursor = await self._execute(self._conn.executescript, sql_script)
        return Cursor(self, cursor)

    async def interrupt(self) -> None:
        return self._conn.interrupt()

    async def create_function(self, name: str, num_params: int, func: Callable, deterministic: bool = False) -> None:
        await self._execute(
            self._conn.create_function,
            name,
            num_params,
            func,
            deterministic=deterministic,
        )

    @property
    def in_transaction(self) -> bool:
        return self._conn.in_transaction

    @property
    def isolation_level(self) -> Optional[str]:
        return self._conn.isolation_level

    @isolation_level.setter
    def isolation_level(self, value: IsolationLevel) -> None:
        self._conn.isolation_level = value

    @property
    def row_factory(self) -> Optional[Type]:
        return self._conn.row_factory

    @row_factory.setter
    def row_factory(self, factory: Optional[Type]) -> None:
        self._conn.row_factory = factory

    @property
    def text_factory(self) -> Callable[[bytes], Any]:
        return self._conn.text_factory

    @text_factory.setter
    def text_factory(self, factory: Callable[[bytes], Any]) -> None:
        self._conn.text_factory = factory

    @property
    def total_changes(self) -> int:
        return self._conn.total_changes

    async def enable_load_extension(self, value: bool) -> None:
        await self._execute(self._conn.enable_load_extension, value)

    async def load_extension(self, path: str):
        await self._execute(self._conn.load_extension, path)

    async def set_progress_handler(self, handler: Callable[[], Optional[int]], n: int) -> None:
        await self._execute(self._conn.set_progress_handler, handler, n)

    async def set_trace_callback(self, handler: Callable) -> None:
        await self._execute(self._conn.set_trace_callback, handler)

    async def iterdump(self) -> AsyncIterator[str]:
        dump_queue: Queue = Queue()

        def dumper():
            try:
                for line in self._conn.iterdump():
                    dump_queue.put_nowait(line)
                dump_queue.put_nowait(None)

            except Exception:
                LOG.exception("exception while dumping db")
                dump_queue.put_nowait(None)
                raise

        fut = self._execute(dumper)
        task = asyncio.ensure_future(fut)

        while True:
            try:
                line: Optional[str] = dump_queue.get_nowait()
                if line is None:
                    break
                yield line

            except Empty:
                if task.done():
                    LOG.warning("iterdump completed unexpectedly")
                    break

                await asyncio.sleep(0.01)

        await task

    async def backup(
        self,
        target: Union["Connection", sqlite3.Connection],
        *,
        pages: int = 0,
        progress: Optional[Callable[[int, int, int], None]] = None,
        name: str = "main",
        sleep: float = 0.250,
    ) -> None:
        
        if isinstance(target, Connection):
            target = target._conn

        await self._execute(
            self._conn.backup,
            target,
            pages=pages,
            progress=progress,
            name=name,
            sleep=sleep,
        )

def connect(
    database: Union[str, Path], 
    *, 
    iter_chunk_size=64, 
    loop: Optional[asyncio.AbstractEventLoop] = None, 
    ac: bool = True,
    **kwargs: Any
) -> Connection:
    if loop is not None:
        warn(
            "aiosqlite.connect() no longer uses the `loop` parameter",
            DeprecationWarning,
        )

    def connector() -> sqlite3.Connection:
        if isinstance(database, str):
            loc = database
        elif isinstance(database, bytes):
            loc = database.decode("utf-8")
        else:
            loc = str(database)

        return sqlite3.connect(loc, **kwargs, autocommit=ac)

    return Connection(connector, iter_chunk_size)