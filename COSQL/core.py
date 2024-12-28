# Copyright 2024 Comet
# Licensed under the MIT license

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
    """설정되지 않은 항목에 대해 차후 결과를 설정합니다."""
    if not fut.done():
        fut.set_result(result)


def set_exception(fut: asyncio.Future, e: BaseException) -> None:
    """설정되지 않은 항목에 대해 차후 예외를 설정합니다."""
    if not fut.done():
        fut.set_exception(e)

_STOP_RUNNING_SENTINEL = object()

class Connection(Thread):
    def __init__(
        self,
        connector: Callable[[], sqlite3.Connection],
        iter_chunk_size: int,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
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

    def _execute_insert(self, sql: str, parameters: Any) -> Optional[sqlite3.Row]:
        cursor = self._conn.execute(sql, parameters)
        cursor.execute("SELECT last_insert_rowid()")
        return cursor.fetchone()

    def _execute_FetchAll(self, sql: str, parameters: Any) -> Iterable[sqlite3.Row]:
        cursor = self._conn.execute(sql, parameters)
        return cursor.fetchall()
    
    def _execute_Fetch(self, sql: str, parameters: Any) -> Iterable[sqlite3.Row]:
        cursor = self._conn.execute(sql, parameters)
        return cursor.fetchone()

    def run(self) -> None:
        """
        별도 스레드에서 함수 호출을 실행합니다.
        """
        while True: # 모든 대기열 항목이 처리될 때까지 계속 실행
            tx_item = self._tx.get()
            if tx_item is _STOP_RUNNING_SENTINEL:
                break
            future, function = tx_item

            try:
                LOG.debug("executing %s", function)
                result = function()
                LOG.debug("operation %s completed", function)
                future.get_loop().call_soon_threadsafe(set_result, future, result)
            except BaseException as e:  # noqa B036
                LOG.debug("returning exception %s", e)
                future.get_loop().call_soon_threadsafe(set_exception, future, e)

    async def _execute(self, fn, *args, **kwargs):
        """인수를 포함해 함수를 실행 대기열에 넣습니다."""
        if not self._running or not self._connection:
            raise ValueError("Connection closed")

        function = partial(fn, *args, **kwargs)
        future = asyncio.get_event_loop().create_future()

        self._tx.put_nowait((future, function))

        return await future

    async def _connect(self) -> "Connection":
        """실제 sqlite 데이터베이스에 연결합니다."""
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
        """SQLite3 커서 객체를 래핑하는 COSQL 커서를 만듭니다."""
        return Cursor(self, await self._execute(self._conn.cursor))

    async def commit(self) -> None:
        """현재 트랜잭션을 적용합니다."""
        await self._execute(self._conn.commit)

    async def rollback(self) -> None:
        """현재 트랜잭션을 되돌립니다."""
        await self._execute(self._conn.rollback)

    async def close(self) -> None:
        """대기 중인 쿼리/커서를 완료하고 연결을 닫습니다."""

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
    async def execute(
        self, sql: str, parameters: Optional[Iterable[Any]] = None
    ) -> Cursor:
        """커서를 생성하고 주어진 쿼리를 실행합니다."""
        if parameters is None:
            parameters = []
        cursor = await self._execute(self._conn.execute, sql, parameters)
        return Cursor(self, cursor)

    @contextmanager
    async def execute_insert(
        self, sql: str, parameters: Optional[Iterable[Any]] = None
    ) -> Optional[sqlite3.Row]:
        """마지막으로 삽입된 ROW를 가져옵니다."""
        if parameters is None:
            parameters = []
        return await self._execute(self._execute_insert, sql, parameters)

    @contextmanager
    async def execute_Fetch(
        self, sql: str, parameters: Optional[Iterable[Any]] = None
    ) -> Iterable[sqlite3.Row]:
        """쿼리를 실행하고 하나의 데이터를 반환합니다."""
        if parameters is None:
            parameters = []
        return await self._execute(self._execute_Fetch, sql, parameters)
    
    @contextmanager
    async def execute_FetchAll(
        self, sql: str, parameters: Optional[Iterable[Any]] = None
    ) -> Iterable[sqlite3.Row]:
        """쿼리를 실행하고 모든 데이터를 반환합니다."""
        if parameters is None:
            parameters = []
        return await self._execute(self._execute_FetchAll, sql, parameters)
    
    @contextmanager
    async def executemany(
        self, sql: str, parameters: Iterable[Iterable[Any]]
    ) -> Cursor:
        """커서를 생성하고 주어진 다중 쿼리를 실행합니다."""
        cursor = await self._execute(self._conn.executemany, sql, parameters)
        return Cursor(self, cursor)

    @contextmanager
    async def executescript(self, sql_script: str) -> Cursor:
        """커서를 생성하고 사용자 스크립트를 실행합니다."""
        cursor = await self._execute(self._conn.executescript, sql_script)
        return Cursor(self, cursor)

    async def interrupt(self) -> None:
        """보류 중인 쿼리를 중단합니다."""
        return self._conn.interrupt()

    async def create_function(
        self, name: str, num_params: int, func: Callable, deterministic: bool = False
    ) -> None:
        """
        나중에 SQL 문에서 사용할 수 있는 사용자 정의 함수를 만듭니다. 
        쿼리 실행이 발생하는 동일한 스레드 내에서 실행해야 하므로 연결에 대해 직접 실행하는 대신 `run` 함수로 연기합니다.
        """
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
        await self._execute(self._conn.enable_load_extension, value)  # type: ignore

    async def load_extension(self, path: str):
        await self._execute(self._conn.load_extension, path)  # type: ignore

    async def set_progress_handler(
        self, handler: Callable[[], Optional[int]], n: int
    ) -> None:
        await self._execute(self._conn.set_progress_handler, handler, n)

    async def set_trace_callback(self, handler: Callable) -> None:
        await self._execute(self._conn.set_trace_callback, handler)

    async def iterdump(self) -> AsyncIterator[str]:
        """
        SQL 텍스트 형식으로 데이터베이스를 덤프하기 위한 비동기 반복자를 반환합니다.
        """
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
        """
        현재 데이터베이스를 대상 데이터베이스에 백업합니다.
        표준 sqlite3 또는 aiosqlite Connection 객체를 대상으로 사용합니다.
        """
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
    **kwargs: Any,
) -> Connection:
    """SQLite 데이터베이스에 대한 연결 프록시를 생성하여 반환합니다."""

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

        return sqlite3.connect(loc, **kwargs, autocommit=True)

    return Connection(connector, iter_chunk_size)