import sqlite3
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Iterable,
    Optional,
    Tuple,
    Type,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from .core import Connection

class Cursor:
    def __init__(self, conn: "Connection", cursor: sqlite3.Cursor) -> None:
        self.iter_chunk_size = conn._iter_chunk_size
        self._conn = conn
        self._cursor = cursor

    def __aiter__(self) -> AsyncIterator[sqlite3.Row]:
        return self._fetch_chunked()

    async def _fetch_chunked(self):
        while True:
            rows = await self.fetchmany(self.iter_chunk_size)
            if not rows:
                return
            for row in rows:
                yield row

    async def _execute(self, fn, *args, **kwargs):
        return await self._conn._execute(fn, *args, **kwargs)

    async def execute(self, sql: str, parameters: Optional[Iterable[Any]] = None) -> "Cursor":
        if parameters is None:
            parameters = []
        await self._execute(self._cursor.execute, sql, parameters)
        return self

    async def executemany(self, sql: str, parameters: Iterable[Iterable[Any]]) -> "Cursor":
        await self._execute(self._cursor.executemany, sql, parameters)
        return self

    async def executescript(self, sql_script: str) -> "Cursor":
        await self._execute(self._cursor.executescript, sql_script)
        return self

    async def fetchone(self) -> Optional[sqlite3.Row]:
        return await self._execute(self._cursor.fetchone)

    async def fetchall(self) -> Iterable[sqlite3.Row]:
        return await self._execute(self._cursor.fetchall)

    async def fetchmany(self, size: Optional[int] = None) -> Iterable[sqlite3.Row]:
        args: Tuple[int, ...] = ()
        if size is not None:
            args = (size,)
        return await self._execute(self._cursor.fetchmany, *args)

    async def close(self) -> None:
        await self._execute(self._cursor.close)

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    @property
    def lastrowid(self) -> Optional[int]:
        return self._cursor.lastrowid

    @property
    def arraysize(self) -> int:
        return self._cursor.arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        self._cursor.arraysize = value

    @property
    def description(self) -> Tuple[Tuple[str, None, None, None, None, None, None], ...]:
        return self._cursor.description

    @property
    def row_factory(self) -> Optional[Callable[[sqlite3.Cursor, sqlite3.Row], object]]:
        return self._cursor.row_factory

    @row_factory.setter
    def row_factory(self, factory: Optional[Type]) -> None:
        self._cursor.row_factory = factory

    @property
    def connection(self) -> sqlite3.Connection:
        return self._cursor.connection

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()