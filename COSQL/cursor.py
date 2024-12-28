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
        """공유 연결의 스레드에서 주어진 함수를 실행합니다."""
        return await self._conn._execute(fn, *args, **kwargs)

    async def execute(
        self, sql: str, parameters: Optional[Iterable[Any]] = None
    ) -> "Cursor":
        """쿼리를 실행합니다."""
        if parameters is None:
            parameters = []
        await self._execute(self._cursor.execute, sql, parameters)
        return self

    async def executemany(
        self, sql: str, parameters: Iterable[Iterable[Any]]
    ) -> "Cursor":
        """다중 쿼리를 실행합니다."""
        await self._execute(self._cursor.executemany, sql, parameters)
        return self

    async def executescript(self, sql_script: str) -> "Cursor":
        """사용자 스크립트를 실행합니다."""
        await self._execute(self._cursor.executescript, sql_script)
        return self

    async def fetchone(self) -> Optional[sqlite3.Row]:
        """단일 행을 가져옵니다."""
        return await self._execute(self._cursor.fetchone)

    async def fetchmany(self, size: Optional[int] = None) -> Iterable[sqlite3.Row]:
        """`cursor.arraysize` 개수까지 행을 가져옵니다."""
        args: Tuple[int, ...] = ()
        if size is not None:
            args = (size,)
        return await self._execute(self._cursor.fetchmany, *args)

    async def fetchall(self) -> Iterable[sqlite3.Row]:
        """모든 행을 가져옵니다."""
        return await self._execute(self._cursor.fetchall)

    async def close(self) -> None:
        """커서를 닫습니다."""
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