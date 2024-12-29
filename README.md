# CoSQL
Comet 서비스에 사용하는 비동기 DB 라이브러리입니다.

## 설치
파이썬 3.11 이상의 모든 버전에서 사용 가능합니다.
``` console
$ pip install cosql
```

## 예시
표준 코드 스타일 (SQLite3 전통 방식도 사용 가능합니다.)
``` python
async with COSQL.connect(...) as db:
    await db.execute("INSERT INTO some_table ...")
    await db.commit()

    async with db.execute("SELECT * FROM some_table") as cursor:
        async for row in cursor:
            ...
```
