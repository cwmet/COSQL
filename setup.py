from setuptools import setup, find_packages

setup(
    name='COSQL',
    version='0.0.2',
    description='비동기 SQLite3 (aiosqlite 포크 추가버전)',
    author='c0met',
    author_email='support@c0met.kr',
    url='https://github.com/cwmet/COSQL',
    install_requires=['asyncio'],
    packages=find_packages(exclude=[]),
    keywords=['aiosqlite', 'sqlite3', 'database'],
    python_requires='>=3.6',
    package_data={},
    zip_safe=False,
)