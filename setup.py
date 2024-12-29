from setuptools import setup, find_packages

setup(
    name = 'CoSQL',
    version = '1.0.0',
    description = 'Comet용 SQLite3 비동기 라이브러리',

    author = 'Comet',
    url = 'https://github.com/cwmet/CoSQL',

    packages = find_packages(exclude=[]),
    python_requires = '>=3.11',
    install_requires = ['asyncio'],
)