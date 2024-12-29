from setuptools import setup, find_packages

setup(
    name = 'COSQL',
    version = '0.1.0',
    description = 'SQLite3 wrapper for asyncio',

    author = 'Comet',
    url = 'https://github.com/cwmet/COSQL',

    packages = find_packages(exclude=[]),
    python_requires = '>=3.12',
    install_requires = ['asyncio'],
)