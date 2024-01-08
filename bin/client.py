#! /usr/bin/python3

from asyncio import run
from pathlib import Path

from autocracy.client import main

if __name__ == '__main__':
    run(main(Path('.')))
