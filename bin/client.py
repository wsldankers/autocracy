#! /usr/bin/python3

from asyncio import run
from os import environ
from sys import argv, exit

from autocracy.client import main

if __name__ == '__main__':
    exit(run(main(*argv, **environ)))
