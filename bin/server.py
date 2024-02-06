#! /usr/bin/python3

from sys import argv
from os import environ
from asyncio import run

from autocracy.server import main

if __name__ == '__main__':
    run(main(*argv, **environ))
