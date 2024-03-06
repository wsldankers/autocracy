from pathlib import Path
import builtins as builtins_module
from weakref import ref as weakref
from subprocess import run
from inspect import currentframe
from typing import (
    Callable,
    MutableMapping,
    Optional,
    Union,
    Any,
    TYPE_CHECKING,
    cast,
)
from collections.abc import Collection, Set
from abc import ABC, abstractmethod
from types import MappingProxyType
from os import (
    mkdir,
    stat,
    chown,
    chmod,
    open as os_open,
    readlink,
    rmdir,
    unlink,
    symlink,
    access,
    F_OK,
)
from subprocess import run, DEVNULL
from pwd import getpwnam, getpwuid
from grp import getgrnam
from stat import S_IMODE, S_ISLNK, S_ISDIR
from errno import ENOTEMPTY
from shutil import rmtree

from .utils import *
from .edicts.base import Decree, Policy, extract_decrees, loadfilename

_builtins = vars(builtins_module)


class DuplicateConfigfile(BaseException):
    pass


def _load_from_repository(
    get_file: Callable[[Union[Path, str]], bytes],
    filename: str,
    **context,
) -> dict[str, Any]:
    builtins: dict[str, Any] = subdict(__file__=None)
    builtins.update(_builtins)
    builtins.update(context)
    weak_builtins = weakref(builtins)

    variables: dict[str, Any] = subdict(__builtins__=builtins)
    weak_variables = weakref(variables)

    seen = set()

    def load(path, ignore_duplicate):
        variables = weak_variables()
        builtins = weak_builtins()
        filename = f"{normalize_path(path)}.py"
        if filename in seen:
            if ignore_duplicate:
                return
            else:
                raise DuplicateConfigfile(f"{path} already included")
        seen.add(filename)
        content = get_file(filename)

        old_file = builtins['__file__']
        try:
            builtins['__file__'] = str(filename)
            code = compile(content, loadfilename(filename), 'exec')
            exec(code, variables)
        finally:
            builtins['__file__'] = old_file

    def include(path):
        load(path, False)

    builtins['include'] = include

    def require(path):
        load(path, True)

    builtins['require'] = require

    include(filename)

    return variables


def load_policy(
    get_file: Callable[[Union[Path, str]], bytes],
    subject: str,
    **context,
) -> Decree:
    tags = load_tags(get_file, subject)

    variables = _load_from_repository(
        get_file, 'policy', **context, **tags, subject=subject
    )

    policy = Policy(**extract_decrees(variables))
    policy._prepare('_root')

    return policy


def load_tags(
    get_file: Callable[[Union[Path, str]], bytes],
    subject: Optional[str] = None,
) -> dict[str, Any]:
    tags = _load_from_repository(get_file, 'tags')
    if subject is None:
        return {key: value for key, value in tags.items() if isinstance(value, set)}
    else:
        return {
            key: subject in value
            for key, value in tags.items()
            if isinstance(value, set)
        }


def load_config(filename: Union[Path, str], **context) -> dict[str, Any]:
    builtins: dict[str, Any] = subdict(__file__=None)
    builtins.update(_builtins)
    weak_builtins = weakref(builtins)

    variables: dict[str, Any] = subdict(__builtins__=builtins)
    variables.update(context)
    weak_variables = weakref(variables)

    def include(filename):
        builtins = weak_builtins()
        variables = weak_variables()

        old_file = builtins['__file__']
        new_file = str(Path(old_file or '.').parent / filename)
        content = get_file(new_file)
        try:
            builtins['__file__'] = new_file
            code = compile(content, new_file, 'exec')
            exec(code, variables)
        finally:
            builtins['__file__'] = old_file

    builtins['include'] = include

    include(filename)

    return {
        name: value for name, value in variables.items() if not name.startswith('_')
    }
