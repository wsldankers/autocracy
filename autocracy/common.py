from pathlib import Path
import builtins as builtins_module
from weakref import ref as weakref
from subprocess import run
from inspect import currentframe
from typing import Callable, Mapping, Iterable, Optional, Any

from .utils import *


class loadfilename(str):
    """To recognize loaded filenames by"""

    __slots__ = ()


class Subject:
    def __init__(self, name):
        self.name = name


class Decree:
    _required_resources = ()
    only_if = True
    name = ""
    _needs_update = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        frame = currentframe()
        while True:
            frame = frame.f_back
            if frame is None:
                self._file = None
                self._line = None
                break
            file = frame.f_code.co_filename
            if isinstance(file, loadfilename):
                self._file = file
                self._line = frame.f_lineno
                break

    def __set_name__(self, objtype, name):
        if not self.name:
            self.name = name

    @fallback
    def applied(self):
        raise RuntimeError(f"{self}: not initialized yet (did you forget a lambda?)")

    @fallback
    def updated(self):
        raise RuntimeError(f"{self}: not initialized yet (did you forget a lambda?)")

    @property
    def _should_run(self):
        return call_if_callable(self.only_if)

    def _apply(self):
        if self.applied:
            raise RuntimeError(f"{self}: refused attempt to run twice")
        if self._should_run:
            if self._needs_update:
                self._update()
                self.updated = True
            self.applied = True

    def _update(self):
        raise RuntimeError(f"{self}: update() not implemented")

    def _finalize(self, name: Optional[str] = None):
        if name and not self.name:
            self.name = name
        self.applied = False
        self.updated = False

    def __str__(self):
        name = self.name
        if name:
            name = f"{name=}, "
        else:
            name = ""
        return f"<{type(self).__name__}({name}{self._file!r}:{self._line})>"


def _extract_decrees(mapping: dict[str, Decree]) -> dict[str, Decree]:
    return {
        name: decree
        for name, decree in mapping.items()
        if not name.startswith('_') and isinstance(decree, Decree)
    }


class Group(Initializer, Decree):
    @fallback
    def decrees(self) -> dict[str, Decree]:
        raise RuntimeError(f"{self}: not initialized yet (did you forget a lambda?)")

    @property
    def _required_resources(self):
        return {
            resource
            for decree in self.decrees.values()
            for resource in decree._required_resources
        }

    @property
    def _provided_resources(self):
        try:
            return self.__dict__['_provided_resources']
        except KeyError:
            raise AttributeError('_provided_resources')

    @_provided_resources.setter
    def _provided_resources(self, value):
        self.__dict__['_provided_resources'] = value
        for decree in self.decrees.values():
            decree._provided_resources = value

    @_provided_resources.deleter
    def _provided_resources(self, value):
        try:
            del self.__dict__['_provided_resources']
        except KeyError:
            raise AttributeError('_provided_resources')

    @initializer
    def _members_that_need_update(self):
        return frozenset(
            name for name, decree in self.decrees.items() if decree._needs_update
        )

    @property
    def _needs_update(self):
        return bool(self._members_that_need_update)

    def _update(self):
        members_that_need_update = self._members_that_need_update
        for name, decree in self.decrees.items():
            if name in members_that_need_update:
                decree._update()

    def _finalize(self, name=None):
        super()._finalize(name)
        decrees = _extract_decrees(vars(self))
        for subname, decree in decrees.items():
            decree._finalize(subname)
        self.decrees = decrees

    # To appease mypy:
    def __getattr__(self, name: str) -> Decree:
        return getattr(super(), name)

    def __setattr__(self, name: str, value: Decree):
        setattr(super(), name, value)

    def __delattr__(self, name: str):
        delattr(super(), name)

    del __getattr__
    del __setattr__
    del __delattr__


class Run(Initializer, Decree):
    @fallback
    def command(self):
        raise RuntimeError(f"{self.name}: no command configured")

    def _update(self):
        command = self.command
        try:
            command = (
                b'/bin/sh',
                b'-ec',
                ensure_bytes(command),
                ensure_bytes(self.name),
            )
        except TypeError:
            command = tuple(map(ensure_bytes, command))
        completed = run(command)
        completed.check_returncode()


class File(Initializer, Decree):
    @property
    def _required_resources(self):
        source = self.source
        return [source] if source else []

    @property
    def contents(self):
        return self.__dict__.get('contents', None)

    @contents.setter
    def contents(self, value):
        attrs = self.__dict__
        if 'source' in attrs:
            raise RuntimeError(f"must set either contents or source, not both")
        attrs['contents'] = value

    content = contents

    @property
    def source(self):
        return self.__dict__.get('source', None)

    @source.setter
    def source(self, value):
        attrs = self.__dict__
        if 'contents' in attrs:
            raise RuntimeError(f"must set either contents or source, not both")
        attrs['source'] = value

    @initializer
    def _computed_contents(self):
        contents = self.contents
        if contents is None:
            contents = self._provided_resources[self.source]
        if isinstance(contents, str):
            contents = contents.encode('UTF-8')
        return contents

    @property
    def _needs_update(self):
        filename = self.filename
        try:
            old_contents = get_file(filename, 'rb')
        except FileNotFoundError:
            return True
        contents = self._computed_contents
        return old_contents != contents

    def _update(self):
        print(f"{self.name}: running")
        put_file(self._computed_contents, self.filename, 'wb')


_builtins = vars(builtins_module)


class DuplicateConfigfile(BaseException):
    pass


def loadconfig(
    subject: str | Path,
    get_file: Callable[[str | Path], bytes],
    **context,
) -> Decree:
    extra_builtins = _builtins.copy()
    globals: dict[str, Any] = subdict(
        __builtins__=extra_builtins,
        __file__=None,
    )
    extra_builtins.update(context)
    weak_globals = weakref(globals)

    seen = set()

    def load(path, ignore_duplicate):
        filename = f"{normalize_path(path)}.py"
        if filename in seen:
            if ignore_duplicate:
                return
            else:
                raise RuntimeError(f"{path} already included")
        seen.add(filename)
        content = get_file(filename)

        old_file = globals['__file__']
        try:
            globals['__file__'] = str(filename)
            code = compile(content, loadfilename(filename), 'exec')
            exec(code, weak_globals())
        finally:
            globals['__file__'] = old_file

    def include(path):
        load(path, False)

    extra_builtins['include'] = include

    def require(path):
        load(path, True)

    extra_builtins['require'] = require

    extra_builtins['subject'] = Subject(subject)

    include(subject)

    decree = Group(**_extract_decrees(globals))
    decree._finalize('_root')

    return decree


__all__ = (
    'Decree',
    'DuplicateConfigfile',
    'File',
    'Group',
    'Run',
    'Subject',
    'loadconfig',
    'loadfilename',
)
