from pathlib import Path
import builtins as builtins_module
from weakref import ref as weakref
from subprocess import run
from inspect import currentframe
from typing import (
    Callable,
    MutableMapping,
    Iterable,
    Optional,
    Union,
    Any,
    TYPE_CHECKING,
    cast,
)
from collections.abc import Collection, Sequence, Set
from abc import ABC, abstractmethod
from asyncio import gather
from types import MappingProxyType
from os import mkdir
from subprocess import run, DEVNULL

from .utils import *


class loadfilename(str):
    """To recognize loaded filenames by"""

    __slots__ = ()


class BaseRepository(ABC, Initializer):
    @abstractmethod
    def get_file(self, path: Union[str, Path]) -> bytes:
        pass

    @abstractmethod
    def get_files(self, path: Union[str, Path]) -> dict[str, bytes]:
        pass


class Decree:
    only_if = True
    name = ""

    if TYPE_CHECKING:

        @property
        def _needs_update(self) -> bool:
            return True

    else:
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

    def _provision(self, repository: BaseRepository):
        pass

    @fallback
    def applied(self):
        raise RuntimeError(f"{self}: not initialized yet (did you forget a lambda?)")

    @fallback
    def updated(self):
        raise RuntimeError(f"{self}: not initialized yet (did you forget a lambda?)")

    @fallback
    def activated(self):
        raise RuntimeError(f"{self}: not initialized yet (did you forget a lambda?)")

    @property
    def _should_activate(self):
        return call_if_callable(self.only_if)

    def _apply(self):
        if self.applied:
            raise RuntimeError(f"{self}: refused attempt to run twice")
        if self._needs_update:
            self._update()
            self.updated = True
        if self._should_activate:
            self._activate()
            self.activated = True
        self.applied = True

    def _update(self):
        pass

    def _activate(self):
        pass

    def _finalize(self, name: Optional[str] = None):
        if name and not self.name:
            self.name = name
        self.applied = False
        self.updated = False
        self.activated = False

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

    def _provision(self, repository: BaseRepository):
        for decree in self.decrees.values():
            decree._provision(repository)

    @initializer
    def _members_that_need_update(self):
        return frozenset(
            name
            for name, decree in self.decrees.items()
            if decree._should_run and decree._needs_update
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


class Policy(Group):
    pass


class Run(Initializer, Decree):
    @fallback
    def command(self):
        raise RuntimeError(f"{self.name}: no command configured")

    def _activate(self):
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
    _file_contents: Optional[bytes] = None

    def _provision(self, repository: BaseRepository):
        source = self.source
        if source is not None:
            self._file_contents = repository.get_file(source)

    @property
    def contents(self):
        return self.__dict__.get('contents', None)

    @contents.setter
    def contents(self, value):
        if self.source is not None:
            raise RuntimeError(f"must set either contents or source, not both")
        self.__dict__['contents'] = value

    content = contents

    @property
    def source(self):
        return self.__dict__.get('source', None)

    @source.setter
    def source(self, value):
        if self.contents is not None:
            raise RuntimeError(f"must set either contents or source, not both")
        self.__dict__['source'] = value

    @initializer
    def _computed_contents(self):
        contents = self.contents
        if contents is None:
            contents = self._file_contents
        if isinstance(contents, str):
            contents = contents.encode('UTF-8')
        return contents

    @property
    def _needs_update(self):
        destination = self.destination
        try:
            old_contents = get_file(destination, 'rb')
        except FileNotFoundError:
            return True
        contents = self._computed_contents
        return old_contents != contents

    def _update(self):
        print(f"{self.name}: running")
        put_file(self._computed_contents, self.destination, 'wb')


class RecursiveFiles(Initializer, Decree):
    _files: MutableMapping[str, bytes] = cast(MutableMapping, MappingProxyType({}))
    destination: Union[Path, str]

    def _provision(self, repository: BaseRepository):
        source = self.source
        if source is not None:
            self._files = repository.get_files(source).copy()

    @property
    def source(self) -> Union[Path, str]:
        return self.__dict__.get('source', None)

    @source.setter
    def source(self, value: Union[Path, str]):
        self.__dict__['source'] = str(value)

    @initializer
    def _existing_parents(self) -> set[Path]:
        return {Path(self.destination).parent}

    @property
    def _needs_update(self) -> bool:
        source = Path(self.source)
        destination = Path(self.destination)
        files = self._files
        existing_parents = self._existing_parents
        for filename, contents in list(files.items()):
            full_path = destination / Path(filename).relative_to(source)
            try:
                old_contents = get_file(full_path, 'rb')
            except FileNotFoundError:
                continue
            self._existing_parents.update(full_path.parents)
            if old_contents != contents:
                continue
            del files[filename]

        return bool(files)

    def _update(self) -> None:
        print(f"{self.name}: running")

        source = Path(self.source)
        destination = Path(self.destination)
        existing_parents = self._existing_parents
        for filename, contents in self._files.items():
            full_path = destination / Path(filename).relative_to(source)
            print(f"updating {full_path}")
            try:
                put_file(contents, full_path, 'wb')
            except FileNotFoundError:
                parent = full_path.parent
                create = []
                for parent in full_path.parents:
                    if parent in existing_parents:
                        break
                    create.append(parent)
                for parent in reversed(create):
                    try:
                        mkdir(parent)
                    except FileExistsError:
                        pass
                existing_parents.update(create)
                put_file(contents, full_path, 'wb')


class Packages(Initializer, Decree):
    install: Collection[str] = ()
    remove: Collection[str] = ()
    purge: Optional[bool] = None
    recommends: Optional[bool] = None
    update = True
    clean = False
    # quick = False
    gentle = False
    _install: Set[str]
    _remove: Set[str]

    @property
    def _needs_update(self) -> bool:
        result = run(
            ['dpkg', '--print-architecture'],
            capture_output=True,
            text=True,
            check=True,
            stdin=DEVNULL,
        )

        (native_arch,) = result.stdout.splitlines()
        default_archs = frozenset((native_arch, 'all'))

        result = run(
            [
                'dpkg-query',
                '-f',
                r'${Package} ${Architecture} ${Version} ${Status} ${Essential}\n',
                '-W',
            ],
            capture_output=True,
            text=True,
            check=True,
            stdin=DEVNULL,
        )

        installed: set[str] = set()

        for line in result.stdout.splitlines():
            name, arch, version, want, error, status, essential = line.split()
            if error != 'ok':
                raise RuntimeError(f"package {name}:{arch} is in error state {error}")
            if status == 'installed':
                installed.add(f"{name}:{arch}")
                if arch in default_archs:
                    installed.add(name)
            elif status != 'config-files':
                raise RuntimeError(
                    f"package {name}:{arch} has unknown status '{status}'"
                )

        install = frozenset(self.install) - installed
        remove = frozenset(self.remove) & installed

        self._install = install
        self._remove = remove

        return bool(install or remove)

    def _update(self) -> None:
        install = self._install
        remove = self._remove

        if self.clean:
            run(
                ['apt-get', 'clean'],
                capture_output=True,
                text=True,
                check=True,
                stdin=DEVNULL,
            )

        if install and self.update:
            run(
                ['apt-get', '-qq', 'update'],
                capture_output=True,
                text=True,
                check=True,
                stdin=DEVNULL,
            )

        apt_get_options: set[str] = {'-qy'}
        if remove:
            if self.purge:
                apt_get_options.add('--purge')
            elif self.purge is not None:
                apt_get_options.add('--no-purge')

        if self.recommends:
            apt_get_options.add('--install-recommends')
        elif self.recommends is not None:
            apt_get_options.add('--no-install-recommends')

        if self.gentle:
            if remove:
                run(
                    ['apt-mark', 'auto', *remove],
                    capture_output=True,
                    text=True,
                    check=True,
                    stdin=DEVNULL,
                )
                apt_get_options.add('--auto-remove')
            run(
                ['apt-get', *apt_get_options, 'install', *install],
                capture_output=True,
                text=True,
                check=True,
                stdin=DEVNULL,
            )
        else:
            run(
                [
                    'apt-get',
                    *apt_get_options,
                    'install',
                    *install,
                    *(f"{package}-" for package in remove),
                ],
                capture_output=True,
                text=True,
                check=True,
                stdin=DEVNULL,
            )

        if install and self.clean:
            run(
                ['apt-get', 'clean'],
                capture_output=True,
                text=True,
                check=True,
                stdin=DEVNULL,
            )


class Service(Initializer, Decree):
    reload: Optional[bool] = None
    enable: Optional[bool] = None
    active: Optional[bool] = None
    _change_enable: Optional[bool] = None
    _change_active: Optional[bool] = None
    unit: str

    @property
    def _needs_update(self) -> bool:
        enable = self.enable
        if enable is not None:
            result = run(
                ['systemctl', 'is-enabled', self.unit],
                capture_output=True,
                text=True,
                check=True,
                stdin=DEVNULL,
            )
            enable = bool(self.enable)
            if (result.stdout.strip() == 'enabled') != enable:
                self._change_enable = enable

        active = self.active
        if active is not None:
            command = ['systemctl', 'is-active', '--quiet', self.unit]
            result = run(
                command,
                capture_output=True,
                text=True,
                stdin=DEVNULL,
            )
            returncode = result.returncode
            if returncode == 0:
                if not active:
                    self._change_active = False
            elif returncode == 3:
                if active:
                    self._change_active = True
            else:
                raise RuntimeError(
                    f"command '{' '.join(command)}' returned non-zero exit status {returncode}:\n{result.stderr}"
                )

        return self._change_enable or self._change_active

    def _update(self) -> None:
        change_enable = self._change_enable
        change_active = self._change_active
        command = ['systemctl']
        if change_enable is not None:
            if change_enable:
                command.append('enable')
                if change_active:
                    command.append('--now')
            else:
                command.append('disable')
                if change_active is not None and not change_active:
                    command.append('--now')
        elif change_active is not None:
            if change_active:
                command.append('start')
            else:
                command.append('stop')
        else:
            return

        command.append(self.unit)
        run(command, text=True, check=True, stdin=DEVNULL)


_builtins = vars(builtins_module)


class DuplicateConfigfile(BaseException):
    pass


def load_policy(
    subject: Union[Path, str],
    get_file: Callable[[Union[Path, str]], bytes],
    **context,
) -> Decree:
    tags = load_tags('tags', subject)
    extra_builtins = {
        **_builtins,
        'subject': subject,
        **context,
        **tags,
    }
    variables: dict[str, Any] = subdict(
        __builtins__=extra_builtins,
        __file__=None,
    )
    weak_variables = weakref(variables)

    seen = set()

    def load(path, ignore_duplicate):
        variables = weak_variables()
        filename = f"{normalize_path(path)}.py"
        if filename in seen:
            if ignore_duplicate:
                return
            else:
                raise DuplicateConfigfile(f"{path} already included")
        seen.add(filename)
        content = get_file(filename)

        old_file = variables['__file__']
        try:
            variables['__file__'] = str(filename)
            code = compile(content, loadfilename(filename), 'exec')
            exec(code, variables)
        finally:
            variables['__file__'] = old_file

    def include(path):
        load(path, False)

    extra_builtins['include'] = include

    def require(path):
        load(path, True)

    extra_builtins['require'] = require

    include('policy')

    policy = Policy(**_extract_decrees(variables))
    policy._finalize('_root')

    return policy


def load_config(filename: Union[Path, str], **context) -> dict[str, Any]:
    extra_builtins = _builtins.copy()
    variables: dict[str, Any] = subdict(
        __builtins__=extra_builtins,
        __file__=None,
    )
    variables.update(context)
    weak_variables = weakref(variables)

    def include(filename):
        variables = weak_variables()
        content = get_file(filename)

        old_file = variables['__file__']
        new_file = str(Path(old_file or '.').parent / filename)
        try:
            variables['__file__'] = new_file
            code = compile(content, new_file, 'exec')
            exec(code, variables)
        finally:
            variables['__file__'] = old_file

    extra_builtins['include'] = include

    include(filename)

    return {
        name: value for name, value in variables.items() if not name.startswith('_')
    }


def load_tags(
    filename: Union[Path, str],
    subject: Optional[str],
    /,
    **context,
) -> dict[str, Any]:
    tags = load_config(filename, **context)
    if subject is None:
        return {key: value for key, value in tags.items() if isinstance(value, set)}
    else:
        return {
            key: subject in value
            for key, value in tags.items()
            if isinstance(value, set)
        }


__all__ = (
    'BaseRepository',
    'Decree',
    'DuplicateConfigfile',
    'File',
    'RecursiveFiles',
    'Group',
    'Policy',
    'Run',
    'Packages',
    'Subject',
    'load_decree',
    'load_config',
    'loadfilename',
)
