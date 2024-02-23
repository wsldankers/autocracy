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
from collections import namedtuple
from collections.abc import Collection, Sequence, Set
from abc import ABC, abstractmethod
from asyncio import gather
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

    def _prepare(self, name: Optional[str] = None):
        if name and not self.name:
            self.name = name
        self.applied = False
        self.updated = False
        self.activated = False

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
    def updated(self):
        return any(decree.updated for decree in self.decrees.values())

    @initializer
    def activated(self):
        return any(decree.activated for decree in self.decrees.values())

    def _apply(self):
        for decree in self.decrees.values():
            decree._apply()

    def _prepare(self, name=None):
        super()._prepare(name)
        decrees = _extract_decrees(vars(self))
        for subname, decree in decrees.items():
            decree._prepare(subname)
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


class _FileHandlingAction:
    __slots__ = ('target', 'create', 'chown', 'chmod', 'contents')

    target: Union[Path, str]
    # create is just informational but used to see if parent directories
    # need to be created.
    create: bool
    chown: Optional[tuple[int, int]]
    chmod: Optional[int]
    contents: Optional[bytes]

    def __bool__(self):
        return (
            self.create
            or self.chown is not None
            or self.chmod is not None
            or self.contents is not None
        )

    def __call__(self):
        do_contents = self.contents
        if do_contents is None:
            open_mode = 'w+b'
        else:
            open_mode = 'wb'

        do_chmod = self.chmod

        def opener(path, flags):
            open_mode = 0o666 if do_chmod is None else 0o600
            return os_open(path, flags, mode=open_mode)

        with open(self.target, open_mode, opener=opener) as fh:
            if do_contents:
                print('contents')
                fh.write(do_contents)
            fd = fh.fileno()
            do_chown = self.chown
            if do_chown is not None:
                print('chown')
                chown(fd, *do_chown)
            if do_chmod is not None:
                print('chmod')
                chmod(fd, do_chmod)


def _parse_owner(owner: Union[str, int, None]) -> tuple[Optional[int], Optional[int]]:
    if owner is None:
        return (None, None)
    owner, sep, group = f"{owner}".partition(':')
    if owner.isdecimal() and owner.isascii():
        pwent = None
        uid = int(owner)
    elif owner == '':
        pwent = None
        uid = None
    else:
        pwent = getpwnam(owner)
        uid = pwent.pw_uid
    if sep:
        if group.isdecimal() and group.isascii():
            gid = int(group)
        elif group == '':
            if uid is None:
                gid = None
            else:
                if pwent is None:
                    pwent = getpwuid(uid)
                gid = pwent.pw_gid
        else:
            gid = getgrnam(group).gr_gid
    else:
        gid = None
    return (uid, gid)


def _parse_mode(mode: Union[str, int, None]) -> Optional[int]:
    if mode is None:
        return None
    if isinstance(mode, str):
        mode = int(mode, base=8)
    return S_IMODE(mode)


class _FileHandlingMixin:
    owner: Union[str, int, None] = None
    mode: Union[str, int, None] = 0o644

    @initializer
    def _owner(self) -> tuple[Optional[int], Optional[int]]:
        return _parse_owner(self.owner)

    @initializer
    def _mode(self) -> Optional[int]:
        return _parse_mode(self.mode)

    def _check_file(self, target, new_contents):
        uid, gid = self._owner
        mode = self._mode
        action = _FileHandlingAction()
        action.target = target
        try:
            with open(target, 'rb') as fh:
                st = stat(fh.fileno())
                old_contents = fh.read()
        except FileNotFoundError:
            action.create = True
            needs_chown = uid is not None or gid is not None
            needs_chmod = mode is not None
            needs_contents = True
        else:
            action.create = False
            needs_chown = (uid is not None and st.st_uid != uid) or (
                gid is not None and st.st_gid != gid
            )
            needs_chmod = mode is not None and S_IMODE(st.st_mode) != mode
            needs_contents = old_contents != new_contents

        if needs_chown:
            action.chown = (-1 if uid is None else uid, -1 if gid is None else gid)
        else:
            action.chown = None

        if needs_chmod:
            action.chmod = mode
        else:
            action.chmod = None

        if needs_contents:
            action.contents = new_contents
        else:
            action.contents = None

        return action


class File(Initializer, _FileHandlingMixin, Decree):
    target: Union[Path, str]
    makedirs = False

    _contents: Optional[bytes] = None
    _needs_chown = False
    _needs_chmod = False
    _needs_contents = False

    def _provision(self, repository: BaseRepository):
        source = self.source
        if source is not None:
            self._contents = repository.get_file(source)

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
            contents = self._contents
        if isinstance(contents, str):
            contents = contents.encode('UTF-8')
        return contents

    @property
    def _needs_update(self):
        target = self.target
        action = self._check_file(self.target, self._computed_contents)
        self._action = action
        return bool(action)

    def _update(self):
        print(f"{self.name}: running")

        if self.makedirs:
            try:
                self._action()
            except FileNotFoundError:
                pass
            else:
                return

            Path(self.target).parent.mkdir(parents=True)

        self._action()


class RecursiveFiles(Initializer, _FileHandlingMixin, Decree):
    _files: MutableMapping[str, bytes] = cast(MutableMapping, MappingProxyType({}))
    target: Union[Path, str]

    def _provision(self, repository: BaseRepository):
        source = self.source
        if source is not None:
            self._files = repository.get_files(source).copy()

    @property
    def source(self) -> Optional[str]:
        return self.__dict__.get('source', None)

    @source.setter
    def source(self, value: Union[Path, str]):
        self.__dict__['source'] = str(value)

    @initializer
    def _actions(self) -> list[_FileHandlingAction]:
        return []

    @initializer
    def _existing_parents(self) -> set[Path]:
        return {Path(self.target).parent}

    @property
    def _needs_update(self) -> bool:
        source_str = self.source
        if source_str is None:
            return False
        source = Path(source_str)
        target = Path(self.target)
        files = self._files
        actions = self._actions
        existing_parents = self._existing_parents
        for filename, contents in list(files.items()):
            full_path = target / Path(filename).relative_to(source)
            action = self._check_file(full_path, contents)
            if action:
                print(action)
                actions.append(action)
            if not action.create:
                self._existing_parents.update(full_path.parents)

        return bool(actions)

    def _update(self) -> None:
        print(f"{self.name}: running")

        existing_parents = self._existing_parents

        for action in self._actions:
            print(f"updating {action.target}")
            try:
                action()
            except FileNotFoundError:
                pass
            else:
                continue
            full_path = cast(Path, action.target)
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
            action()


class Symlink(Initializer, Decree):
    target: Union[Path, str]
    owner: Union[str, int, None] = None
    contents: Union[str]
    force = False

    _needs_remove = False
    _needs_chown = False
    _needs_create = False

    @initializer
    def _owner(self) -> tuple[Optional[int], Optional[int]]:
        return _parse_owner(self.owner)

    @property
    def _needs_update(self) -> bool:
        target = self.target
        uid, gid = self._owner
        try:
            st = stat(target, follow_symlinks=False)
        except FileNotFoundError:
            self._needs_create = True
        else:
            if S_ISLNK(st.st_mode) and readlink(target) == self.contents:
                self._needs_chown = (uid is not None and st.st_uid != uid) or (
                    gid is not None and st.st_gid != gid
                )
            else:
                self._needs_remove = True

        if self._needs_remove:
            self._needs_create = True

        if self._needs_create:
            self._needs_chown = uid is not None or gid is not None

        return self._needs_chown

    def _update(self) -> None:
        print(f"{self.name}: running")
        target = self.target

        if self._needs_remove:
            try:
                unlink(target)
            except IsADirectoryError:
                try:
                    rmdir(target)
                except OSError as e:
                    if e.errno == ENOTEMPTY and self.force:
                        rmtree(target)
                    else:
                        raise e from None

        if self._needs_create:
            symlink(self.contents, target)

        if self._needs_chown:
            uid, gid = self._owner
            chown(
                target,
                -1 if uid is None else uid,
                -1 if gid is None else gid,
                follow_symlinks=False,
            )


class Directory(Initializer, Decree):
    target: Union[Path, str]
    owner: Union[str, int, None] = None
    mode: Union[str, int, None] = 0o755

    _needs_remove = False
    _needs_chown = False
    _needs_chmod = False
    _needs_create = False

    @initializer
    def _owner(self) -> tuple[Optional[int], Optional[int]]:
        return _parse_owner(self.owner)

    @initializer
    def _mode(self) -> Optional[int]:
        return _parse_mode(self.mode)

    @property
    def _needs_update(self) -> bool:
        target = self.target
        uid, gid = self._owner
        mode = self._mode
        try:
            st = stat(target, follow_symlinks=False)
        except FileNotFoundError:
            self._needs_create = True
        else:
            if S_ISDIR(st.st_mode):
                self._needs_chown = (uid is not None and st.st_uid != uid) or (
                    gid is not None and st.st_gid != gid
                )
                self._needs_chmod = mode is not None and S_IMODE(st.st_mode) != mode
            else:
                self._needs_remove = True

        if self._needs_remove:
            self._needs_create = True

        if self._needs_create:
            self._needs_chown = uid is not None or gid is not None
            self._needs_chmod = mode is not None

        return self._needs_chown or self._needs_chmod

    def _update(self) -> None:
        print(f"{self.name}: running")
        target = self.target

        if self._needs_remove:
            unlink(target)

        if self._needs_create:
            if self._needs_chmod:
                mkdir(target, mode=0o700)
            else:
                mkdir(target)

        if self._needs_chown:
            uid, gid = self._owner
            chown(
                target,
                -1 if uid is None else uid,
                -1 if gid is None else gid,
                follow_symlinks=False,
            )

        if self._needs_chmod:
            mode = self._mode
            assert mode is not None
            chmod(target, mode, follow_symlinks=False)


class Permissions(Initializer, Decree):
    target: Union[Path, str]
    owner: Union[str, int, None] = None
    mode: Union[str, int, None] = None
    missing_ok = False

    _needs_chown = False
    _needs_chmod = False

    @initializer
    def _owner(self) -> tuple[Optional[int], Optional[int]]:
        return _parse_owner(self.owner)

    @initializer
    def _mode(self) -> Optional[int]:
        return _parse_mode(self.mode)

    @property
    def _needs_update(self) -> bool:
        target = self.target
        uid, gid = self._owner
        mode = self._mode
        try:
            st = stat(target, follow_symlinks=False)
        except FileNotFoundError:
            if not self.missing_ok:
                raise
        else:
            self._needs_chown = (uid is not None and st.st_uid != uid) or (
                gid is not None and st.st_gid != gid
            )
            self._needs_chmod = mode is not None and S_IMODE(st.st_mode) != mode

        return self._needs_chown or self._needs_chmod

    def _update(self) -> None:
        print(f"{self.name}: running")
        target = self.target

        if self._needs_chown:
            uid, gid = self._owner
            chown(
                target,
                -1 if uid is None else uid,
                -1 if gid is None else gid,
                follow_symlinks=False,
            )

        if self._needs_chmod:
            assert self._mode is not None
            chmod(target, self._mode, follow_symlinks=False)


class Remove(Initializer, Decree):
    target: Union[Path, str]
    force = False

    @property
    def _needs_update(self) -> bool:
        return access(self.target, F_OK)

    def _update(self) -> None:
        print(f"{self.name}: running")
        target = self.target

        try:
            unlink(target)
        except IsADirectoryError:
            try:
                rmdir(target)
            except OSError as e:
                if e.errno == ENOTEMPTY and self.force:
                    rmtree(target)
                else:
                    raise e from None


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
    unit: str
    reload = False
    restart = False
    enable: Optional[bool] = None
    active: Optional[bool] = None
    mask: Optional[bool] = None

    _change_enable: Optional[bool] = None
    _change_active: Optional[bool] = None
    _change_mask: Optional[bool] = None
    _was_active = False

    def _prepare(self, name: Optional[str] = None) -> None:
        super()._prepare()

        if is_true(self.mask) and (is_true(self.enable) or is_true(self.active)):
            raise RuntimeError(
                "{self.name}: masked units can't be enabled or activated"
            )

        if is_false(self.active) and (is_true(self.reload) or is_true(self.restart)):
            raise RuntimeError(
                "{self.name}: deactivated units can't be reloaded or restarted"
            )

    def _systemctl_is_enabled(self):
        command = ['systemctl', 'is-enabled', self.unit]
        result = run(command, capture_output=True, text=True, stdin=DEVNULL)

        enabled = result.stdout.strip()
        returncode = result.returncode
        if not enabled and returncode:
            raise RuntimeError(
                f"command '{' '.join(command)}' returned non-zero exit status {returncode}:\n{result.stderr}"
            )

        return enabled

    @property
    def _needs_update(self) -> bool:
        unit = self.unit
        enable = self.enable
        mask = self.mask
        if enable is not None or mask is not None:
            enabled = self._systemctl_is_enabled()

            bool_mask = bool(mask)
            if mask is not None and (enabled == 'masked') != bool_mask:
                self._change_mask = bool_mask

            bool_enable = bool(enable)
            if enable is not None and (
                enabled == 'masked' or (enabled == 'enabled') != bool_enable
            ):
                self._change_enable = bool_enable

        active = self.active
        if active is not None:
            command = ['systemctl', 'is-active', '--quiet', unit]
            result = run(command, capture_output=True, text=True, stdin=DEVNULL)
            returncode = result.returncode
            if returncode == 0:
                self._was_active = True
                if not active:
                    self._change_active = False
            elif returncode == 3:
                if active:
                    self._change_active = True
            else:
                raise RuntimeError(
                    f"command '{' '.join(command)}' returned non-zero exit status {returncode}:\n{result.stderr}"
                )

        return bool(self._change_enable or self._change_active or self._change_mask)

    def _update(self) -> None:
        unit = self.unit

        change_active = self._change_active
        change_mask = self._change_mask
        if change_mask is not None:
            command = ['systemctl']
            if change_mask:
                command.append('mask')
            else:
                command.append('unmask')
            command.append(unit)
            run(command, text=True, check=True, stdin=DEVNULL)

            # Avoid running _systemctl_is_enabled() unless we really need the
            # extra info:
            if (
                not change_mask
                and change_active
                and (self.reload or self.restart)
                and self._systemctl_is_enabled() == 'active'
            ):
                # It turned out to be active already, after unmasking
                change_active = None
                self._change_active = None
                self._was_active = True

        change_enable = self._change_enable
        if change_enable is not None or change_active is not None:
            command = ['systemctl']
            if change_enable is not None:
                if change_enable:
                    command.append('enable')
                    if is_true(change_active):
                        command.append('--now')
                else:
                    command.append('disable')
                    if is_false(change_active):
                        command.append('--now')
            else:
                if change_active:
                    command.append('start')
                else:
                    command.append('stop')

            command.append(unit)
            run(command, text=True, check=True, stdin=DEVNULL)

    def _activate(self) -> None:
        reload = self.reload
        restart = self.restart

        if not reload and not restart:
            return

        if self._change_active is not None:
            # We just started or stopped it, no use reloading/restarting it again
            return

        if not self._was_active:
            # It wasn't active and we didn't start it. Nothing to do, then.
            return

        command = ['systemctl']

        if reload:
            if restart:
                command.append('try-reload-or-restart')
            else:
                command.append('reload')
        else:
            command.append('try-restart')

        command.append(self.unit)
        run(command, text=True, check=True, stdin=DEVNULL)


_builtins = vars(builtins_module)


class DuplicateConfigfile(BaseException):
    pass


def _load_from_repository(
    get_file: Callable[[Union[Path, str]], bytes],
    filename: str,
    **context,
) -> dict[str, Any]:
    extra_builtins = {
        **_builtins,
        **context,
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

    policy = Policy(**_extract_decrees(variables))
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


__all__ = (
    'File',
    'RecursiveFiles',
    'Symlink',
    'Directory',
    'Permissions',
    'Remove',
    'Group',
    'Policy',
    'Run',
    'Packages',
)
