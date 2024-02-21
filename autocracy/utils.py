from pathlib import Path, PurePath
from functools import update_wrapper
from typing import (
    Iterable,
    Any,
    TypeVar,
    Callable,
    Optional,
    Union,
    cast,
    TYPE_CHECKING,
)
from sys import stderr
from functools import wraps
from weakref import ref as weakref
from collections.abc import KeysView
from os.path import commonprefix


class Initializer:
    def __init__(self, *args, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        super().__init__(*args)


if TYPE_CHECKING:
    ReturnValue = TypeVar("ReturnValue")

    def initializer(f: Callable[..., ReturnValue]) -> ReturnValue:
        return cast(ReturnValue, None)

else:

    class initializer:
        def __init__(self, getfunction):
            self.getfunction = getfunction
            self.name = getfunction.__name__
            update_wrapper(self, getfunction)

        def __set_name__(self, objtype, name):
            self.name = name

        def __get__(self, obj, objtype=None):
            try:
                objdict = vars(obj)
            except AttributeError:
                if obj is None:
                    # This typically happens when querying for docstrings,
                    # so return something with the appropriate docstring.
                    return self
                raise

            value = self.getfunction(obj)
            objdict[self.name] = value
            return value

        @property
        def __isabstractmethod__(self):
            return getattr(self.getfunction, '__isabstractmethod__', False)


class weakproperty(property):
    """A property that keeps a weak reference.

    Unlike a normal property it doesn't call functions for
    getting/setting/deleting but just stores the value in the object's
    dictionary.

    The function you'd decorate with this property is used as an
    initializer that is called when the attribute is retrieved and the
    property was either never set, it was deleted, or the weak reference
    was lost.

    The name of the supplied initializer function is also used as the key
    for the object's dictionary.

    Because it is implemented as a property with preset get/set/delete
    functions, any attempts to change the get/set/delete functions will
    break it.

    :param function f: Called with self as an argument when the property
        is dereferenced and no value was available in the dictionary."""

    def __init__(prop, f):
        name = f.__name__

        @wraps(f)
        def getter(self):
            objdict = vars(self)
            try:
                weak = objdict[name]
            except KeyError:
                pass
            else:
                return weak()
            value = f(self)

            def unsetter(weak):
                try:
                    if objdict[name] is weak:
                        del objdict[name]
                except KeyError:
                    pass

            weak = weakref(value, unsetter)
            objdict[name] = weak
            return value

        @wraps(f)
        def setter(self, value):
            objdict = vars(self)

            def unsetter(weak):
                try:
                    if objdict[name] is weak:
                        del objdict[name]
                except KeyError:
                    pass

            objdict[name] = weakref(value, unsetter)

        @wraps(f)
        def deleter(self):
            del vars(self)[name]

        super().__init__(getter, setter, deleter)


class fallback:
    def __init__(self, method):
        self._method = method
        update_wrapper(self, method)

    def __get__(self, instance, owner):
        return self._method(owner if instance is None else instance)

    @property
    def __isabstractmethod__(self):
        return getattr(self._method, '__isabstractmethod__', False)


def is_byteslike(obj):
    try:
        memoryview(obj)
    except TypeError:
        return False
    else:
        return True


def ensure_bytes(obj):
    if isinstance(obj, bytes):
        return obj

    try:
        memoryview(obj)
    except TypeError:
        pass
    else:
        return bytes(obj)

    if isinstance(obj, PurePath):
        return bytes(obj)

    try:
        return bytes(obj, 'UTF-8', 'surrogateescape')
    except TypeError:
        pass

    raise TypeError(f"cannot convert '{type(obj).__name__}' object to bytes")


def get_file(*args, **kwargs):
    with open(*args, **kwargs) as fh:
        return fh.read()


def put_file(contents, *args, **kwargs):
    with open(*args, **kwargs) as fh:
        return fh.write(contents)


def warn(*args, **kwargs):
    kwargs.setdefault('file', stderr)
    kwargs.setdefault('flush', True)
    return print(*args, **kwargs)


def clean_whitespace(text: str, max_empty_lines: Optional[int] = 1) -> str:
    """
    Deindent the text by removing common leading whitespace on each line.
    Also remove leading and trailing empty lines, trailing whitespace on
    each line, and ensure that the text ends with a newline.
    Consecutive empty lines are reduced in number to max_empty_lines unless
    this parameter is None.
    """

    lines: list[str] = []
    empty_lines = 0
    for line in text.rstrip().splitlines():
        line = line.rstrip()
        if line:
            if lines:
                lines.extend(("",) * empty_lines)
            empty_lines = 0
            lines.append(line)
        else:
            empty_lines += 1
            if max_empty_lines is not None and empty_lines > max_empty_lines:
                empty_lines = max_empty_lines

    prefix = commonprefix(list(filter(None, lines)))
    prefix_len = len(prefix) - len(prefix.lstrip())
    return ''.join(line[prefix_len:] + "\n" for line in lines)


def call_if_callable(v, *args, **kwargs):
    return v(*args, **kwargs) if callable(v) else v


class subdict(dict):
    """Subclass dict so that we can weakref it"""

    __slots__ = ('__weakref__',)


def normalize_path(path):
    path = Path(path)
    parts = []
    for part in path.relative_to(path.anchor).parts:
        if part == '..':
            try:
                parts.pop()
            except IndexError:
                pass
        elif part != '.':
            parts.append(part)
    return Path(*parts)


class Ghost:
    """A convenience object that tries to be as inoffensive and easygoing as
    possible, returning a neutral answer from each operation you might try
    to perform on it."""

    __slots__ = ('__weakref__',)

    def __str__(self, *args, **kwargs):
        return ""

    def __repr__(self):
        return '<ghost>'

    def __bytes__(self):
        return b''

    def __buffer__(self, flags):
        return memoryview(b'')

    def __complex__(self):
        return complex()

    def __int__(self):
        return 0

    def __float__(self):
        return 0.0

    def __bool__(self, *args, **kwargs):
        return False

    def __call__(self, *args, **kwargs):
        return self

    def __setitem__(self, *args, **kwargs):
        pass

    def __eq__(self, other):
        return self is other

    def __ne__(self, other):
        return self is not other

    def __iter__(self):
        return iter(())

    async def __anext__(self):
        raise StopAsyncIteration

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        pass

    __getattr__ = __call__
    __setattr__ = __setitem__
    __delattr__ = __setitem__
    __len__ = __int__
    __lt__ = __bool__
    __le__ = __eq__
    __gt__ = __bool__
    __ge__ = __eq__
    __hash__ = __int__
    __getitem__ = __call__
    __delitem__ = __setitem__
    __reversed__ = __iter__
    __contains__ = __bool__
    __add__ = __call__
    __sub__ = __call__
    __mul__ = __call__
    __matmul__ = __call__
    __truediv__ = __call__
    __floordiv__ = __call__
    __mod__ = __call__
    __divmod__ = __call__
    __pow__ = __call__
    __lshift__ = __call__
    __rshift__ = __call__
    __and__ = __call__
    __xor__ = __call__
    __or__ = __call__
    __radd__ = __call__
    __rsub__ = __call__
    __rmul__ = __call__
    __rmatmul__ = __call__
    __rtruediv__ = __call__
    __rfloordiv__ = __call__
    __rmod__ = __call__
    __rdivmod__ = __call__
    __rpow__ = __call__
    __rlshift__ = __call__
    __rrshift__ = __call__
    __rand__ = __call__
    __rxor__ = __call__
    __ror__ = __call__
    __iadd__ = __call__
    __isub__ = __call__
    __imul__ = __call__
    __imatmul__ = __call__
    __itruediv__ = __call__
    __ifloordiv__ = __call__
    __imod__ = __call__
    __ipow__ = __call__
    __ilshift__ = __call__
    __irshift__ = __call__
    __iand__ = __call__
    __ixor__ = __call__
    __ior__ = __call__
    __neg__ = __call__
    __pos__ = __call__
    __abs__ = __call__
    __invert__ = __call__
    __index__ = __int__
    __round__ = __int__
    __trunc__ = __int__
    __floor__ = __int__
    __ceil__ = __int__
    __enter__ = __call__
    __exit__ = __setitem__
    __aiter__ = __call__
    __format__ = __str__


# Singleton
ghost = Ghost()


class Object:
    """Proxy that allows you to treat dict keys as properties and
    returns `ghost` for missing entries. Can also be used to proxy
    lists. All access must be read-only.

    Also records accessed subitems."""

    __slots__ = ('_target', '_members', '_misses')

    _members: dict[Any, 'Object']
    _misses: set[Any]

    def __init__(self, target: Union[dict, list]):
        self._target = target
        self._members = {}
        self._misses = set()

    def get(self, name, default=None):
        try:
            return self[name]
        except LookupError:
            return default

    def _get_accessed(self, prefix=()):
        for name, member in self._members.items():
            member_prefix = (*prefix, name)
            yield member_prefix
            if isinstance(member, Object):
                yield from member._get_accessed(member_prefix)
        for name in self._misses:
            yield (*prefix, name)

    def __getitem__(self, sub):
        members = self._members
        try:
            return members[sub]
        except KeyError:
            pass

        target = self._target
        if isinstance(target, dict) and not isinstance(sub, str):
            return ghost
            # raise TypeError(f"dict key must be a string, not {type(sub).__name__}")

        try:
            member = target[sub]
        except LookupError:
            self._misses.add(sub)
            return ghost

        if isinstance(member, (dict, list)):
            member = type(self)(member)

        if isinstance(target, list) and sub < 0:
            members[len(target) + sub] = member

        members[sub] = member
        return member

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            return ghost

    def __contains__(self, key):
        target = self._target
        if isinstance(target, dict) and not isinstance(sub, str):
            return ghost
            # raise TypeError(f"dict key must be a string, not {type(sub).__name__}")
        return key in target

    def __delattr__(self, key):
        raise NotImplemented("this object is read-only")

    def __delitem__(self, key):
        raise NotImplemented("this object is read-only")

    def __eq__(self, other):
        if isinstance(other, Object):
            return self._target == other._target
        else:
            return self._target == other

    @property
    def __format__(self):
        return self._target.format

    def __ge__(self, other):
        if isinstance(other, Object):
            return self._target >= other._target
        else:
            return self._target >= other

    @property
    def __getstate__(self):
        return self._target.__getstate__

    def __gt__(self, other):
        if isinstance(other, Object):
            return self._target > other._target
        else:
            return self._target > other

    def __hash__(self):
        return hash(self._target)

    def __iter__(self):
        return iter(self._target)

    def __le__(self, other):
        if isinstance(other, Object):
            return self._target <= other._target
        else:
            return self._target <= other

    def __len__(self):
        return len(self._target)

    def __lt__(self, other):
        if isinstance(other, Object):
            return self._target < other._target
        else:
            return self._target < other

    def __ne__(self, other):
        if isinstance(other, Object):
            return self._target != other._target
        else:
            return self._target != other

    def __or__(self, other):
        if isinstance(other, Object):
            return Object(self._target | other._target)
        else:
            return Object(self._target | other)

    __ror__ = __or__
    __ior__ = __or__

    def __add__(self, other):
        if isinstance(other, Object):
            return Object(self._target + other._target)
        else:
            return Object(self._target + other)

    __iadd__ = __add__

    def __mul__(self, other):
        if isinstance(other, Object):
            return Object(self._target * other._target)
        else:
            return Object(self._target * other)

    __imul__ = __mul__
    __rmul__ = __mul__

    @property
    def __reduce__(self):
        return self._target.__reduce__

    @property
    def __reduce_ex__(self):
        return self._target.__reduce_ex__

    def __repr__(self):
        return repr(self._target)

    def __reversed__(self):
        return Object(reversed(self._target))

    def __str__(self):
        return str(self._target)

    def copy(self):
        return Object(self._target)

    __copy__ = copy

    @classmethod
    def fromkeys(cls, *args, **kwargs):
        return cls(dict.fromkeys(*args, **kwargs))

    def items(self):
        return tuple(((key, self[key]) for key in self._target))

    def keys(self):
        return self._target.keys()

    def values(self):
        return tuple((self[key] for key in self._target))

    def index(self, *args):
        return self._target.index(*args)

    def count(self, *args):
        return self._target.count(*args)


def exports(
    locals: Union[dict[str, Any], Iterable[str]],
    imports: Union[set[str], frozenset[str]] = frozenset(),
):
    """Put this just after the imports of your module:

        _imports = frozenset(locals())

    And this at the end:

        __all__ = exports(locals(), _imports)
        del _imports

    This will keep your exports clean of all imported symbols.

    Or use:

        print(__name__, repr(tuple(sorted(exports(locals(), _imports)))))

    To get a name of symbols.
    """

    locals_set: KeysView | frozenset
    if isinstance(locals, dict):
        locals_set = locals.keys()
    else:
        locals_set = frozenset(locals)

    return tuple(v for v in locals_set - imports if not v.startswith('_'))


# __all__ = exports(locals(), _imports)
__all__ = (
    'Ghost',
    'Initializer',
    'Object',
    'call_if_callable',
    'ensure_bytes',
    'exports',
    'fallback',
    'get_file',
    'ghost',
    'initializer',
    'is_byteslike',
    'normalize_path',
    'put_file',
    'subdict',
    'warn',
    'weakproperty',
)
