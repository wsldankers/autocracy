from collections.abc import Iterable, Mapping, Reversible
from configparser import ConfigParser
from io import IOBase, StringIO
from json import dumps
from os import linesep
from re import compile as regcomp
from shlex import quote as quote_shell
from typing import Callable, Optional

from lxml.etree import Element, ElementTree, _Element


class INI(ConfigParser):
    def __init__(self, initial_content=None, /, **kwargs):
        super().__init__(**kwargs)
        if isinstance(initial_content, Mapping):
            self.read_dict(initial_content)
        elif isinstance(initial_content, str):
            self.read_string(initial_content)
        elif initial_content is not None:
            self.read_file(initial_content)

    def __str__(self):
        with StringIO() as fh:
            self.write(fh)
            return fh.getvalue()


_newline_split = regcomp(r'\r?\n').split


class KeyValue(dict):
    newline = linesep
    key_separator = ' = '
    value_separator: Optional[str] = ' '
    continuation_indent = "\t"
    buffer_io_class: Callable[[], IOBase] = StringIO

    print = print

    def newline_split(self, value):
        return _newline_split(value)

    def print_empty_value(self, fh: IOBase, key: str) -> None:
        self.print(key, end=self.newline, file=fh)

    def print_single_value(self, fh: IOBase, key: str, value: str) -> None:
        iterator = iter(self.newline_split(value))

        newline = self.newline
        self.print(key, next(iterator), sep=self.key_separator, end=newline, file=fh)
        continuation_indent = self.continuation_indent
        for line in iterator:
            print(continuation_indent, line, sep='', end=newline, file=fh)

    def print_value(self, fh, key, value) -> None:
        if value is None:
            self.print_empty_value(fh, key)
        elif isinstance(value, Iterable) and not isinstance(value, str):
            self.print_list_value(fh, key, value)
        else:
            self.print_single_value(fh, key, str(value))

    def print_list_value(self, fh, key, values) -> None:
        if not isinstance(values, Reversible):
            # If it's not reversible it probably doesn't have a defined order.
            # We need the process to be deterministic though, to prevent
            # edict.updated from becoming true spuriously.
            values = sorted(values)
        value_separator = self.value_separator
        if value_separator is None:
            for value in values:
                self.print_value(fh, key, value)
        else:
            self.print_single_value(fh, key, value_separator.join(values))

    def print_start_section(self, fh, name) -> None:
        newline = self.newline
        if fh.tell():
            print(end=newline, file=fh)
        print(f"[{name}]", end=newline, file=fh)

    def print_end_section(self, fh, name) -> None:
        pass

    def print_section(self, fh, name, section) -> None:
        for key, value in section.items():
            self.print_value(fh, key, value)

    def __str__(self):
        with self.buffer_io_class() as fh:
            sections = []
            for key, value in self.items():
                if isinstance(value, Mapping):
                    sections.append((key, value))
                else:
                    self.print_value(fh, key, value)

            for name, section in sections:
                self.print_start_section(fh, name)
                self.print_section(fh, name, section)
                self.print_end_section(fh, name)

            return fh.getvalue()


class SystemdUnit(KeyValue):
    key_separator = '='
    value_separator = None


class ResolvConf(KeyValue):
    key_separator = ' '
    value_separator = None


class ShellEnv(KeyValue):
    key_separator = '='
    continuation_indent = ''

    def print_single_value(self, fh: IOBase, key: str, value: str) -> None:
        self.print(
            key,
            quote_shell(value),
            sep=self.key_separator,
            end=self.newline,
            file=fh,
        )


class SshConfig(KeyValue):
    key_separator = ' '
    value_separator = None


class SshdConfig(SshConfig):
    pass


class XML(list):
    def __init__(self, *args, **kwargs):
        super().__init__(args)
        kwargs.setdefault('xml_declaration', True)
        kwargs.setdefault('pretty_print', True)
        self.options = kwargs

    def __str__(self):
        """
        Create an Element according to specifications.

        ['foo', {'x': 'y'}, 'bar'] => <foo x="y">bar</foo>
        ['foo', ['bar', 'baz']] => <foo><bar>baz</bar></foo>
        """

        args = iter(self)
        name = next(args)
        assert isinstance(name, str)

        children = []
        attrib = {}
        text = ''

        for arg in args:
            if isinstance(arg, (list, tuple)):
                child = xml(*arg)
                if child is not None:
                    children.append(child)
            elif isinstance(arg, _Element):
                children.append(arg)
            elif isinstance(arg, Mapping):
                for key, value in arg.items():
                    if value is None:
                        attrib.pop(key, None)
                    else:
                        attrib[key] = value
            else:
                arg = str(arg)
                if arg:
                    if children:
                        last_child = children[-1]
                        last_child.tail = (last_child.tail or '') + arg
                    else:
                        text += arg

            node = Element(name, attrib=attrib, **kwargs)
            if text:
                node.text = text
            for child in children:
                node.append(child)

        with StringIO() as fh:
            ElementTree(node).write(fh, **self.options)
            return fh.getvalue()


class JSON(dict):
    def __str__(self):
        return dumps(self, indent="\t")


class JSONlist(list):
    def __str__(self):
        return dumps(self, indent="\t")
