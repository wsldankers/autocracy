from configparser import ConfigParser
from collections.abc import Mapping, Iterable
from io import StringIO
from lxml.etree import Element, _Element, ElementTree
from json import dumps
from re import compile as regcomp
from os import linesep


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
    def __init__(
        self,
        *args,
        newline=None,
        key_separator=' = ',
        value_separator=' ',
        continuation_indent="\t",
    ):
        super().__init__(*args)
        self.newline = newline
        self.key_separator = key_separator
        self.value_separator = value_separator
        self.continuation_indent = continuation_indent

    def __str__(self):
        newline = self.newline
        if newline is None:
            newline = linesep
        key_separator = self.key_separator
        value_separator = self.value_separator
        continuation_indent = self.continuation_indent

        with StringIO() as fh:

            def write_key_value(key, value):
                if value is None:
                    print(key, end=newline, file=fh)
                else:
                    if not isinstance(value, str):
                        if isinstance(value, Iterable):
                            if value_separator is None:
                                for v in value:
                                    write_key_value(key, v)
                                return
                            value = value_separator.join(value)
                        else:
                            value = str(value)

                    iterator = iter(_newline_split(value))

                    print(
                        key, key_separator, next(iterator), sep='', end=newline, file=fh
                    )
                    for line in iterator:
                        print(
                            continuation_indent,
                            next(iterator),
                            sep='',
                            end=newline,
                            file=fh,
                        )

            sections = []
            for key, value in self.items():
                if isinstance(value, Mapping):
                    sections.append((key, value))
                else:
                    write_key_value(key, value)

            for name, section in sections:
                if fh.tell():
                    print("", end=newline, file=fh)
                print(f"[{name}]", end=newline, file=fh)
                for key, value in section.items():
                    write_key_value(key, value)

            return fh.getvalue()


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
