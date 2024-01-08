from json import dumps
from autocracy.common import File, Run, Group
from typing import Any

facts: Any

foo = File(
    filename='/tmp/foo',
    contents="Hello world!\n",
)

bar = File(
    filename='/tmp/bar',
    contents="Hello world?\n",
    only_if=lambda: foo.updated,
)

factsfile = File(
    filename='/tmp/facts',
    contents=f"{facts.uname.version} {facts[4].foo.bar()=}\n",
)

gitignore = File(
    filename='/tmp/gitignore',
    source='.gitignore',
)

grp = Group(
    file1=File(filename='/tmp/1', contents="1\n"),
    file2=File(filename='/tmp/2', contents="2\n"),
)

ls = Run(command='date', only_if=lambda: grp.updated)

grp.file3 = File(filename='/tmp/3', contents="3\n")
