from json import dumps
from autocracy.common import File, Run, Group, RecursiveFiles, Package
from typing import Any

facts: Any

foo = File(
    destination='/tmp/foo',
    contents="Hello world!\n",
)

bar = File(
    destination='/tmp/bar',
    contents="Hello world?\n",
    only_if=lambda: foo.updated,
)

factsfile = File(
    destination='/tmp/facts',
    contents=f"{facts.uname.version} {facts[4].foo.bar()=}\n",
)

gitignore = File(
    destination='/tmp/gitignore',
    source='.gitignore',
)

bin = RecursiveFiles(
    source='bin',
    destination='/tmp/recursive',
)

hello = Packages(
    install={'hello'},
    gentle=True,
    purge=True,
    recommends=False,
)

grp = Group(
    file1=File(filename='/tmp/1', contents="1\n"),
    file2=File(filename='/tmp/2', contents="2\n"),
)

ls = Run(command='date', only_if=lambda: grp.updated)

grp.file3 = File(filename='/tmp/3', contents="3\n")
