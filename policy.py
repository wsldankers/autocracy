from json import dumps
from autocracy.edicts import File, Run, Group, RecursiveFiles, Packages
from typing import Any

facts: Any

foo = File(
    target='/tmp/foo',
    contents="Hello world!\n",
)

bar = File(
    target='/tmp/bar',
    contents="Hello world?\n",
    only_if=lambda: foo.updated,
)

factsfile = File(
    target='/tmp/facts',
    contents=f"{facts.uname.version} {facts[4].foo.bar()=}\n",
)

gitignore = File(
    target='/tmp/gitignore',
    source='.gitignore',
)

bin = RecursiveFiles(
    target='/tmp/recursive',
    source='bin',
    mode='755',
)

hello = Packages(
    remove={'hello'},
    gentle=True,
    purge=True,
    recommends=False,
)

grp = Group(
    file1=File(target='/tmp/1', contents="1\n"),
    file2=File(target='/tmp/2', contents="2\n"),
)

ls = Run(command='date', only_if=lambda: grp.updated)

grp.file3 = File(target='/tmp/3', contents="3\n")
