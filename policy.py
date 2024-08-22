from typing import Any

from autocracy.decrees import File, Group, Packages, RecursiveFiles, Run

reports: Any

foo = File(
    target='/tmp/foo',
    contents="Hello world!\n",
)

bar = File(
    target='/tmp/bar',
    contents="Hello world?\n",
    activate_if=lambda: foo.updated,
)

reportsfile = File(
    target='/tmp/reports',
    contents=f"{reports.uname.version} {reports[4].foo.bar()=}\n",
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

ls = Run(command='date', activate_if=lambda: grp.updated)

grp.file3 = File(target='/tmp/3', contents="3\n")
