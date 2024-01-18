## filescan: track files and python variable usage

We assume your account has sufficiant permissions to create,
delete and modify the necessary database objects in the
database you select.

Under poetry control run

    python filescan [path ...]

You will be asked which database technology you want to use.
Enter one of `postgresql`, `sqlite` or `mongo`.

It then asks you for a database name. Your choice.

Finally it asks whether you want to create a new database.
Unless the response is the three literal characters "yes"
the program assumes you wish to use an existing database.
A "yes" answer will require confirmation in the same way,
since it will delete any existing database of the same name.

The above is slightly misleading, as the present code is
not capable of dropping and creating databases, so it should
really talk about creating the tables, rather than the
whole database. This should be fixed in short order.

The program then proceeds to scan the filestore starting
from each of the paths given on the command line. Any
directories it encounters with the names _.git_,
_\_\_pycache\_\__ or _site\_packages_ will be ignored.
\[TODO: make the ignored directory list configurable].

Any Python files encountered are tokenised and indexed
on all names used other than Python keywords, recording
the file path, line number and character position of
each occurrence. This has now been modified to use the
new plugin architecture (see below).

Processing specific file types with plugins
-------------------------------------------

When filescan runs, it searches for a list of importable
modules or packages with names matching "filescan_* will
be imported and their `process` function will be called with
the connection object as the first argument and the relevant
Location object as the second.

Each new or modified file processed
