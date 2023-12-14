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

Any Python files encountered are tokenised and indexed
on all names used other than Python keywords, recording
the file path, line number and character position of
each occurrence.
