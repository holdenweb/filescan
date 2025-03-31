## filescan: track files and Python name usage

We assume your account has sufficient permissions to create,
delete and modify the necessary database objects in the
database you select.

filescan uses the [`python-dotenv`](https://pypi.org/project/python-dotenv/)
module, meaning you can affect your program's environment by editing the
value of `DBNAME` in the _.env_ file.


### Creating a database

At present the required database must exist before filescan
runs, so you'll need to create it manually some other way.

Once created, to add the tables run the command

    poetry run alembic upgrade head

### Runing the prograM

Run the command

    poetry run python -m filescan [path ...]

Each of the arguments should be directory.
By default the system uses a database called "default_db".
You can change this by setting the DBNAME environment
variable.

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
modules or packages with names matching "filescan_*.
All such modules will
be imported and their `process` function will be called with
the connection object as the first argument and the relevant
Location object as the second for each new or modified file
encountered.
