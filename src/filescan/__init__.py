import hashlib
import importlib
import os
import sys
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

from .sqlalchemy_store import Checksum, Database

DEBUG = False  # Think _hard_ before enabling DEBUG

import importlib
import pkgutil

DB_NAME = "alembic"

IGNORE_DIRS = {
    "__pycache__",
    "site-packages",
    ".git",
    ".ipynb_checkpoints",
    ".mypy_cache",
}

discovered_plugins = {
    name: importlib.import_module(name)
    for finder, name, ispkg in pkgutil.iter_modules()
    if name.startswith("filescan_")
}
if discovered_plugins:
    print("Plugins:", ", ".join(name for name in discovered_plugins))
discovered_plugins = list(discovered_plugins.values())


def debug(*args, **kwargs):
    if DEBUG:
        print(*args, **kwargs)


def scan_directory(base_dir: str, conn: Database):
    """
    Recursively traverses a directory, noting which files
    are new since the last scan, which have been modified
    and which have been deleted.
    """
    started: datetime = datetime.now()
    file_count = known_files = updated_files = 0
    unchanged_files = new_files = deleted_files = 0
    base_dir = os.path.abspath(base_dir)
    if not base_dir.endswith("/"):
        base_dir += "/"
    conn.clear_seen_bits(base_dir)

    for dirpath, dirnames, filenames in os.walk(base_dir):
        for ignore_dir in IGNORE_DIRS:
            if ignore_dir in dirnames:
                dirnames.remove(ignore_dir)
        if not dirpath.endswith("/"):
            dirpath = f"{dirpath}/"
        for filename in filenames:
            archive_data = None
            file_count += 1
            current_file_path = os.path.join(dirpath, filename)
            stat = os.stat(current_file_path, follow_symlinks=False)
            disk_modified = stat.st_mtime
            size = stat.st_size
            try:  # Known file happy path
                loc = conn.location_for(dirpath, filename)
                id, checksum, seen = loc.id, loc.checksum, loc.seen
                known_files += 1
                if disk_modified != loc.modified:  # Changed since last scan
                    updated_files += 1
                    cs = conn.register_hash(current_file_path)
                    loc = conn.update_details(loc, disk_modified, cs, size)
                    for plugin in discovered_plugins:
                        plugin.process(conn, loc)
                    debug("*UPDATED*", current_file_path)
                    archive_data = ("UPDATED", "location", loc)
                else:
                    unchanged_files += 1
                    conn.update_seen(loc)
            except conn.DoesNotExist:  # New file
                new_files += 1
                cs = conn.register_hash(current_file_path)
                loc = conn.insert_location(
                    dirpath=dirpath,
                    filename=filename,
                    modified=disk_modified,
                    checksum=cs,
                    filesize=size,
                )
                for plugin in discovered_plugins:
                    plugin.process(conn, loc)
                debug("*CREATED*", current_file_path)
                archive_data = ("CREATED", "location", loc)
            conn.commit()
            if archive_data:
                conn.archive_record(*archive_data)
    ct = conn.all_file_count(base_dir)
    deleted_files = conn.unseen_location_count(base_dir)
    for loc in conn.unseen_locations(base_dir):
        debug(f"*DELETED* {loc.dirpath}{loc.filename}")
        conn.archive_record("DELETED", "location", loc)
    conn.delete_unseen_locations(base_dir)
    conn.record_run(
        started,
        base_dir,
        file_count,
        known_files,
        updated_files,
        unchanged_files,
        new_files,
        deleted_files,
    )
    conn.commit()

    print(
        f"""\
Known:      {known_files:7,d}
Unchanged:  {unchanged_files:7,d}
Updated:    {updated_files:7,d}
New:        {new_files:7,d}
Deleted:    {deleted_files:7,d}
-------------------
Total seen: {file_count:7,d}
==================="""
    )


def main(
    args=sys.argv[1:],
    DEBUG=True,
    create=False,
):
    if len(sys.argv) == 1:
        sys.exit("Nothing to do!")
    conn = Database(dbname=DB_NAME)

    print(f"Using production database {DB_NAME}")
    for base_dir in args:
        scan_directory(base_dir, conn)


if __name__ == "__main__":
    main()
