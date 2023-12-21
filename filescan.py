import hashlib
import importlib
import os
import sys

from datetime import datetime

from load_tokens import scan_tokens

DEBUG = False  # Think _hard_ before enabling DEBUG


def debug(*args, **kwargs):
    if DEBUG:
        print(*args, **kwargs)


def scan_directory(base_dir, conn):
    started = datetime.now()
    file_count = known_files = updated_files = unchanged_files = new_files = deleted_files = 0
    base_dir = os.path.abspath(base_dir)
    if not base_dir.endswith("/"):
        base_dir += "/"
    conn.clear_seen_bits(base_dir)

    for dir_path, dirnames, filenames in os.walk(base_dir):
        for ignore_dir in [
            "__pycache__",
            "site-packages",
            ".git",
            ".ipynb_checkpoints",
            ".mypy_cache",
        ]:
            if ignore_dir in dirnames:
                dirnames.remove(ignore_dir)
        if not dir_path.endswith("/"):
            dir_path = f"{dir_path}/"
        for filename in filenames:
            file_count += 1
            current_file_path = os.path.join(dir_path, filename)
            stat = os.stat(current_file_path, follow_symlinks=False)
            disk_modified = stat.st_mtime
            size = stat.st_size
            try:
                loc = conn.location_for(dir_path, filename)
                id, hash, seen = loc.id, loc.checksum, loc.seen
                known_files += 1
                if disk_modified != loc.modified:  # Changed since last scan
                    updated_files += 1
                    hash = hashlib.sha256(
                        open(current_file_path, "rb").read()
                    ).hexdigest()
                    loc = conn.update_details(loc, disk_modified, hash, size)
                    scan_tokens(conn, current_file_path, hash)
                    debug("*UPDATED*", current_file_path)
                    conn.archive_record("UPDATED", 'location', loc)
                else:
                    unchanged_files += 1
                    conn.update_seen(loc)
            except conn.DoesNotExist:  # New file
                new_files += 1
                try:
                    hash = hashlib.sha256(
                        open(current_file_path, "rb").read()
                    ).hexdigest()
                    scan_tokens(conn, current_file_path, hash)
                except FileNotFoundError:
                    hash = "UNHASHABLE"
                loc = conn.db_insert_location(dir_path, filename, disk_modified, hash, size)
                debug("*CREATED*", current_file_path)
                conn.archive_record("CREATED", 'locqtion', loc)
            conn.commit()
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
Known:      {known_files:6,d}
Unchanged:  {unchanged_files:6,d}
Updated:    {updated_files:6,d}
New:        {new_files:6,d}
Deleted:    {deleted_files:6,d}
------------------
Total seen: {file_count:6,d}
=================="""
    )


def main(
    args=sys.argv[1:],
    DEBUG=True,
    storage="postgresql",
    database="filescan",
    create=False,
):

    store_name = f"{storage}_store"
    store = importlib.import_module(store_name)
    conn = store.Connection(database, create=create)

    for base_dir in args:
        scan_directory(base_dir, conn)


if __name__ == "__main__":

    if len(sys.argv) == 1:
        sys.exit("Nothing to do!")

    main(storage="sqlalchemy", database="sa", create=False)
