import hashlib
import importlib
import os
import sys

from datetime import datetime

from load_tokens import scan_tokens

DEBUG = False # Think _hard_ before enabling DEBUG

def debug(*args, **kwargs):
    if DEBUG:
        print(*args, **kwargs)

def main(args=sys.argv[1:], DEBUG=False, storage='postgresql', database='filescan', create=False):

    if create:
        answer = input(f"""
This operation will destroy any existing
database with the following characteristics:

Storage:    {storage}
Database:   {database}

Do you wish to proceed (yes/no): """)
        if answer != "yes":
            sys.exit("Aborted: user opted not to create a new database.")


    store_name = f"{storage}_store"
    print("Using", store_name, "with DB", database)
    store = importlib.import_module(store_name)
    conn = store.Connection(database, create=create)

    file_count = known_files = updated_files = unchanged_files = new_files = deleted_files = 0
    started = datetime.now()
    for base_dir in args:

        if not base_dir.endswith("/"):
            base_dir += "/"
        conn.clear_seen_bits(base_dir)

        for dir_path, dirnames, filenames in os.walk(base_dir):
            for ignore_dir in ["__pycache__", "site-packages", ".git"]:
                if ignore_dir in dirnames:
                    dirnames.remove(ignore_dir)
            if not dir_path.endswith("/"):
                dir_path = f"{dir_path}/"
            for filename in filenames:
                file_count += 1
                current_file_path = os.path.join(dir_path, filename)
                stat = os.stat(current_file_path, follow_symlinks=False)
                disk_modified = stat.st_mtime
                try:
                    id, modified, hash, seen = conn.id_mod_hash_seen(dir_path, filename)
                    known_files += 1
                    if disk_modified != modified: # Changed since last scan
                        #  print(f"{dir_path}/{file_path} is now {disk_modified} was {modified}")
                        updated_files += 1
                        hash = hashlib.sha256(open(current_file_path, "rb").read()).hexdigest()
                        conn.update_modified_hash_seen(id, disk_modified, hash)
                        scan_tokens(conn, current_file_path, hash)
                        # debug("*UPDATED*", thisfile)
                    else:
                        unchanged_files += 1
                        conn.update_seen(id)
                except conn.DoesNotExist:   # New file
                    new_files += 1
                    try:
                        hash = hashlib.sha256(open(current_file_path, "rb").read()).hexdigest()
                        scan_tokens(conn, current_file_path, hash)
                    except FileNotFoundError:
                        hash = "UNHASHABLE"
                    conn.db_insert_location(filename, dir_path, disk_modified, hash)
                    debug("*CREATED*", current_file_path)
                conn.commit()
    ct = conn.all_file_count()
    deleted_files = conn.count_not_seen()
    for dirname, filepath in conn.dir_files_not_seen():
        debug("*DELETED*", os.path.join(dirname, filepath))
    conn.delete_not_seen()
    conn.commit()

    print(f"""\
Known:      {known_files}
Updated:    {updated_files}
Unchanged:  {unchanged_files}
New:        {new_files}
Deleted:    {deleted_files}

Total seen: {file_count}""")

    conn.record_run(started, file_count, known_files, updated_files, unchanged_files, new_files, deleted_files)

if __name__ == '__main__':

    if len(sys.argv) == 1:
       sys.exit("Nothing to do!")

    args = {}
    for name in "storage", "database", "create":
        args[name] = input(f"{name.capitalize()}: ")
    args['create'] = (args['create'] == 'yes')
    main(**args)
