import datetime
import glob
import hashlib
import os
import time

import sqlite_store as store

DEBUG = True # Think _hard_ before enabling DEBUG

def debug(*args, **kwargs):
    if DEBUG:
        print(*args, **kwargs)


base_dirs = ['/Users/sholden/Projects/Python/filescan']
conn = store.Connection('test')
conn.clear_seen_bits()

for base_dir in base_dirs:
    file_count = known_files = updated_files = unchanged_files = new_files = deleted_files = 0
    for rec in os.walk(base_dir):
        dir_path, dirnames, filenames = rec
        for file_path in filenames:
            file_count += 1
            thisfile = os.path.join(dir_path, file_path)
            stat = os.stat(thisfile, follow_symlinks=False)
            disk_modified = stat.st_mtime
            try:
                id, modified, seen = conn.id_mod_seen(dir_path, file_path)
                known_files += 1
                if disk_modified != modified: # Changed since last scan
                    debug(f"Modified is now {disk_modified}({type(disk_modified)}) was {modified}({type(modified)})")
                    updated_files += 1
                    hash = hashlib.sha256(open(thisfile, "rb").read()).hexdigest()
                    conn.update_modified_hash_seen(id, disk_modified, hash)
                    debug("*UPDATED*", thisfile)
                else:
                    debug("*REMAINS*", thisfile)
                    unchanged_files += 1
                    conn.update_seen(id)
            except store.DoesNotExist:   # New file
                new_files += 1
                try:
                    hash = hashlib.sha256(open(thisfile, "rb").read()).hexdigest()
                except FileNotFoundError:
                    hash = "UNHASHABLE"
                conn.db_insert_location(file_path, dir_path, disk_modified, hash)
                debug("*CREATED*", thisfile)
ct = conn.all_file_count()
deleted_files = conn.count_not_seen()
for dirname, filepath in conn.dir_files_not_seen():
    debug("*DELETED*", os.path.join(dirname, filepath))
conn.delete_not_seen()
conn.commit()

print(f"""
Known:     {known_files}
New:       {new_files}
Deleted:   {deleted_files}
Updated:   {updated_files}
Unchanged: {unchanged_files}

Total:     {file_count}""")
