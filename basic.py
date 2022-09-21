import datetime
import glob
import hashlib
import os
import sqlite3
import time

DEBUG = True # Think _hard_ before enabling DEBUG

def connect(dbname='test', create=False):
    conn = sqlite3.connect('/Users/sholden/Desktop/{dbname}.sqlite')
    if create:
        conn.execute("DROP TABLE IF EXISTS location")
        conn.execute("CREATE TABLE location (id INTEGER PRIMARY KEY, filename VARCHAR, dirpath varchar, modified datetime, checksum integer, seen boolean)")
        conn.execute("ALTER TABLE location ADD COLUMN length INTEGER")
    return conn

def debug(*args, **kwargs):
    if DEBUG:
        print(*args, **kwargs)

def clear_seen_bits():
    conn.execute('UPDATE LOCATION SET seen=FALSE')

def id_mod_seen(dir_path, file_path):
    curs = conn.execute('''
    SELECT id, modified, seen FROM location
    WHERE dirpath=? AND filename=?''',
                       (dir_path, file_path))
    return curs.fetchone()

def update_modified_hash_seen(id, modified, hash, seen=True):
    curs = conn.execute('''
    UPDATE location \
    SET modified=?, checksum=?, seen=? \
            WHERE id=?''',
            (modified, hash, seen, id))

def update_seen(id):
    curs = conn.execute('''
    UPDATE location SET seen=TRUE
            WHERE id=?''',
            (id, ))

def db_insert_location(file_path, dir_path, disk_modified, hash):
    conn.execute('''
    INSERT INTO location (filename, dirpath, modified, checksum, seen)
    VALUES (?, ?, ?, ?, TRUE)''',
    (file_path, dir_path, disk_modified, hash))

def all_file_count():
    curs = conn.execute("SELECT count(*) FROM location")
    return curs.fetchone()[0]

def count_not_seen():
    curs = conn.execute('''SELECT count(*) FROM location WHERE NOT seen''')
    return curs.fetchone()[0]

def dir_files_not_seen():
    curs = conn.execute('''SELECT dirpath, filename FROM location WHERE NOT seen''')
    return curs.fetchmany()

def delete_not_seen():
    curs = conn.execute('''DELETE from location WHERE NOT seen''')


base_dirs = ['/Users/sholden/Projects/Python/filescan']
conn = connect()
clear_seen_bits()

for base_dir in base_dirs:
    file_count = known_files = updated_files = unchanged_files = new_files = deleted_files = 0
    for rec in os.walk(base_dir):
        dir_path, dirnames, filenames = rec
        for file_path in filenames:
            file_count += 1
            thisfile = os.path.join(dir_path, file_path)
            stat = os.stat(thisfile, follow_symlinks=False)
            disk_modified = f"{datetime.datetime.fromtimestamp(stat.st_mtime, None)}"
            rec = id_mod_seen(dir_path, file_path)
            if rec: # Known file
                id, modified, seen = rec
                known_files += 1
                if disk_modified != modified: # Changed since last scan
                    debug(f"Modified was {disk_modified}({type(disk_modified)}) is now {modified}({type(modified)})")
                    updated_files += 1
                    hash = hashlib.sha256(open(thisfile, "rb").read()).hexdigest()
                    update_modified_hash_seen(id, modified, hash)
                    debug("*UPDATED*", thisfile)
                else:
                    debug("*REMAINS*", thisfile)
                    unchanged_files += 1
                    update_seen(id)
            else:   # New file
                new_files += 1
                try:
                    hash = hashlib.sha256(open(thisfile, "rb").read()).hexdigest()
                except FileNotFoundError:
                    hash = "UNHASHABLE"
                db_insert_location(file_path, dir_path, disk_modified, hash)
                debug("*CREATED*", thisfile)
ct = all_file_count()
deleted_files = count_not_seen()
for dirname, filepath in dir_files_not_seen():
    debug("*DELETED*", os.path.join(dirname, filepath))
delete_not_seen()
conn.commit()

print(f"""
Known:     {known_files}
New:       {new_files}
Deleted:   {deleted_files}
Updated:   {updated_files}
Unchanged: {unchanged_files}

Total:     {file_count}""")
