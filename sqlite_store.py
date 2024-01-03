import sqlite3


class Connection:
    class DoesNotExist(Exception):
        pass

    def __init__(self, dbname="test", create=False):
        self.conn = sqlite3.connect(f"/Users/sholden/Desktop/{dbname}.sqlite")
        if create:
            self.conn.execute("DROP TABLE IF EXISTS location")
            self.conn.execute(
                "CREATE TABLE location (id INTEGER PRIMARY KEY, filename VARCHAR, dirpath varchar, modified number, checksum integer, seen boolean)"
            )
            self.conn.execute("ALTER TABLE location ADD COLUMN filesize INTEGER")

    def commit(self):
        return self.conn.commit()

    def clear_seen_bits(self, prefix):
        self.conn.execute(
            "UPDATE LOCATION SET seen=FALSE WHERE dirpath LIKE (%s || '%%')", (dirpath,)
        )

    def id_mod_seen(self, dir_path, file_path):
        curs = self.conn.execute(
            """
        SELECT id, modified, seen FROM location
        WHERE dirpath=? AND filename=?""",
            (dir_path, file_path),
        )
        result = curs.fetchone()
        if result:
            return result
        else:
            raise Connection.DoesNotExist()

    def update_modified_hash_seen(self, id, modified, checksum, seen=True):
        curs = self.conn.execute(
            """
        UPDATE location \
        SET modified=?, checksum=?, seen=? \
                WHERE id=?""",
            (modified, checksum, seen, id),
        )

    def update_seen(self, id):
        curs = self.conn.execute(
            """
        UPDATE location SET seen=TRUE
                WHERE id=?""",
            (id,),
        )

    def insert_location(self, file_path, dir_path, disk_modified, checksum):
        self.conn.execute(
            """
        INSERT INTO location (filename, dirpath, modified, checksum, seen)
        VALUES (?, ?, ?, ?, TRUE)""",
            (file_path, dir_path, disk_modified, checksum),
        )

    def all_file_count(self):
        curs = self.conn.execute("SELECT count(*) FROM location")
        return curs.fetchone()[0]

    def unseen_location_count(self):
        curs = self.conn.execute("""SELECT count(*) FROM location WHERE NOT seen""")
        return curs.fetchone()[0]

    def dir_files_not_seen(self):
        curs = self.conn.execute(
            """SELECT dirpath, filename FROM location WHERE NOT seen"""
        )
        return curs.fetchmany()

    def delete_unseen_locations(self):
        curs = self.conn.execute("""DELETE from location WHERE NOT seen""")
