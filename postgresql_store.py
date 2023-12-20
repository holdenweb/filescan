import psycopg2
from datetime import datetime


class Connection:
    class DoesNotExist(Exception):
        pass

    def __init__(self, dbname="test", create=False):
        self.dbname = dbname
        if create:
            self.create_db()
        self.conn = psycopg2.connect(f"dbname={dbname}")
        self.curs = self.conn.cursor()
        if create:
            self.curs.execute("DROP TABLE IF EXISTS tokenpos")
            self.curs.execute(
                "CREATE TABLE tokenpos (id SERIAL PRIMARY KEY, hash CHAR(64), name VARCHAR, line INTEGER, pos INTEGER)"
            )
            self.curs.execute("DROP TABLE IF EXISTS location")
            self.curs.execute(
                "CREATE TABLE location ("
                "id SERIAL PRIMARY KEY, "
                "filename VARCHAR, dirpath VARCHAR, "
                "modified DOUBLE PRECISION, checksum CHAR(64), "
                "seen BOOLEAN)"
            )
            self.curs.execute("ALTER TABLE location ADD COLUMN filesize INTEGER")
            self.curs.execute("DROP TABLE IF EXISTS runlog")
            self.curs.execute(
                "CREATE TABLE runlog ("
                "id SERIAL PRIMARY KEY, "
                "when_run TIMESTAMP, "
                "files INTEGER, "
                "known INTEGER, "
                "updated INTEGER, "
                "unchanged INTEGER, "
                "new_files INTEGER, "
                "deleted INTEGER)"
            )
            self.curs.execute("ALTER TABLE runlog ADD COLUMN rootdir VARCHAR")

    def create_db(self):
        m_conn = psycopg2.connect(dbname="postgres")
        m_conn.autocommit = True
        m_curs = m_conn.cursor()
        m_curs.execute(f"DROP DATABASE IF EXISTS {self.dbname}")
        m_curs.execute(f"CREATE DATABASE {self.dbname}")
        m_conn.close()

    def commit(self):
        return self.conn.commit()

    def hash_exists(self, hash):
        self.curs.execute("SELECT id FROM tokenpos WHERE hash = %s LIMIT 1", (hash,))
        return len(self.curs.fetchall()) > 0

    def save_reference(self, hash, name, line, pos):
        self.curs.execute(
            "INSERT INTO tokenpos (hash, name, line, pos) VALUES(%s, %s, %s, %s)",
            (hash, name, line, pos),
        )

    def clear_seen_bits(self, prefix):
        self.curs.execute(
            "UPDATE location SET seen=FALSE WHERE dirpath LIKE (%s || '%%')", (prefix,)
        )

    def location_for(self, dir_path, file_path):
        self.curs.execute(
            """
        SELECT id, modified, checksum, seen FROM location
        WHERE dirpath=%s AND filename=%s""",
            (dir_path, file_path),
        )
        result = self.curs.fetchone()
        if result:
            return result
        else:
            raise Connection.DoesNotExist()

    def update_modified_hash_seen(self, id, modified, hash, seen=True):
        self.curs.execute(
            """
        UPDATE location \
        SET modified=%s, checksum=%s, seen=%s \
                WHERE id=%s""",
            (modified, hash, seen, id),
        )

    def update_seen(self, id):
        self.curs.execute(
            """
        UPDATE location SET seen=TRUE
                WHERE id=%s""",
            (id,),
        )

    def db_insert_location(self, file_path, dir_path, disk_modified, hash, size):
        self.curs.execute(
            """
        INSERT INTO location (filename, dirpath, modified, checksum, seen, filesize)
        VALUES (%s, %s, %s, %s, TRUE, %s)""",
            (file_path, dir_path, disk_modified, hash, size),
        )

    def all_file_count(self, prefix):
        self.curs.execute("SELECT count(*) FROM location WHERE dirpath LIKE %s", (f"{prefix}%", ))
        return self.curs.fetchone()[0]

    def count_not_seen(self, prefix):
        self.curs.execute("""SELECT count(*) FROM location WHERE NOT seen AND dirpath LIKE %s""", (f"{prefix}%", ))
        return self.curs.fetchone()[0]

    def dir_files_not_seen(self, prefix):
        self.curs.execute("""SELECT dirpath, filename FROM location WHERE dirpath LIKE %s AND NOT seen""", (f"{prefix}%", ))
        return self.curs.fetchmany()

    def delete_not_seen(self, prefix):
        self.curs.execute("""DELETE from location WHERE NOT seen AND dirpath LIKE %s""", (f"{prefix}%", ))

    def record_run(
        self,
        when: datetime,
        rootdir: str,
        files: int,
        known: int,
        updated: int,
        unchanged: int,
        new: int,
        deleted: int,
    ):
        self.curs.execute(
            "INSERT INTO runlog (when_run, rootdir, files, known, updated, unchanged, new_files, deleted) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (when, rootdir, files, known, updated, unchanged, new, deleted),
        )
        self.commit()
