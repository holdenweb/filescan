from datetime import datetime

from sqlalchemy import (
    create_engine,
    MetaData,
    String,
    Float,
    Boolean,
    DateTime,
    update,
    select,
    exists,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
from sqlalchemy.exc import NoResultFound
from dotenv import load_dotenv

DB_URL_FMT = "postgresql+psycopg2://localhost:5432/{}"


class Model(DeclarativeBase):
    metadata = MetaData(
        naming_convention={
            "ix": "ix_%column_0_label)s",
            "uq": "uq_%(table_name)s_%(column_0_name)s",
            "ck": "ck_%(table_name)s_%(constraint_name)s",
            "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
            "pk": "pk_%(table_name)s`",
        }
    )


class Location(Model):
    __tablename__ = "location"
    id: Mapped[int] = mapped_column(primary_key=True)
    filename: Mapped[str] = mapped_column(String())
    dirpath: Mapped[str] = mapped_column(String())
    modified: Mapped[float] = mapped_column(Float())
    checksum: Mapped[str] = mapped_column(String())
    seen: Mapped[bool] = mapped_column(Boolean())
    filesize: Mapped[int]


class TokenPos(Model):
    __tablename__ = 'tokenpos'
    id: Mapped[int] = mapped_column(primary_key=True)
    hash: Mapped[str] = mapped_column(String(64))
    name: Mapped[str] = mapped_column(String())
    line: Mapped[int]
    pos: Mapped[int]


class RunLog(Model):
    __tablename__ = "runlog"
    id: Mapped[int] = mapped_column(primary_key=True)
    when_run: Mapped[datetime] = mapped_column(DateTime)
    rootdir: Mapped[str]
    files: Mapped[int]
    known: Mapped[int]
    updated: Mapped[int]
    unchanged: Mapped[int]
    new_files: Mapped[int]
    deleted: Mapped[int]


class Connection:

    class DoesNotExist(Exception):
        ...

    def __init__(self, dbname="sa", create=False):
        self.dbname = dbname
        # if create:
        #    self.create_db()
        self.db_url = DB_URL_FMT.format(dbname)
        self.engine = create_engine(self.db_url, echo=False)
        self.session = sessionmaker(self.engine)()
        if create:
            Model.metadata.drop_all(self.engine)
            Model.metadata.create_all(self.engine)

    def create_db(self):
        raise NotImplementedError("Sorry, Dave, I'm afraid I can't do that.")
        m_conn = psycopg2.connect(dbname="postgres")
        m_conn.autocommit = True
        m_curs = m_conn.cursor()
        m_curs.execute(f"DROP DATABASE IF EXISTS {self.dbname}")
        m_curs.execute(f"CREATE DATABASE {self.dbname}")
        m_conn.close()

    def commit(self):
         return self.session.commit()

    def hash_exists(self, hash):
        q = select(exists().where(Location.checksum == hash))
        return self.session.scalar(q)

    def save_reference(self, hash, name, line, pos):
        t = TokenPos(hash=hash, name=name, line=line, pos=pos)
        self.session.add(t)

    def clear_seen_bits(self, prefix):
        q = update(Location).where(Location.dirpath.like(f"{prefix}%")).values(seen=False)
        return self.session.execute(q)

    def id_mod_hash_seen(self, dirpath, filename):
        try:
            q = select(
                Location
            ).where(Location.dirpath == dirpath, Location.filename == filename)
            result = self.session.scalars(q).one()
            return result
        except NoResultFound:
            raise self.DoesNotExist

    def update_modified_hash_size(self, id, modified, hash, size, seen=True):
        q = select(Location).where(Location.id == id)
        loc = self.session.scalars(q).one()
        loc.modified = modified
        loc.checksum = hash
        loc.filesize = size
        loc.seen = seen
        self.session.add(loc)

    def update_seen(self, loc, value=True):
        loc.seen = value
        self.session.add(loc)

    def db_insert_location(self, dirpath, filename, disk_modified, hash, size):
        loc = Location(dirpath=dirpath, filename=filename, modified=disk_modified, checksum=hash, filesize=size, seen=True)
        #print(f"Added {dirpath}{filename}")
        self.session.add(loc)

    def all_file_count(self, prefix):
        q = select(func.count(Location.id)).where(Location.dirpath.like(f"{prefix}%"))
        return self.session.scalar(q)

    def count_not_seen(self, prefix):
        q = select(func.count(Location.id)).where(Location.dirpath.like(f"{prefix}%"), Location.seen == False)
        return self.session.scalars(q).one()

    def dir_files_not_seen(self, prefix):
        q = select(Location.dirpath, Location.filename).where(Location.dirpath.like(f"{prefix}%"), Location.seen == False)
        result = self.session.execute(q)
        return result

    def delete_not_seen(self, prefix):
        q = select(Location).where(Location.dirpath.like(f"{prefix}%"), Location.seen == False)
        for r in self.session.scalars(q):
            self.session.delete(r)

    def record_run(
        self,
        when: datetime,
        rootdir: str,
        files: int,
        known: int,
        updated: int,
        unchanged: int,
        new_files: int,
        deleted: int,
    ):
        run = RunLog(when_run=when, rootdir=rootdir, files=files, known=known, updated=updated, unchanged=unchanged, new_files=new_files, deleted=deleted)
        self.session.add(run)


if __name__ == "__main__":
    dbname = 'sa'
    db_url = DB_URL_FMT.format(dbname)
    engine = create_engine(db_url, echo=True)
    Model.metadata.drop_all(engine)
    Model.metadata.create_all(engine)
    c = Connection()
    import sys

    sys.exit("STOPS HERE!")
