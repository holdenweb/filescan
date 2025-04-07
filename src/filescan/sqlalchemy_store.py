import hashlib
import logging
import os
import sys
from datetime import datetime


from alembic import context
from dotenv import load_dotenv

load_dotenv()  #

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    MetaData,
    String,
    create_engine,
    exists,
    func,
    select,
    update,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import ArgumentError, NoResultFound
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
    sessionmaker,
)
from sqlalchemy.types import BIGINT
from sqlalchemy_serializer import SerializerMixin


root = logging.getLogger()
root.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
root.addHandler(handler)

DB_URL_FORMAT = "postgresql+psycopg://localhost:5432/{dbname}".format


class Model(DeclarativeBase):
    metadata = MetaData(
        naming_convention={
            "ix": "ix_%(column_0_label)s",
            "uq": "uq_%(table_name)s_%(column_0_name)s",
            "ck": "ck_%(table_name)s_%(constraint_name)s",
            "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
            "pk": "pk_%(table_name)s",
        }
    )


class Checksum(Model, SerializerMixin):
    __tablename__ = "checksum"
    id: Mapped[int] = mapped_column(primary_key=True)
    checksum: Mapped[str] = mapped_column(String(), index=True, unique=True)
    locations: Mapped[list["Location"]] = relationship(back_populates="checksum")
    tokens: Mapped[list["TokenPos"]] = relationship(back_populates="checksum")
    serialize_only = ("checksum",)


class Location(Model, SerializerMixin):
    __tablename__ = "location"
    id: Mapped[int] = mapped_column(primary_key=True)
    filename: Mapped[str] = mapped_column(String())
    dirpath: Mapped[str] = mapped_column(String())
    modified: Mapped[float] = mapped_column(Float())
    seen: Mapped[bool] = mapped_column(Boolean())
    filesize: Mapped[int] = mapped_column(BigInteger())
    serialize_rules = ("-checksum_id", "checksum.checksum")
    checksum_id: Mapped[int] = mapped_column(
        ForeignKey("checksum.id"), nullable=True, index=True
    )
    checksum: Mapped[Checksum] = relationship("Checksum", back_populates="locations")


class TokenPos(Model, SerializerMixin):
    __tablename__ = "tokenpos"
    id: Mapped[int] = mapped_column(primary_key=True)
    checksum_id: Mapped[int] = mapped_column(ForeignKey("checksum.id"), index=True)
    checksum: Mapped[Checksum] = relationship("Checksum", back_populates="tokens")
    ttype: Mapped[int] = mapped_column(nullable=False, default=1)
    name: Mapped[str]
    line: Mapped[int]
    pos: Mapped[int]
    serialize_rules = ("-checksum.tokens",)


class RunLog(Model, SerializerMixin):
    __tablename__ = "runlog"
    id: Mapped[int] = mapped_column(primary_key=True)
    when_run: Mapped[datetime] = mapped_column(DateTime)
    when_finished: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    rootdir: Mapped[str]
    files: Mapped[int]
    known: Mapped[int]
    updated: Mapped[int]
    unchanged: Mapped[int]
    new_files: Mapped[int]
    deleted: Mapped[int]
    archives: Mapped[list["Archive"]] = relationship("Archive", back_populates="runlog")


class Archive(Model):
    __tablename__ = "archive"
    id: Mapped[int] = mapped_column(primary_key=True)
    reason: Mapped[str] = mapped_column(String())
    rectype: Mapped[str] = mapped_column(String())
    data: Mapped[dict] = mapped_column(JSONB())
    runlog_id: Mapped[int] = mapped_column(
        ForeignKey("runlog.id"), nullable=True, index=True
    )
    runlog: Mapped[RunLog] = relationship("RunLog", back_populates="archives")


class Database:
    class DoesNotExist(Exception):
        ...

    def __init__(self, dbname=None, temporary=False, echo=False):
        self.dbname = dbname if dbname is not None else os.environ.get("DBNAME", "test")
        self.db_url = DB_URL_FORMAT(dbname=self.dbname)
        exists = self._database_exists(self.dbname)
        if temporary:
            if not exists:
                self._create_database(self.dbname)  # Call the database creation method
        elif not exists:
            raise ValueError(f"Cannot access non-existent database {self.dbname!r}")
        # Reaching this point indicates that a suitable database exists
        self.engine = create_engine(self.db_url, echo=echo)
        self.session = sessionmaker(bind=self.engine)()

    def _create_database(self, dbname: str):
        """
        Creates a new PostgreSQL database with the given name.
        """
        temp_engine = create_engine(
            "postgresql+psycopg://localhost:5432/postgres",
            echo=True,
            isolation_level="AUTOCOMMIT",
        )  # Isolation level allows DDL

        with temp_engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE {dbname}"))
        temp_engine.dispose()

        engine = create_engine(
            self.db_url, echo=False
        )  # Create an engine to the new database
        Model.metadata.create_all(engine)  # Create the tables
        engine.dispose()

    def _database_exists(self, dbname: str) -> bool:
        """
        Checks if a PostgreSQL database with the given name exists.

        Args:
            dbname: The name of the database to check.

        Returns:
            True if the database exists, False otherwise.
        """
        temp_engine = create_engine(
            "postgresql+psycopg://localhost:5432/postgres", echo=False
        )  # Connect to the server, not a specific DB
        with temp_engine.connect() as conn:
            result = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :dbname"),
                {"dbname": dbname},
            )
            return result.scalar() is not None

    def all_file_count(self, prefix):
        # Refactoring candidate ...
        q = select(func.count(Location.id)).where(Location.dirpath.like(f"{prefix}%"))
        return self.session.scalar(q)

    def archive_record(self, reason, rectype, record, runlog):
        self.session.flush()  # Ensure the RunLog record has an id!
        archive = Archive(
            reason=reason, rectype=rectype, data=record.to_dict(), runlog=runlog
        )
        self.session.add(archive)

    def clear_seen_bits(self, prefix):
        q = (
            update(Location)
            .where(Location.dirpath.like(f"{prefix}%"))
            .values(seen=False)
        )
        return self.session.execute(q)

    def commit(self):
        return self.session.commit()

    def insert_location(
        self, dirpath, filename, modified, checksum: Checksum, filesize: int
    ):
        loc = Location(
            dirpath=dirpath,
            filename=filename,
            modified=modified,
            checksum=checksum,
            filesize=filesize,
            seen=True,
        )
        # print(f"Added {dirpath}{filename}")
        self.session.add(loc)
        return loc

    def register_hash(self, file_path):
        """
        Checksum file's content, creating a new Checksum row if necessary.

        File scanning was formerly performed here, but is now move to
        plugins. Importable modules with names matching "filescan_* will"
        be imported and their `process` function will be called with
        the connection object as the first argument and the relevant
        Location object as the second.
        """
        try:
            new_file = open(file_path, "rb")
            hash = hashlib.file_digest(new_file, "sha256").hexdigest()
        except (FileNotFoundError, PermissionError):
            return None
        cs = self.session.query(Checksum).filter_by(checksum=hash).first()
        if cs is None:
            cs = Checksum(checksum=hash)
            self.session.add(cs)
        return cs

    def save_reference(
        self, checksum: Checksum, name: str, line: int, pos: int, ttype: int = 1
    ) -> TokenPos:

        t = TokenPos(checksum=checksum, ttype=ttype, name=name, line=line, pos=pos)
        self.session.add(t)
        return t

    def location_for(self, dirpath: str, filename: str):
        try:
            q = select(Location).where(
                Location.dirpath == dirpath, Location.filename == filename
            )
            result = self.session.scalars(q).one()
            return result
        except NoResultFound:
            raise self.DoesNotExist(f"{dirpath}{filename}")

    def start_run(self, rootdir) -> int:
        runlog = RunLog(
            when_run=datetime.now(),
            rootdir=rootdir,
            files=0,
            known=0,
            updated=0,
            unchanged=0,
            new_files=0,
            deleted=0,
        )
        self.session.add(runlog)
        self.session.flush()
        return runlog

    def end_run(
        self,
        run: RunLog,
        files: int,
        known: int,
        updated: int,
        unchanged: int,
        new_files: int,
        deleted: int,
    ):
        run.files = files
        run.known = known
        run.updated = updated
        run.unchanged = unchanged
        run.new_files = new_files
        run.deleted = deleted
        run.when_finished = datetime.now()
        self.session.add(run)

    def update_details(
        self,
        loc: Location,
        modified: float,
        checksum: Checksum,
        size,
        seen: bool = True,
    ):
        loc.modified = modified
        loc.checksum = checksum
        loc.filesize = size
        loc.seen = seen
        self.session.add(loc)
        return loc

    def update_seen(self, loc, value=True):
        loc.seen = value
        self.session.add(loc)

    def unseen_location_count(self, prefix):
        q = select(func.count(Location.id)).where(
            Location.dirpath.like(f"{prefix}%"), Location.seen == False
        )
        return self.session.scalars(q).one()

    def unseen_locations(self, prefix):
        q = select(Location).where(
            Location.dirpath.like(f"{prefix}%"), Location.seen == False
        )
        result = self.session.scalars(q)
        return result

    def delete_unseen_locations(self, prefix):
        q = select(Location).where(
            Location.dirpath.like(f"{prefix}%"), Location.seen == False
        )
        for r in self.session.scalars(q):
            self.session.delete(r)


#
# RANDOM STUFF CREATED DURING DEVELOPMENT
#
def counted_symbols_from_filename_q(filename, *, dirpath):
    q = (
        select(TokenPos.name, func.count(TokenPos.name).label("ct"))
        .join(Location.checksum)
        .join(Checksum.tokens)
        .where(Location.filename == filename, Location.dirpath == dirpath)
        .group_by(TokenPos.name)
    )
    return q


i = 0
