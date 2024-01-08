import hashlib
import keyword as kw
import logging
import os
import sys
import token
from tokenize import tokenize
from datetime import datetime

from alembic import context
from dotenv import load_dotenv

load_dotenv()  #

from sqlalchemy import (
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
from sqlalchemy_serializer import SerializerMixin


root = logging.getLogger()
root.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
root.addHandler(handler)

DB_URL = (
    f"postgresql+psycopg2://localhost:5432/{os.environ.get('DBNAME', 'default_db')}"
)


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
    checksum_id: Mapped[int] = mapped_column(
        ForeignKey("checksum.id"), nullable=True, index=True
    )
    checksum: Mapped[Checksum] = relationship("Checksum", back_populates="locations")
    seen: Mapped[bool] = mapped_column(Boolean())
    filesize: Mapped[int]
    serialize_rules = ("-checksum_id", "checksum.checksum")


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
    rootdir: Mapped[str]
    files: Mapped[int]
    known: Mapped[int]
    updated: Mapped[int]
    unchanged: Mapped[int]
    new_files: Mapped[int]
    deleted: Mapped[int]


class Archive(Model):
    __tablename__ = "archive"
    id: Mapped[int] = mapped_column(primary_key=True)
    reason: Mapped[str] = mapped_column(String())
    rectype: Mapped[str] = mapped_column(String())
    data: Mapped[dict] = mapped_column(JSONB())


class Connection:
    class DoesNotExist(Exception):
        ...

    def __init__(self):
        self.db_url = DB_URL
        self.engine = create_engine(self.db_url, echo=False)
        self.session = sessionmaker(self.engine)()

    def all_file_count(self, prefix):
        # Refactoring candidate ...
        q = select(func.count(Location.id)).where(Location.dirpath.like(f"{prefix}%"))
        return self.session.scalar(q)

    def archive_record(self, reason, rectype, record):
        archive = Archive(reason=reason, rectype=rectype, data=record.to_dict())
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

        On the assumption that files needing scanning should be scanned once,
        when the content is first logged. Identical files are identified by
        equality of checksum value, implying that if the checksum already
        exists then all necessary scanning has been performed.
        """
        try:
            new_file = open(file_path, "rb")
            hash = hashlib.file_digest(new_file, "sha256").hexdigest()
        except FileNotFoundError:
            return None
        cs = self.session.query(Checksum).filter_by(checksum=hash).first()
        if cs is None:
            cs = Checksum(checksum=hash)
            self.session.add(cs)
            self.scan_tokens(file_path, cs)
        return cs

    def save_reference(
        self, checksum: Checksum, ttype: int, name: str, line: int, pos: int
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
        run = RunLog(
            when_run=when,
            rootdir=rootdir,
            files=files,
            known=known,
            updated=updated,
            unchanged=unchanged,
            new_files=new_files,
            deleted=deleted,
        )
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

    def scan_tokens(self, filepath, checksum: Checksum):
        """
        Add the non-keyword tokens to the position index for this file.
        Only called when no checksum previously existed for the file's
        current incarnation - otherwise we assume scanning took place
        when the original checksum was created.
        XXX The above assumption fails when the first incarnation of a
            Python source file doesn't have the ".py" extension. Hmmm.
            Maybe one solution is an explicit test for the existence of
            at least one TokenPos for a given checksum, but even this
            would cause repeated parsing of files containing no names.
        """
        if not filepath.endswith(".py"):
            return
        with open(filepath, "rb") as inf:
            try:
                for t in tokenize(inf.readline):
                    if t.type == token.NAME and not kw.iskeyword(t.string):
                        self.save_reference(
                            checksum, 1, t.string, t.start[0], t.start[1]
                        )
            except Exception as e:
                print(
                    f"** {filepath}: {type(e)}\n   {e}"
                )  # XXX: sensible handling of parse and other errors
