"""test_storage.py: make sure you can rely on the integrity of storage."""

import tempfile

from datetime import datetime

import pytest

from sqlalchemy_store import (
    Location,
    TokenPos,
    RunLog,
    Archive,
    Database,
    Checksum,
)
from sqlalchemy import select, func
from sqlalchemy.orm import sessionmaker

PREFIX = "/Users/sholden/"


@pytest.fixture(scope="function")  # 'function' scope means for each test function
def db():
    """
    A pytest fixture that provides a database with a SQLAlchemy session,
    and rolls back the session after the test completes.
    """
    db = Database(dbname="test", temporary=True, echo=False)
    with db.session.begin():  # Start a transaction *on the connection*
        if not verify_empty(db.session):
            raise ValueError("Session was not empty before test")
        yield db
        db.session.rollback()
    if not verify_empty(db.session):
        purge_db(db.session)
        db.session.commit()
        raise ValueError("Session was not empty after test")


def verify_empty(session):
    return all(
        session.scalar(func.count(record_type.id)) == 0
        for record_type in (Checksum, Location, TokenPos, RunLog, Archive)
    )


def purge_db(session):
    for record_type in (Location, TokenPos, RunLog, Archive):
        session.query(record_type).delete()


def test_empty_session(db):
    pass  # We want the empty session test to pass!


def test_structure(db):
    cs = db.register_hash("/dev/null")
    db.session.add(
        Location(
            filename="nosuch.py",
            dirpath="/Users/sholden/",
            modified=3.14159,
            checksum=cs,
            seen=False,
            filesize=1024,
        )
    )
    assert (loc := db.session.scalars(select(Location)).one())
    assert isinstance(loc, Location)
    db.session.delete(loc)
    db.session.delete(cs)
    q = select(func.count(Location.id))
    assert db.session.scalar(q) == 0


def test_seen_bits(db):
    q = select(func.count(Location.id))
    assert db.session.scalar(q) == 0
    for i in range(20):
        cs = db.register_hash("/dev/null")
        db.session.add(
            Location(
                filename=f"file{i:02d}.tst",
                dirpath=PREFIX,
                modified=3.14159,
                checksum=cs,
                seen=(i % 2 == 0),
                filesize=1024 * i,
            )
        )
    assert db.unseen_location_count(PREFIX) == 10
    q1 = select(func.count(Location.id))
    q2 = q1.where(Location.seen == True)
    q3 = q1.where(Location.seen == False)
    for q, r in (q1, 20), (q2, 10), (q3, 10):
        assert db.session.scalar(q) == r
    db.delete_unseen_locations(PREFIX)
    assert db.session.scalar(q1) == 10
    assert db.unseen_location_count(PREFIX) == 0
    q = select(Location)
    for loc in db.session.scalars(q):  # Should be passing the whole object here.
        db.update_seen(loc, False)
    assert db.unseen_location_count(PREFIX) == 10


def test_archive_references_runlog(db):
    with db.session.begin_nested() as nested:
        runlog = RunLog(
            when_run=datetime.now(),
            rootdir="/no/such/directory/",
            files=0,
            known=0,
            updated=0,
            unchanged=0,
            new_files=0,
            deleted=0,
        )
        cs = db.register_hash("/dev/null")
        db.session.add(cs)
        assert db.session.scalar(func.count(Archive.id)) == 0
        loc = Location(
            filename="test.txt",
            dirpath="/nosuch/directory/",
            modified=115678.0,
            checksum=cs,
            seen=True,
            filesize=1025,
        )
        db.session.add(loc)
        db.session.flush()
        db.archive_record(
            reason="TESTING", rectype="location", record=loc, runlog=runlog
        )
    assert db.session.scalar(func.count(Archive.id)) == 1
    archives = db.session.execute(select(Archive)).scalars()
    archive = next(archives)
    assert archive.runlog is runlog


def test_register_hash(db):
    # check creation adds a record
    with db.session.begin_nested() as nested:
        cs = db.register_hash("/dev/null")
    assert db.session.scalar(func.count(Checksum.id)) == 1
    assert isinstance(cs, Checksum)
    cs = db.register_hash("/dev/null")
    assert db.session.scalar(func.count(Checksum.id)) == 1
    assert isinstance(cs, Checksum)
    with tempfile.NamedTemporaryFile(delete_on_close=False) as fp:
        fp.write(b"Hello world!")
        fp.close()
        # the file is not removed within the context
        with db.session.begin_nested() as nested:
            cs = db.register_hash(fp.name)
    assert db.session.scalar(func.count(Checksum.id)) == 2
    assert isinstance(cs, Checksum)


def test_location_serialization(db):
    cs = db.register_hash("/dev/null")
    for name in ("one", "two", "three"):
        db.save_reference(checksum=cs, name=name, line=1, pos=1)
    loc = db.insert_location(
        dirpath="/somwhere/over/the/rainbow",
        filename="far_away.txt",
        modified=1023.25,
        checksum=cs,
        filesize=999,
    )
    loc_dict = loc.to_dict()
    assert list(loc_dict["checksum"].keys()) == ["checksum"]


def test_large_filesize(db):
    with db.session.begin_nested():
        cs = db.register_hash("/dev/null")
        loc = db.insert_location(
            dirpath="/somwhere/over/the/rainbow",
            filename="far_away.txt",
            modified=1023.25,
            checksum=cs,
            filesize=3515506688,
        )
        db.session.add(loc)
    assert True
