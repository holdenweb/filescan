"""test_storage.py: make sure you can rely on the integrity of storage."""

import tempfile

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


@pytest.fixture(scope="function")
def db():
    yield Database(dbname="test", temporary=True)


@pytest.fixture(scope="function")  # 'function' scope means for each test function
def db_session(db):
    """
    A pytest fixture that provides a SQLAlchemy session, and rolls back
    the session after the test completes.
    """
    with db.session.begin():  # Start a transaction *on the connection*
        if not verify_empty(db.session):
            raise ValueError("Session was not empty before test")
        yield db.session  # Provide the session to the test
        db.session.rollback()
    if not verify_empty(db.session):
        purge_db(db.session)
        db.session.commit()
        raise ValueError("Session was not empty after test")


def verify_empty(session):
    return all(
        session.scalar(func.count(record_type.id)) == 0
        for record_type in (Location, TokenPos, RunLog, Archive)
    )


def purge_db(session):
    for record_type in (Location, TokenPos, RunLog, Archive):
        session.query(record_type).delete()


def test_empty_session(db_session):
    pass  # We want the empty session test to pass!


def test_structure(db, db_session):
    cs = db.register_hash("/dev/null")
    db_session.add(
        Location(
            filename="nosuch.py",
            dirpath="/Users/sholden/",
            modified=3.14159,
            checksum=cs,
            seen=False,
            filesize=1024,
        )
    )
    assert (loc := db_session.scalars(select(Location)).one())
    assert isinstance(loc, Location)
    db_session.delete(loc)
    db_session.delete(cs)
    q = select(func.count(Location.id))
    assert db_session.scalar(q) == 0


def test_seen_bits(db, db_session):
    q = select(func.count(Location.id))
    assert db_session.scalar(q) == 0
    for i in range(20):
        cs = db.register_hash("/dev/null")
        db_session.add(
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
        assert db_session.scalar(q) == r
    db.delete_unseen_locations(PREFIX)
    assert db_session.scalar(q1) == 10
    assert db.unseen_location_count(PREFIX) == 0
    q = select(Location)
    for loc in db_session.scalars(q):  # Should be passing the whole object here.
        db.update_seen(loc, False)
    assert db.unseen_location_count(PREFIX) == 10


def test_archive(db, db_session):
    with db_session.begin_nested() as nested:
        cs = db.register_hash("/dev/null")
        db_session.add(cs)
        assert db_session.scalar(func.count(Archive.id)) == 0
        loc = Location(
            filename="test.txt",
            dirpath="/nosuch/directory/",
            modified=115678.0,
            checksum=cs,
            seen=True,
            filesize=1025,
        )
        db_session.add(loc)
        db.archive_record("TESTING", "location", loc)
    assert db_session.scalar(func.count(Archive.id)) == 1


def test_register_hash(db, db_session):
    verify_empty(db_session)
    with db_session.begin_nested():
        # check creation adds a record
        cs = db.register_hash("/dev/null")
        assert db_session.scalar(func.count(Checksum.id)) == 1
        assert isinstance(cs, Checksum)
        cs = db.register_hash("/dev/null")
        assert db_session.scalar(func.count(Checksum.id)) == 1
        assert isinstance(cs, Checksum)
        with tempfile.NamedTemporaryFile(delete_on_close=False) as fp:
            fp.write(b"Hello world!")
            fp.close()
            # the file is closed, but not removed
            cs = db.register_hash(fp.name)
            db_session.add(cs)
    assert db_session.scalar(func.count(Checksum.id)) == 2
    assert isinstance(cs, Checksum)


def test_location_serialization(db, db_session):
    verify_empty(db_session)
    with db_session.begin_nested():
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
