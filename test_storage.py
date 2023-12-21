"""test_storage.py: make sure you can rely on the integrity of storage."""

import pytest

from sqlalchemy_store import Model, Location, TokenPos, RunLog, Archive, Connection
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import Session

DB_URL_FMT = "postgresql+psycopg2://localhost:5432/{}"
PREFIX = "/Users/sholden/"

@pytest.fixture(scope="module")
def connection():
    conn = Connection("test", create=True)
    yield conn
    conn.engine.dispose()

@pytest.fixture(scope='function')
def session(connection):
    with connection.session.begin():
        yield connection.session
    connection.session.rollback()

def verify_empty(session):
    for record_type in (Location, TokenPos, RunLog, Archive):
        assert session.scalar(func.count(Location.id)) == 0

def test_structure(session):
    verify_empty(session)
    with session.begin_nested() as test_session:
        session.add(
            Location(
                filename='nosuch.py',
                dirpath='/Users/sholden/',
                modified=3.14159,
                checksum='c78fbae7bc8707342384586fc1a21e23a410bd8bf7bd8519c9dfa41149ca2070',
                seen=False,
                filesize=1024
            )
        )
        test_session.commit()
    with session.begin_nested() as test_session:
        assert (loc := session.scalars(select(Location)).one())
        assert isinstance(loc, Location)
        session.delete(loc)
    with session.begin_nested() as test_session:
        q = select(func.count(Location.id))
        assert session.scalar(q) == 0

def test_seen_bits(connection, session):
    verify_empty(session)
    with session.begin_nested() as test_session:
        q = select(func.count(Location.id))
        assert session.scalar(q) == 0
        for i in range(20):
            session.add(
                Location(
                    filename=f"file{i:02d}.tst",
                    dirpath=PREFIX,
                    modified=3.14159,
                    checksum='---- FICTITIOUS ----',
                    seen=(i % 2 == 0),
                    filesize=1024*i
                )
            )
        test_session.commit()
    with session.begin_nested() as test_session:
        assert connection.unseen_location_count(PREFIX) == 10
        q1 = select(func.count(Location.id))
        q2 = q1.where(Location.seen == True)
        q3 = q1.where(Location.seen == False)
        for q, r in (q1, 20), (q2, 10), (q3, 10):
            assert session.scalar(q) == r
        connection.delete_unseen_locations(PREFIX)
        assert session.scalar(q1) == 10
        assert connection.unseen_location_count(PREFIX) == 0
        q = select(Location)
        for loc in session.scalars(q):  # Should be passing the whole object here.
            connection.update_seen(loc, False)
        assert connection.unseen_location_count(PREFIX) == 10
