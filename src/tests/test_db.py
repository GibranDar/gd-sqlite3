import pytest

from typing import Optional
from attrs import define, field, validators
from gd_sqlite3 import SQLite3Database


@define(kw_only=True)
class Location:
    id: Optional[int] = field(
        validator=[validators.optional(validators.instance_of(int))],
        default=None,
    )
    ref: str = field(validator=[validators.instance_of(str)])
    name: str = field(validator=[validators.instance_of(str)])
    postcode: str = field(
        validator=[
            validators.matches_re(
                r"^[A-Za-z]{1,2}(?:\d{1,2}[A-Za-z]?|\d[A-Za-z]{2})\s?\d[A-Za-z]{2}$"
            )
        ]
    )


@pytest.fixture
def db():
    db = SQLite3Database("tests/test.db")
    return db


@pytest.fixture
def loc():
    return Location(ref="lrh", name="Louisa Ryland House", postcode="B3 3PL")


def test_create_table(db: SQLite3Database):
    """Test that the get_table_columns method returns the correct columns and definition for the locations table."""

    table_info = db.create_table(
        "locations",
        {
            "ref": "TEXT NOT NULL UNIQUE",
            "name": "TEXT NOT NULL UNIQUE",
            "postcode": "TEXT NOT NULL",
        },
    )

    assert table_info == [
        (0, "id", "INTEGER", 0, None, 1),
        (1, "ref", "TEXT", 1, None, 0),
        (2, "name", "TEXT", 1, None, 0),
        (3, "postcode", "TEXT", 1, None, 0),
    ]


def test_insert_one(db: SQLite3Database, loc: Location):
    """Test that the object provided is successfully inserted into the table"""

    db.insert_one("locations", loc)
    items = db.select(
        "locations",
        Location,
        {"ref": loc.ref, "name": loc.name, "postcode": loc.postcode},
    )
    assert len(items) == 1
    assert items[0].ref == loc.ref
    assert items[0].name == loc.name
    assert items[0].postcode == loc.postcode


def test_select_all(db: SQLite3Database):
    items = db.select_all("locations", Location)
    for item in items:
        assert isinstance(item, Location)


def test_update(db: SQLite3Database, loc: Location):
    loc.ref = "Louisa"
    db.update("locations", loc, query={"ref": "lrh"})
    items = db.select("locations", type(loc), query={"ref": loc.ref})
    assert len(items) == 1
    assert items[0].ref == "Louisa"


def test_delete(db: SQLite3Database):
    db.delete("locations", query={"ref": "Louisa"})
    assert len(db.select_all("locations", Location)) == 0
