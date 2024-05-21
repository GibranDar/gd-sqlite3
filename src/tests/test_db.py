import pytest

import csv
from pprint import pprint
from typing import Optional

from attrs import define, field, validators, converters

from gd_sqlite3 import SQLite3Database


@define
class BaseMeta:
    created_on: Optional[str] = field(
        validator=[
            validators.optional(
                validators.matches_re(
                    (r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
                )
            )
        ],
        default=None,
    )
    last_updated: Optional[str] = field(
        validator=[
            validators.optional(
                validators.matches_re(
                    (r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
                )
            )
        ],
        default=None,
    )


@define(kw_only=True)
class Location(BaseMeta):
    id: Optional[int] = field(
        validator=[validators.optional(validators.instance_of(int))],
        converter=converters.optional(int),
        default=None,
    )
    slug: str = field(validator=[validators.instance_of(str)])
    name: str = field(validator=[validators.instance_of(str)])
    google_business_id: Optional[str] = field(
        validator=[validators.optional(validators.instance_of(str))],
        default=None,
    )
    officernd_id: Optional[str] = field(
        validator=[validators.optional(validators.instance_of(str))],
        default=None,
    )
    email: str = field(validator=[validators.instance_of(str)])
    phone: str = field(validator=[validators.instance_of(str)])
    address: str = field(validator=[validators.instance_of(str)])
    city: str = field(validator=[validators.instance_of(str)])
    postcode: str = field(
        validator=[
            validators.matches_re(
                r"^[A-Za-z]{1,2}(?:\d{1,2}[A-Za-z]?|\d[A-Za-z]{2})\s?\d[A-Za-z]{2}$"
            )
        ]
    )
    latitude: float = field(
        validator=[validators.instance_of(float)], converter=float
    )
    longitude: float = field(
        validator=[validators.instance_of(float)], converter=float
    )
    floorplan: str = field(
        validator=[
            validators.instance_of(str),
            validators.max_len(255),
        ],  # type:ignore[arg-type]
        default="",
    )
    brochure: str = field(
        validator=[
            validators.instance_of(str),
            validators.max_len(255),
        ],  # type:ignore[arg-type]
        default="",
    )
    website: str = field(
        validator=[
            validators.instance_of(str),
            validators.max_len(255),
        ]  # type:ignore[arg-type]
    )
    type: str = field(
        validator=[
            validators.instance_of(str),
            validators.in_(["managed", "serviced", "hot_desking"]),
        ]  # type:ignore[arg-type]
    )
    size: int = field(validator=[validators.instance_of(int)], converter=int)


@pytest.fixture
def db():
    db = SQLite3Database("tests/test.db")
    return db


@pytest.fixture
def loc():
    return Location(
        slug="louisa-ryland-house",
        name="Louisa Ryland House",
        email="lrh@re-defined.co.uk",
        phone="0121 393 3818",
        google_business_id="2666212045029758695",
        officernd_id="633ac05fb85f276bea3f644b",
        address="44 Newhall Street",
        city="Birmingham",
        postcode="B3 3PL",
        latitude=52.48154,
        longitude=-1.90268,
        website="www.louisarylandhouse.com",
        type="serviced",
        size=35000,
    )


def test_create_table(db: SQLite3Database):
    """Test that the get_table_columns method returns the correct columns and definition for the locations table."""

    locations_table = db.create_table(
        "locations",
        {
            "slug": "TEXT NOT NULL UNIQUE",
            "name": "TEXT NOT NULL UNIQUE",
            "google_business_id": "VARCHAR(255)",
            "officernd_id": "VARCHAR(255) NOT NULL UNIQUE",
            "email": "VARCHAR(255) NOT NULL",
            "phone": "VARCHAR(20) NOT NULL",
            "address": "TEXT NOT NULL",
            "city": "TEXT NOT NULL",
            "postcode": "TEXT NOT NULL",
            "latitude": "DECIMAL(10, 8) NOT NULL",
            "longitude": "DECIMAL(10, 8) NOT NULL",
            "floorplan": "VARCHAR(255) DEFAULT ''",
            "brochure": "VARCHAR(255) DEFAULT ''",
            "website": "VARCHAR(255) NOT NULL",
            "type": "VARCHAR(100) NOT NULL",
            "size": "INTEGER NOT NULL",
            "created_on": "TEXT",
            "last_updated": "TEXT",
        },
    )

    db.create_trigger(
        "locations_meta_created_on",
        """
            AFTER INSERT ON locations
            FOR EACH ROW
            BEGIN
                UPDATE locations
                SET created_on = CURRENT_TIMESTAMP
                WHERE rowid = NEW.rowid;
            END
        """,
    )
    db.create_trigger(
        "locations_meta_last_updated",
        """
           AFTER UPDATE ON locations
            FOR EACH ROW
            BEGIN
                UPDATE locations
                SET last_updated = CURRENT_TIMESTAMP
                WHERE rowid = NEW.rowid;
            END 
        """,
    )

    assert locations_table == [
        (0, "id", "INTEGER", 0, None, 1),
        (1, "slug", "TEXT", 1, None, 0),
        (2, "name", "TEXT", 1, None, 0),
        (3, "google_business_id", "VARCHAR(255)", 0, None, 0),
        (4, "officernd_id", "VARCHAR(255)", 1, None, 0),
        (5, "email", "VARCHAR(255)", 1, None, 0),
        (6, "phone", "VARCHAR(20)", 1, None, 0),
        (7, "address", "TEXT", 1, None, 0),
        (8, "city", "TEXT", 1, None, 0),
        (9, "postcode", "TEXT", 1, None, 0),
        (10, "latitude", "DECIMAL(10, 8)", 1, None, 0),
        (11, "longitude", "DECIMAL(10, 8)", 1, None, 0),
        (12, "floorplan", "VARCHAR(255)", 0, "''", 0),
        (13, "brochure", "VARCHAR(255)", 0, "''", 0),
        (14, "website", "VARCHAR(255)", 1, None, 0),
        (15, "type", "VARCHAR(100)", 1, None, 0),
        (16, "size", "INTEGER", 1, None, 0),
        (17, "created_on", "TEXT", 0, None, 0),
        (18, "last_updated", "TEXT", 0, None, 0),
    ]


def test_insert_one(db: SQLite3Database, loc: Location):
    """Test that the object provided is successfully inserted into the table"""

    db.insert_one("locations", loc)
    items = db.select(
        "locations",
        Location,
        {"slug": loc.slug, "name": loc.name, "postcode": loc.postcode},
    )
    assert len(items) == 1
    assert items[0].slug == loc.slug
    assert items[0].name == loc.name
    assert items[0].postcode == loc.postcode

    print(items)


def test_select_all(db: SQLite3Database):
    items = db.select_all("locations", Location)
    for item in items:
        assert isinstance(item, Location)


def test_update(db: SQLite3Database, loc: Location):
    loc.slug = "Louisa"
    db.update("locations", loc, query={"slug": "louisa-ryland-house"})
    items = db.select("locations", type(loc), query={"slug": loc.slug})
    assert len(items) == 1
    assert items[0].slug == "Louisa"


def test_delete(db: SQLite3Database):
    db.delete("locations", query={"slug": "Louisa"})
    assert (
        db.select("locations", Location, query={"slug": "louisa-ryland-house"})
        == []
    )


def test_insert_from_csv(db: SQLite3Database):
    db.insert_from_csv("tests/locations.data.csv", "locations", Location)
    locations = db.select_all("locations", Location)
    pprint(locations, sort_dicts=False, width=100, indent=2)


def test_export_to_csv(db: SQLite3Database):
    db.export_table_to_csv("locations", "tests/export.data.csv")
    csv_reader = csv.reader("export.data.csv")
    for row in csv_reader:
        pprint(row, sort_dicts=False, indent=2)
