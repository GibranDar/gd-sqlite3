import sqlite3
import csv
from typing import TypeVar, Optional, Union

from attrs import define, field, validators

T = TypeVar("T")


@define
class SQLite3Database:
    """
    Implementation of Database protocol for SQLite3 databases.
    """

    path: str = field(validator=validators.instance_of(str))
    conn: sqlite3.Connection = field(
        validator=validators.instance_of(sqlite3.Connection),
        init=False,
    )

    def __attrs_post_init__(self):
        """Creates a connection to the SQLite3 database after initialization."""
        self.conn = sqlite3.connect(self.path, check_same_thread=False)

    @property
    def cursor(self) -> sqlite3.Cursor:
        return self.conn.cursor()

    def get_table_info(
        self, table: str
    ) -> list[tuple[int, str, str, int, Optional[str], int]]:
        """
        Returns table info as a list of columns with following description:
            Id, Name, Type, Is not null, Default value, Is primary key
        """

        stmt = f"PRAGMA table_info({table});"
        return self.cursor.execute(stmt).fetchall()

    def table_columns(self, table: str) -> list[str]:
        columns = self.get_table_info(table)
        return [col[1] for col in columns]

    def create_table(
        self, table: str, fields: dict[str, str], sql: str = ""
    ) -> list[tuple[int, str, str, int, Optional[str], int]]:
        """Create a table in the database."""

        stmt = f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id INTEGER PRIMARY KEY,
                {", ".join(" ".join([field[0], field[1].upper()]) for field in fields.items())}
            )
        """
        stmt += sql
        self.cursor.execute(stmt)
        self.conn.commit()
        return self.get_table_info(table)

    def drop_table(self, table: str) -> None:
        """Drop given table"""

        self.cursor.execute(f"DROP TABLE IF EXISTS {table}")
        self.conn.commit()
        return

    def export_table_to_csv(self, table: str, file: str) -> None:
        stmt = f"SELECT * FROM {table};"
        data = self.cursor.execute(stmt).fetchall()
        fieldnames = [n for n in self.table_columns(table)]
        with open(file, "w", newline="", encoding="utf-8") as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(fieldnames)
            csv_writer.writerows(data)
        return

    def create_trigger(self, trigger_name: str, trigger_stmt: str) -> None:
        self.cursor.execute(
            f"""
            CREATE TRIGGER {trigger_name}
            {trigger_stmt};
        """
        )
        self.conn.commit()
        return

    def insert_one(self, table: str, obj: T) -> None:
        """Insert (or replace) an object into the given table"""

        col_names = self.table_columns(table)
        stmt = f"""
            INSERT OR REPLACE INTO {table} ({', '.join(col_names[1:])})
            VALUES ({', '.join(['?'] * len(col_names[1:]))});
        """
        print(stmt)
        self.cursor.execute(
            stmt, tuple([getattr(obj, name) for name in col_names[1:]])
        )
        self.conn.commit()
        return

    def insert_many(self, table: str, objs: list[T]) -> None:
        """Insert many objects into a given table"""

        col_names = self.table_columns(table)
        placeholders = ", ".join(["?"] * len(col_names[1:]))
        stmt = f"""
            INSERT INTO {table} ({', '.join(col_names[1:])})
            VALUES ({placeholders});
        """
        values = [
            tuple(getattr(obj, name) for name in col_names[1:]) for obj in objs
        ]
        self.cursor.executemany(stmt, values)
        self.conn.commit()
        return

    def insert_from_csv(
        self, file: str, table: str, c: type[T], include_id=False
    ) -> None:
        with open(file, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            data: list[T] = []
            for row in reader:
                row_clean = {}
                for k, v in row.items():
                    if k == "id" and not include_id:
                        continue
                    if k in c.__annotations__:
                        row_clean[k] = v
                data.append(c(**row_clean))
        self.insert_many(table, data)
        return

    def select(
        self,
        table: str,
        c: type[T],
        query: dict[str, Union[str, int, float, bool]],
    ) -> list[T]:

        col_names = self.table_columns(table)
        stmt = f"""
            SELECT {', '.join(col_names)} 
            FROM {table}
            WHERE {' AND '.join(f"{f}=?" for f in query.keys())};
        """
        res = self.cursor.execute(stmt, tuple(query.values())).fetchall()
        items: list[T] = []
        for row in res:
            items.append(c(**dict(zip(col_names, row))))
        return items

    def select_all(self, table: str, c: type[T]) -> list[T]:
        """Select all rows from the given table."""

        col_names = self.table_columns(table)
        res = self.cursor.execute(f"SELECT * FROM {table}").fetchall()
        items: list[T] = [c(**dict(zip(col_names, row))) for row in res]
        return items

    def update(
        self,
        table: str,
        obj: T,
        query: dict[str, Union[str, int, float, bool]],
    ) -> None:
        """Update rows meeting query criteria"""

        col_names = self.table_columns(table)
        stmt = f"""
            UPDATE {table}
            SET {', '.join(f"{col}=?" for col in col_names)}
            WHERE {' AND '.join(f"{f}=?" for f in query.keys())};
        """
        new_values = [getattr(obj, name) for name in col_names]
        params = (*new_values, *query.values())
        self.cursor.execute(stmt, params)
        self.conn.commit()
        return

    def delete(
        self, table: str, query: dict[str, Union[str, int, float, bool]]
    ) -> None:
        """Delete all rows in table matching query"""

        stmt = f"""
            DELETE FROM {table}
            WHERE {' AND '.join(f"{f}=?" for f in query.keys())};
        """
        self.cursor.execute(stmt, tuple(query.values()))
        self.conn.commit()
        return

    def delete_all(self, table: str) -> None:
        """Delete all data in given table"""

        self.cursor.execute(f"DELETE from {table}")
        self.conn.commit()
        return
