"""

Implements various algorithms from https://hakibenita.com/fast-load-data-python-postgresql

"""

import io
from collections.abc import Iterator
from typing import Any

import psycopg2
from typing_extensions import Self
from utils import clean_csv_value, create_staging_table, parse_first_brewed, profile

"""

    Functions you will need to edit at the top

"""

def open_connection() -> psycopg2.extensions.connection:
    conn = psycopg2.connect(
        host="localhost", dbname="testload", user="jlc", password=None
    )
    conn.autocommit = True
    return conn


@profile
def copy_stringio(connection: psycopg2.extensions.connection, beers: list[dict[str, Any]]) -> None:
    """You don't really need to create a stringio file object to use modern psycopg's copy
    but you do need to carefully set all the arguments to copy if you're going
    to string-ify everything yourself.
    """
    with connection.cursor() as cursor:
        create_staging_table(cursor)
        csv_file_like_object = io.StringIO()
        for beer in beers:
            csv_file_like_object.write(
                 "|".join(
                        map(
                            clean_csv_value,
                            (
                                beer["id"],
                                beer["name"],
                                beer["tagline"],
                                parse_first_brewed(beer["first_brewed"]).isoformat(),
                                beer["description"],
                                beer["image_url"],
                                beer["abv"],
                                beer["ibu"],
                                beer["target_fg"],
                                beer["target_og"],
                                beer["ebc"],
                                beer["srm"],
                                beer["ph"],
                                beer["attenuation_level"],
                                beer["contributed_by"],
                                beer["brewers_tips"],
                                beer["volume"]["value"],
                            ), # end tuple
                        )  # end map
                    )  # end join |
                + "\n"
            )  # end write

        csv_file_like_object.seek(0)
        cursor.copy_from(
                csv_file_like_object, 'staging_beers', sep='|'
            )


class StringIteratorIO(io.TextIOBase):
    def __init__(self: Self, iter8: Iterator[str]) -> None:
        self._iter = iter8
        self._buff = ''

    def readable(self: Self) -> bool:
        return True

    def _read1(self: Self, n: int | None = None) -> str:
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret):]
        return ret

    def read(self: Self, n: int | None = None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return ''.join(line)


@profile
def copy_string_iterator(
    connection: psycopg2.extensions.connection, beers: list[dict[str, Any]]
) -> None:
    with connection.cursor() as cursor:
        create_staging_table(cursor)
        beers_string_iterator = StringIteratorIO(
            '|'.join(map(clean_csv_value, (
                beer['id'],
                beer['name'],
                beer['tagline'],
                parse_first_brewed(beer['first_brewed']).isoformat(),
                beer['description'],
                beer['image_url'],
                beer['abv'],
                beer['ibu'],
                beer['target_fg'],
                beer['target_og'],
                beer['ebc'],
                beer['srm'],
                beer['ph'],
                beer['attenuation_level'],
                beer['brewers_tips'],
                beer['contributed_by'],
                beer['volume']['value'],
            ))) + '\n'
            for beer in beers
        )
        cursor.copy_from(beers_string_iterator, 'staging_beers', sep='|')
