"""

    Functions needed by more than one set of experiments

"""

import datetime
import time
from collections.abc import Callable, Iterator
from functools import wraps
from typing import Any
from urllib.parse import urlencode

import psycopg
import psycopg2
import requests
from memory_profiler import memory_usage

#  --> Data fetching related

def parse_first_brewed(text: str) -> datetime.date:
    """Turn a MM/YYYY or YYYY into datetime.date"""

    parts = text.split("/")
    if len(parts) == 2:
        return datetime.date(int(parts[1]), int(parts[0]), 1)
    if len(parts) == 1:
        return datetime.date(int(parts[0]), 1, 1)
    assert False, f"Unknown date format {text}"


def iter_beers_from_api(page_size: int = 5) -> Iterator[dict[str, Any]]:
    session = requests.Session()
    page = 1
    while True:
        response = session.get(
            "https://api.punkapi.com/v2/beers?"
            + urlencode({"page": page, "per_page": page_size})
        )
        response.raise_for_status()

        data = response.json()
        if not data:
            break

        yield from data

        page += 1


# using cache to take network out of performance analysis
# copy dataset 100x to get "big" data
beers = list(iter_beers_from_api()) * 100


# --> Time and memory profiler
#
def profile(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Calls input function fn twice, once to time profile and
    once to memory profile"""

    @wraps(fn)
    def inner(*args: Any, **kwargs: Any) -> Any:
        fn_kwargs_str = ", ".join(f"{k}={v}" for k, v in kwargs.items())
        print(f"\n{fn.__name__}({fn_kwargs_str})")

        # Measure time
        t = time.perf_counter()
        retval = fn(*args, **kwargs)
        elapsed = time.perf_counter() - t
        print(f"Time   {elapsed:0.4}")

        # Measure memory
        mem, retval = memory_usage(
            (fn, args, kwargs), retval=True, timeout=200, interval=1e-7
        )

        print(f"Memory {max(mem) - min(mem)}")
        return retval

    return inner


# --> Data loading functions that are the same between
#     psycopg2 and psycopg

EitherConnection = psycopg2.extensions.connection | psycopg.Connection

def create_staging_table(cursor: EitherConnection) -> None:
    cursor.execute(
        """
        DROP TABLE IF EXISTS staging_beers;
        CREATE UNLOGGED TABLE staging_beers (
            id                  INTEGER,
            name                TEXT,
            tagline             TEXT,
            first_brewed        DATE,
            description         TEXT,
            image_url           TEXT,
            abv                 DECIMAL,
            ibu                 DECIMAL,
            target_fg           DECIMAL,
            target_og           DECIMAL,
            ebc                 DECIMAL,
            srm                 DECIMAL,
            ph                  DECIMAL,
            attenuation_level   DECIMAL,
            brewers_tips        TEXT,
            contributed_by      TEXT,
            volume              INTEGER
        );"""
    )


@profile
def insert_one_by_one(
    connection: EitherConnection, beers: list[dict[str, Any]]
) -> None:
    """The insert-one-by-one implementation doesn't change
    between psycopg2 and psycopg"""

    with connection.cursor() as cursor:
        create_staging_table(cursor)
        for beer in beers:
            cursor.execute(
                """
                INSERT INTO staging_beers VALUES (
                    %(id)s,
                    %(name)s,
                    %(tagline)s,
                    %(first_brewed)s,
                    %(description)s,
                    %(image_url)s,
                    %(abv)s,
                    %(ibu)s,
                    %(target_fg)s,
                    %(target_og)s,
                    %(ebc)s,
                    %(srm)s,
                    %(ph)s,
                    %(attenuation_level)s,
                    %(brewers_tips)s,
                    %(contributed_by)s,
                    %(volume)s
                );""",
                {
                    **beer,
                    "first_brewed": parse_first_brewed(beer["first_brewed"]),
                    "volume": beer["volume"]["value"],
                },
            )


@profile
def insert_executemany(
    connection: EitherConnection, beers: list[dict[str, Any]]
) -> None:
    """executevalues, executebatch, and executemany are basically all the same
    thing in modern psycopg, and the page_size argument is gone.
    """
    with connection.cursor() as cursor:
        create_staging_table(cursor)

        cursor.executemany("""
            INSERT INTO staging_beers VALUES (
                %(id)s,
                %(name)s,
                %(tagline)s,
                %(first_brewed)s,
                %(description)s,
                %(image_url)s,
                %(abv)s,
                %(ibu)s,
                %(target_fg)s,
                %(target_og)s,
                %(ebc)s,
                %(srm)s,
                %(ph)s,
                %(attenuation_level)s,
                %(brewers_tips)s,
                %(contributed_by)s,
                %(volume)s
            );""",
            [
                {
                    **beer,
                    "first_brewed": parse_first_brewed(beer["first_brewed"]),
                    "volume": beer["volume"]["value"],
                }
                for beer in beers
            ],
        )


@profile
def insert_executemany_iterator(
    connection: EitherConnection, beers: list[dict[str, Any]]
) -> None:
    with connection.cursor() as cursor:
        create_staging_table(cursor)

        cursor.executemany("""
            INSERT INTO staging_beers VALUES (
                %(id)s,
                %(name)s,
                %(tagline)s,
                %(first_brewed)s,
                %(description)s,
                %(image_url)s,
                %(abv)s,
                %(ibu)s,
                %(target_fg)s,
                %(target_og)s,
                %(ebc)s,
                %(srm)s,
                %(ph)s,
                %(attenuation_level)s,
                %(brewers_tips)s,
                %(contributed_by)s,
                %(volume)s
            );
            """,
            # insert_executemany_iterator uses parents to get a generator
            # insert_executemany uses square-brackets to pre-compute the entire list
            (
                {
                    **beer,
                    "first_brewed": parse_first_brewed(beer["first_brewed"]),
                    "volume": beer["volume"]["value"],
                }
                for beer in beers
            ),
        )


# --> Change a value to a string for a column in a CSV file

def clean_csv_value(value: Any | None) -> str:
    """Stringify a possibly-null, possibly embedded-newlines-text value
    for copy from csv operations"""

    if value is None:
        return r"\N"
    return str(value).replace("\n", "\\n")
