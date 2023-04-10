"""
Implements various algorithms from https://hakibenita.com/fast-load-data-python-postgresql
but with newer psycopg3
"""

import io
from typing import Any

import psycopg
from utils import clean_csv_value, create_staging_table, parse_first_brewed, profile

"""

    Functions you will need to edit at the top

"""


def open_connection() -> psycopg.Connection:
    return psycopg.connect(
        host="localhost", dbname="testload", user="jlc", password=None, autocommit=True
    )



@profile
def copy_stringio(connection: psycopg.Connection, beers: list[dict[str, Any]]) -> None:
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
        with cursor.copy(
            """COPY
            staging_beers
            FROM STDIN (
                FORMAT CSV,
                HEADER FALSE,
                DELIMITER '|',
                NULL '\\N'
            )""",
        ) as copy:
            copy.write(csv_file_like_object.getvalue())



@profile
def copy_string_iterator(
    connection: psycopg.Connection, beers: list[dict[str, Any]]
) -> None:
    """Rather than making the whole csv string at once, a for-loop
    yields it up one row at a time.
    """

    with connection.cursor() as cursor:
        create_staging_table(cursor)

        with cursor.copy(
            """COPY
            staging_beers FROM STDIN (
                FORMAT CSV,
                HEADER FALSE,
                DELIMITER '|',
                NULL '\\N'
            )""",
        ) as copy:
            for beer in beers:
                copy.write(
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
                            ),
                        )  # end map
                    )  # end join |
                )  # end write
                copy.write("\n")


@profile
def copy_tuple_iterator(
    connection: psycopg.Connection,
    beers: list[dict[str, Any]],
) -> None:
    """Neither of the above methods is actually the best way to use modern psycopg:
    Let psycopg handle any nulls and deciding whether to send the value as a string
    or as a binary
    """
    with connection.cursor() as cursor:
        create_staging_table(cursor)

        with cursor.copy(
            """COPY staging_beers(
                  id,                  -- 1
                  name,                -- 2
                  tagline,             -- 3
                  first_brewed,        -- 4
                  description,         -- 5
                  image_url,           -- 6
                  abv,                 -- 7
                  ibu,                 -- 8
                  target_fg,           -- 9
                  target_og,           -- 10
                  ebc,                 -- 11
                  srm,                 -- 12
                  ph,                  -- 13
                  attenuation_level,   -- 14
                  contributed_by,      -- 15
                  brewers_tips,        -- 16
                  volume               -- 17
            ) FROM STDIN""",
        ) as copy:
            copy.set_types(
                (
                    "integer",  # 1
                    "text",  # 2
                    "text",  # 3
                    "date",  # 4
                    "text",  # 5
                    "text",  # 6
                    "numeric",  # 7
                    "numeric",  # 8
                    "numeric",  # 9
                    "numeric",  # 10
                    "numeric",  # 11
                    "numeric",  # 12
                    "numeric",  # 13
                    "numeric",  # 14
                    "text",  # 15
                    "text",  # 16
                    "integer",  # 17
                )
            )
            for beer in beers:
                copy.write_row(
                    (
                        beer["id"],  # 1
                        beer["name"],  # 2
                        beer["tagline"],  # 3
                        parse_first_brewed(beer["first_brewed"]),  # 4
                        beer["description"],  # 5
                        beer["image_url"],  # 6
                        beer["abv"],  # 7
                        beer["ibu"],  # 8
                        beer["target_fg"],  # 9
                        beer["target_og"],  # 10
                        beer["ebc"],  # 11
                        beer["srm"],  # 12
                        beer["ph"],  # 13
                        beer["attenuation_level"],  # 14
                        beer["contributed_by"],  # 15
                        beer["brewers_tips"],  # 16
                        beer["volume"]["value"],  # 17
                    ),
                )


