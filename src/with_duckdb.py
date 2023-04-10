"""
Let's try some modern ways of collecting data and see how psycopg does
"""


import json
from collections.abc import Sequence
from typing import Any

import duckdb
import psycopg
from utils import create_staging_table, profile


def save_beers_json(beers: Sequence[dict[str,Any]]) -> None:
    with open("beers.json", "w") as fp:
        fp.writelines(json.dumps(beer) + "\n" for beer in beers)


def save_beers_parquet() -> None:
    with duckdb.connect(":memory:") as ddb:
        ddb.sql("""SELECT * from 'beers.json' """).write_parquet('beers.parquet')


@profile
def copy_with_duckdb(db_conn : psycopg.Connection, whichfile: str) -> None:

    with duckdb.connect(":memory:") as ddb:

        create_staging_table(db_conn)

        cleaned_beers = ddb.sql(f"""SELECT
            id,        -- 1
            name,      -- 2
            tagline,   -- 3

            case
                -- YYYY to YYYY-01-01
                when strlen(first_brewed) = 4
                then first_brewed || '-01-01'

                -- MM/YYYY to YYYY-MM-01
                -- 1234567
                when strlen(first_brewed) = 7
                then first_brewed[4:7] || '-' || first_brewed[1:2] || '-01'

                /* this "when" result won't cast to a date, so we will fail
                 * on unexpected data, and the error message should be
                 * both explanatory and tell us what the data looked like
                 */
                when first_brewed is not NULL
                then 'unexpected data format ' || first_brewed

            end::date
                AS first_brewed,  -- 4

            description,   -- 5
            image_url,     -- 6
            abv,           -- 7
            ibu,           -- 8
            target_fg,     -- 9
            target_og,     -- 10
            ebc,           -- 11
            srm,           -- 12
            ph,            -- 13
            attenuation_level,  -- 14
            brewers_tips,       -- 15
            contributed_by,         -- 16
            volume.value AS volume  -- 17
        FROM '{whichfile}'
        """)

        with db_conn.cursor() as cursor:
            with cursor.copy("""
                COPY staging_beers(
                    id,             -- 1
                    name,           -- 2
                    tagline,        -- 3
                    first_brewed,   -- 4
                    description,    -- 5
                    image_url,      -- 6
                    abv,            -- 7
                    ibu,            -- 8
                    target_fg,      -- 9
                    target_og,      -- 10
                    ebc,            -- 11
                    srm,            -- 12
                    ph,             -- 13
                    attenuation_level,  -- 14
                    brewers_tips,       -- 15
                    contributed_by,     -- 16
                    volume              -- 17
                )
                FROM STDIN"""
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

                row = cleaned_beers.fetchone()
                while row is not None:
                    copy.write_row(row)
                    row = cleaned_beers.fetchone()
