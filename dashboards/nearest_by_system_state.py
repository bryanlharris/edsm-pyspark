#!/usr/bin/env python3
"""Plot nearby systems for a given commander system and state."""

import argparse
from functions.utility import create_spark_session
import plotly.express as px

QUERY = """
WITH target_system AS (
    SELECT coords.x AS tx, coords.y AS ty, coords.z AS tz
    FROM edsm.silver.systemsWithCoordinates
    WHERE name = '{commander}'
),
filtered_systems AS (
    SELECT *
    FROM edsm.silver.systemsPopulated sp
    CROSS JOIN target_system t
    WHERE sp.state = '{state}'
      AND ABS(sp.coords.x - t.tx) <= 100
      AND ABS(sp.coords.y - t.ty) <= 100
      AND ABS(sp.coords.z - t.tz) <= 100
),
with_distance AS (
    SELECT
        name,
        coords.x AS x,
        coords.y AS y,
        coords.z AS z,
        SQRT(
            POW(coords.x - t.tx, 2) +
            POW(coords.y - t.ty, 2) +
            POW(coords.z - t.tz, 2)
        ) AS distance
    FROM filtered_systems
    CROSS JOIN target_system t
)
SELECT *
FROM with_distance
ORDER BY distance ASC
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Nearest systems by state")
    parser.add_argument("commander", help="Commander system name")
    parser.add_argument("state", help="System state")
    parser.add_argument("--master", default="local[*]", help="Spark master URL")
    args = parser.parse_args()

    spark = create_spark_session(args.master, "nearest-systems")
    try:
        df = spark.sql(QUERY.format(commander=args.commander, state=args.state)).toPandas()
        fig = px.scatter_3d(
            df,
            x="x",
            y="y",
            z="z",
            color="distance",
            text="name",
            title="Nearby Systems",
            labels={"x": "X", "y": "Y", "z": "Z"},
        )
        fig.update_traces(marker=dict(size=5), textposition="top center")
        fig.show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
