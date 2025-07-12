WITH file_history AS (

    SELECT

        h.table_name,

        h.version,

        h.timestamp,

        h.operation,

        f.file_path AS file

    FROM edsm.silver.powerPlay_transaction_history h

    JOIN (

        SELECT

            primary_key,

            EXPLODE(file_path) AS file_path

        FROM edsm.silver.powerPlay_file_version_history

    ) f

      ON h.primary_key = f.primary_key

)

SELECT

    CAST(p.valid_from AS DATE) AS valid_from,

    CAST(p.valid_to AS DATE) AS valid_to,

    p.name,

    p.id,

    p.power,

    p.allegiance,

    p.government,

    p.powerState,

    p.state,

    p.derived_ingest_time,

    p.source_metadata.file_path,

    fh.table_name,

    fh.version,

    fh.timestamp,

    fh.operation

FROM edsm.silver.powerPlay p

JOIN file_history fh

  ON p.source_metadata.file_path = fh.file

WHERE (

    (TRY_CAST(NULLIF({id}, '') AS BIGINT) IS NULL OR p.id = TRY_CAST(NULLIF({id}, '') AS BIGINT))

    AND (NULLIF({name}, '') IS NULL OR p.name = NULLIF({name}, ''))

    AND (NULLIF({power}, '') IS NULL OR p.power = NULLIF({power}, ''))

    AND (TRY_CAST({valid_from} AS DATE) IS NULL OR DATE(p.valid_from) >= TRY_CAST({valid_from} AS DATE))

    AND (TRY_CAST({valid_to} AS DATE) IS NULL OR DATE(p.valid_to) <= TRY_CAST({valid_to} AS DATE))

)

ORDER BY fh.timestamp;
