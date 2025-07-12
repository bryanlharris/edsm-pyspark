WITH silver_versions AS (

    SELECT

        p.valid_from,

        p.valid_to,

        p.name,

        p.id,

        p.power,

        p.allegiance,

        p.government,

        p.powerState,

        p.state

    FROM edsm.silver.powerPlay p

    WHERE (

        (TRY_CAST(NULLIF({id}, '') AS BIGINT) IS NULL OR p.id = TRY_CAST(NULLIF({id}, '') AS BIGINT))

        AND (NULLIF({name}, '') IS NULL OR p.name = NULLIF({name}, ''))

        AND (NULLIF({power}, '') IS NULL OR p.power = NULLIF({power}, ''))

        AND (TRY_CAST({valid_from} AS DATE) IS NULL OR DATE(p.valid_from) >= TRY_CAST({valid_from} AS DATE))

        AND (TRY_CAST({valid_to} AS DATE) IS NULL OR DATE(p.valid_to) <= TRY_CAST({valid_to} AS DATE))

        AND (NULLIF({allegiance}, '') IS NULL OR p.allegiance = NULLIF({allegiance}, ''))

        AND (NULLIF({government}, '') IS NULL OR p.government = NULLIF({government}, ''))

        AND (NULLIF({powerState}, '') IS NULL OR p.powerState = NULLIF({powerState}, ''))

        AND (NULLIF({state}, '') IS NULL OR p.state = NULLIF({state}, ''))

    )

)

SELECT

    cast(s.valid_from as date) as valid_from,

    cast(s.valid_to as date) as valid_to,

    s.name,

    s.power,

    s.allegiance,

    s.government,

    s.powerState,

    s.state,

    b.derived_ingest_time,

    b.source_metadata.file_path,

    b.source_metadata.file_modification_time,

    b._metadata.row_index

FROM silver_versions s

JOIN edsm.bronze.powerPlay b

  ON s.id = b.id

  AND s.power = b.power

  AND s.allegiance = b.allegiance

  AND s.government = b.government

  AND s.powerState = b.powerState

  AND s.state = b.state

WHERE b.derived_ingest_time BETWEEN s.valid_from AND s.valid_to

ORDER BY s.valid_from, b.derived_ingest_time;
