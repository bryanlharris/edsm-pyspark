
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



WHERE (

    (TRY_CAST(NULLIF({id}, '') AS BIGINT) IS NULL OR p.id = TRY_CAST(NULLIF({id}, '') AS BIGINT))

    AND (NULLIF({name}, '') IS NULL OR p.name = NULLIF({name}, ''))

    AND (NULLIF({power}, '') IS NULL OR p.power = NULLIF({power}, ''))

    AND (TRY_CAST({valid_from} AS DATE) IS NULL OR DATE(p.valid_from) >= TRY_CAST({valid_from} AS DATE))

    AND (TRY_CAST({valid_to} AS DATE) IS NULL OR DATE(p.valid_to) <= TRY_CAST({valid_to} AS DATE))

)

ORDER BY p.derived_ingest_time;
