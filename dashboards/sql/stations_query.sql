WITH silver_versions AS (

    SELECT

        s.valid_from,

        s.valid_to,

        s.id,

        s.name,

        s.systemId,

        s.systemName,

        s.distanceToArrival,

        s.type,

        s.government,

        s.allegiance,

        s.controllingFaction.name AS faction,

        s.haveMarket,

        s.haveOutfitting,

        s.haveShipyard

    FROM edsm.silver.stations s

    WHERE (

        (TRY_CAST(NULLIF({id}, '') AS BIGINT) IS NULL OR s.id = TRY_CAST(NULLIF({id}, '') AS BIGINT))

        AND (NULLIF({name}, '') IS NULL OR s.name = NULLIF({name}, ''))

        AND (TRY_CAST({valid_from} AS DATE) IS NULL OR DATE(s.valid_from) >= TRY_CAST({valid_from} AS DATE))

        AND (TRY_CAST({valid_to} AS DATE) IS NULL OR DATE(s.valid_to) <= TRY_CAST({valid_to} AS DATE))

        AND (TRY_CAST(NULLIF({systemId}, '') AS BIGINT) IS NULL OR s.systemId = TRY_CAST(NULLIF({systemId}, '') AS BIGINT))

        AND (NULLIF({type}, '') IS NULL OR s.type = NULLIF({type}, ''))

        AND (NULLIF({faction}, '') IS NULL OR s.controllingFaction.name = NULLIF({faction}, ''))

        AND (NULLIF({government}, '') IS NULL OR s.government = NULLIF({government}, ''))

        AND (NULLIF({allegiance}, '') IS NULL OR s.allegiance = NULLIF({allegiance}, ''))

    )

)

SELECT

    CAST(s.valid_from AS DATE) AS valid_from,

    CAST(s.valid_to AS DATE) AS valid_to,

    s.id,

    s.name,

    s.systemId,

    s.systemName,

    s.distanceToArrival,

    s.type,

    s.government,

    s.allegiance,

    s.faction,

    s.haveMarket,

    s.haveOutfitting,

    s.haveShipyard,

    b.derived_ingest_time,

    b.source_metadata.file_path,

    b.source_metadata.file_modification_time,

    b._metadata.row_index

FROM silver_versions s

JOIN edsm.bronze.stations b

  ON s.id = b.id

  AND s.name = b.name

  AND s.systemId = b.systemId

  AND s.type = b.type

  AND s.government = b.government

  AND s.allegiance = b.allegiance

  AND s.faction = b.controllingFaction.name

WHERE b.derived_ingest_time BETWEEN s.valid_from AND s.valid_to

ORDER BY s.valid_from, b.derived_ingest_time
