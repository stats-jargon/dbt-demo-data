{% set date_between %}
    WHERE CAST(appointment_start_at AS DATE) BETWEEN '2025-04-15' AND '2025-12-31' 
{% endset %}

-- assuming we do want to map Optum to United Healthcare --
{% set optum_identifier %}
    (
        'uhc', 
        -- 'united healthcare', 
        -- 'unitedhealthcare',
        'optum'
    ) 
{% endset %}

WITH
    /*
    I would ideally have the created as a seed table instead of hard-coding
    but for this exercise, will keep as a CTE
    */
    optum_base_entity_pairs AS (

        select 'FL' AS state_code,
            'Headway Florida Behavioral Health Services, P.A.' AS base_entity_name,
            'Sunshine Medical Behavioral Health Services, P.A.' AS optum_entity_name
        union all
        select 'NY',
            'New York Medical Behavioral Health Services',
            'Hudson Liberty Medical, P.C.'
        union all
        select 'MI',
            'Headway Michigan Behavioral Health Services, P.C.',
            'Great Lakes Behavioral Health Services, P.C.'
        union all
        select 'CA',
            'Headway California Behavioral Health Services',
            'Golden Gate Behavioral Health Services, P.C.'
        union all
        select 'NJ',
            'Headway New Jersey Behavioral Health Services, P.C.',
            'Garden State Behavioral Health Services, P.C.'
        union all
        select 'CO',
            'Headway Colorado Behavioral Health Services, Inc.',
            'Mile High Behavioral Health Services, Inc.'
        union all
        select 'IL',
            'Headway Illinois Behavioral Health Services, PLLC',
            'Windy City Behavioral Health Services, PLLC'
        union all
        select 'WI',
            'Headway Wisconsin Behavioral Health Services, S.C.',
            'Badger Behavioral Health Services, S.C.'
        union all
        select 'KS',
            'Headway Kansas Behavioral Health Services P.A.',
            'Sunflower Behavioral Health Services, P.A.'

    ),

    optum_entity_pair_lookup AS (
        /*
            Single entity lookup.
            - This only looks for one entity at a time
            - This is essentially a lookup in attempt to match the entity names for both the ledger and billing
            - This reduces the need for downstream logic by centralizing it prior, instead of the join logic
            - Is only used to enrich state fields, not classifications
        */


        SELECT
            state_code,
            base_entity_name AS entity_name
        FROM optum_base_entity_pairs
        
        -- big query specific, need to remove for Snowflake
        UNION DISTINCT

        SELECT
            state_code,
            optum_entity_name AS entity_name
        FROM optum_base_entity_pairs
    ),

    optum_pair_keys AS (
        /*
            This is used to specifically look for acceptable optum pairs, agnostic of position, 
                by attempting to normalize the pairs and create a key
        */

        SELECT
            state_code,
            base_entity_name,
            optum_entity_name,

            -- if needed can create a hashing for this
            CONCAT(
                LEAST(LOWER(base_entity_name), LOWER(optum_entity_name)),
                '||',
                GREATEST(LOWER(base_entity_name), LOWER(optum_entity_name))
            ) AS optum_pair_key
        FROM optum_base_entity_pairs
    ),

    ledger_stg AS (
        SELECT
            appointment_id,
            appointment_start_at,
            appointment_status,
            event_type,
            corporate_entity_name,
            corporate_entity_tin,
            provider_id,
            provider_state,
            payer_id,
            created_at,
            /*
                manual mapping order of event type, in reverse order from last to first:
                1. provider payout:         1
                2. patient charge:          2
                3. appointment confirmed:   3

                This will help obtain the most recent event status of the appointment, logically speaking
            */
            CASE event_type
                WHEN 'PROVIDER_PAYOUT' THEN         1
                WHEN 'PATIENT_CHARGE' THEN          2
                WHEN 'APPOINTMENT_CONFIRMED' THEN   3
                ELSE                                4
            end AS ledger_event_priority
        FROM {{ source('hw_revenue_subledger_stg', 'revenue_subledger_preliminary') }}
        {{ date_between }}
    ),

    ledger_dedupe AS (
        SELECT
            *
        FROM ledger_stg
        QUALIFY ROW_NUMBER() OVER (PARTITION BY appointment_id ORDER BY ledger_event_priority, created_at DESC) = 1
    ),

    billing_stg AS (
        -- source of truth, started in April 15
        SELECT
            appointment_id,
            appointment_start_at,
            appointment_status,
            corporate_entity_id,
            corporate_entity_name,
            corporate_entity_tin,
            billing_lifecycle_stage,
            provider_id,
            payer_id,
            updated_at,
            CASE billing_lifecycle_stage
                WHEN 'ERA_RECEIVED' THEN            1
                WHEN 'CLAIM_FILED' THEN             2
                WHEN 'APPOINTMENT_CONFIRMED' THEN   3
                ELSE                                4
            END AS billing_priority
        FROM {{ source('hw_billing_stg', 'base_headway__billing_appointment_details') }}
        {{ date_between }}
    ),

    billing_dedupe AS (
        SELECT
            *
        FROM billing_stg
        QUALIFY ROW_NUMBER() OVER (PARTITION BY appointment_id ORDER BY billing_priority, updated_at DESC) = 1
    ),

    reconciliation_stg AS (
        SELECT
            -- coalesced values --
            COALESCE(b.appointment_id       , l.appointment_id)         AS appointment_id,
            COALESCE(b.appointment_start_at , l.appointment_start_at)   AS appointment_start_at,
            COALESCE(b.appointment_status   ,  l.appointment_status)    AS appointment_status,
            COALESCE(b.provider_id          , l.provider_id)            AS provider_id,
            COALESCE(b.payer_id             , l.payer_id)               AS payer_id,

            -- values from the ledger --
            l.provider_state                                            AS ledger_provider_state,
            l.corporate_entity_name                                     AS ledger_entity_name,
            l.corporate_entity_tin                                      AS ledger_entity_tin,
            l.event_type                                                AS ledger_current_event_type,
            l.created_at                                                AS ledger_created_at,
            
            -- values from billing (the SOT) --
            b.corporate_entity_id                                       AS billing_entity_id,
            b.corporate_entity_name                                     AS billing_entity_name,
            b.corporate_entity_tin                                      AS billing_entity_tin,
            b.billing_lifecycle_stage                                   AS billing_current_lifecycle_stage,
            b.updated_at                                                AS billing_updated_at,

            -- flags for if billing / ledger records are not available --
            l.appointment_id IS NOT NULL                                AS has_ledger_record,
            b.appointment_id IS NOT NULL                                AS has_billing_record,

            -- logic to identify if Optum is the payer --
            CASE 
                WHEN 
                    LOWER(COALESCE(b.payer_id, l.payer_id)) IN {{ optum_identifier }}
                    THEN TRUE
                ELSE FALSE     
            END                                                         AS is_optum_payer,

            CONCAT(
                COALESCE(l.corporate_entity_name, '<missing>'),
                ' <> ',
                COALESCE(b.corporate_entity_name, '<missing>')
            )                                                           AS entity_pair,
            CONCAT(
                LEAST(
                    LOWER(COALESCE(l.corporate_entity_name, '<missing>')),
                    LOWER(COALESCE(b.corporate_entity_name, '<missing>'))
                ),
                '||',
                GREATEST(
                    LOWER(COALESCE(l.corporate_entity_name, '<missing>')),
                    LOWER(COALESCE(b.corporate_entity_name, '<missing>'))
                )
            )                                                           AS entity_pair_key

        FROM ledger_dedupe l
        FULL OUTER JOIN 
            billing_dedupe b
            ON l.appointment_id = b.appointment_id
    ),

    reconciliation_processing AS (
        SELECT
            r.appointment_id,
            r.appointment_start_at,
            r.appointment_status,
            r.provider_id,
            r.payer_id,
            r.ledger_provider_state,
            r.ledger_entity_name,
            r.ledger_entity_tin,
            r.ledger_current_event_type,
            r.ledger_created_at,
            r.billing_entity_id,
            r.billing_entity_name,
            r.billing_entity_tin,
            r.billing_current_lifecycle_stage,
            r.billing_updated_at,
            r.has_ledger_record,
            r.has_billing_record,
            r.entity_pair,
            lo.state_code                       AS ledger_entity_state_from_name,
            bo.state_code                       AS billing_entity_state_from_name,
            is_optum_payer,
            CASE
                WHEN r.is_optum_payer = TRUE
                     AND pk.optum_pair_key IS NOT NULL
                    THEN TRUE
                ELSE FALSE
            END                                 AS is_known_optum_pair_mismatch
        FROM reconciliation_stg r
        LEFT JOIN 
            optum_entity_pair_lookup lo
            ON LOWER(r.ledger_entity_name) = LOWER(lo.entity_name)
        LEFT JOIN
            optum_entity_pair_lookup bo
            ON LOWER(r.billing_entity_name) = LOWER(bo.entity_name)
        LEFT JOIN optum_pair_keys pk
            ON r.entity_pair_key = pk.optum_pair_key
    ),

    reconciliation AS (
        SELECT
            *,

            /*
                This uses a hierarchy based off of ordered precedence and business/triage priority:
                1. Checks if records exist in both systems, if so...
                2. Checks whether required entity fields are present
                3. Checks if an exact match

                if not...

                4. Checks if it is a known exception (Optum pair keys)
                5. Checks type of mismatch subtypes (entity state, TIN, etc.) 
                6. Unclassified mismatch
            */


            CASE
                -- when the record is not found in the ledger --
                WHEN NOT has_ledger_record AND has_billing_record
                    THEN 'missing_in_ledger'

                -- when the record is not found in billing --
                WHEN has_ledger_record AND NOT has_billing_record
                    THEN 'missing_in_billing'

                -- when billing record is specifically missing entity fields --
                WHEN has_billing_record
                    AND (billing_entity_name IS NULL OR billing_entity_tin IS NULL)
                    THEN 'missing_billing_entity_fields'

                -- when the ledger record is specifically missing entity fields --
                WHEN has_ledger_record
                    AND (ledger_entity_name IS NULL OR ledger_entity_tin IS NULL)
                    THEN 'missing_ledger_entity_fields'

                -- entity names match, as well as the entity tin --
                WHEN LOWER(ledger_entity_name) = LOWER(billing_entity_name)
                    AND ledger_entity_tin = billing_entity_tin
                    THEN 'exact_match'

                -- optum/uhc payer where the ledger/billing entity pair matches a known approved mapping --
                WHEN is_known_optum_pair_mismatch
                    THEN 'known_optum_pair_mismatch'

                -- same entity between sources, however there is a TIN mismatch --
                WHEN LOWER(ledger_entity_name) = LOWER(billing_entity_name)
                    AND ledger_entity_tin != billing_entity_tin
                    THEN 'tin_mismatch_same_name'

                -- mismatch between inferred ledger and billing entity states --
                WHEN ledger_entity_state_from_name IS NOT NULL
                    AND billing_entity_state_from_name IS NOT NULL
                    AND ledger_entity_state_from_name != billing_entity_state_from_name
                    THEN 'cross_state_entity_mismatch'

                -- same inferred state on both sides, but different entities remain after prior match checks --
                WHEN ledger_entity_state_from_name IS NOT NULL
                    AND billing_entity_state_from_name IS NOT NULL
                    AND ledger_entity_state_from_name = billing_entity_state_from_name
                    THEN 'same_state_unexpected_entity_mismatch'

                ELSE 'unclassified_entity_mismatch'
            END AS reconciliation_classification
        FROM reconciliation_processing        
    ),

    summary AS (
        SELECT
            reconciliation_classification,
            COUNT(*) AS appointments
        FROM reconciliation
        GROUP BY 1        
    )

SELECT
    *
FROM summary