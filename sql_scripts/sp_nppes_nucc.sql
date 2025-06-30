-- Create the provider taxonomy export table
CREATE TABLE IF NOT EXISTS provider_taxonomy_export (
    npi VARCHAR(10) PRIMARY KEY,
    entity_type VARCHAR(20) NOT NULL,
    entity_name VARCHAR(200),
    provider_location_address_1 VARCHAR(100),
    provider_location_address_2 VARCHAR(100),
    provider_city VARCHAR(100),
    provider_state VARCHAR(50),
    provider_postal_code VARCHAR(10),
    primary_taxonomy_code VARCHAR(20),
    grouping VARCHAR(100),
    classification VARCHAR(100),
    specialization VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for optimal performance
CREATE INDEX IF NOT EXISTS idx_provider_taxonomy_state_city 
    ON provider_taxonomy_export (provider_state, provider_city);
CREATE INDEX IF NOT EXISTS idx_provider_taxonomy_entity_type 
    ON provider_taxonomy_export (entity_type);
CREATE INDEX IF NOT EXISTS idx_provider_taxonomy_code 
    ON provider_taxonomy_export (primary_taxonomy_code);
CREATE INDEX IF NOT EXISTS idx_provider_taxonomy_grouping 
    ON provider_taxonomy_export (grouping);
CREATE INDEX IF NOT EXISTS idx_provider_taxonomy_updated 
    ON provider_taxonomy_export (updated_at);

-- Main procedure to populate/update the provider taxonomy export table
CREATE OR REPLACE PROCEDURE populate_provider_taxonomy_export()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Log start
    RAISE NOTICE 'Starting provider taxonomy export population at %', NOW();
    
    -- Insert or update provider taxonomy data using UPSERT
    INSERT INTO provider_taxonomy_export (
        npi, entity_type, entity_name,
        provider_location_address_1, provider_location_address_2,
        provider_city, provider_state, provider_postal_code,
        primary_taxonomy_code, grouping, classification, specialization,
        updated_at
    )
    WITH primary_taxonomy AS (
        SELECT 
            npi,
            entity_type_code,
            provider_organization_name,
            provider_last_name,
            provider_first_name,
            provider_middle_name,
            provider_name_prefix,
            provider_name_suffix,
            provider_credential,
            provider_location_address_1,
            provider_location_address_2,
            provider_city,
            provider_state,
            provider_postal_code,
            CASE
                WHEN healthcare_provider_primary_taxonomy_switch_1 = 'Y' THEN healthcare_provider_taxonomy_code_1
                WHEN healthcare_provider_primary_taxonomy_switch_2 = 'Y' THEN healthcare_provider_taxonomy_code_2
                WHEN healthcare_provider_primary_taxonomy_switch_3 = 'Y' THEN healthcare_provider_taxonomy_code_3
                WHEN healthcare_provider_primary_taxonomy_switch_4 = 'Y' THEN healthcare_provider_taxonomy_code_4
                WHEN healthcare_provider_primary_taxonomy_switch_5 = 'Y' THEN healthcare_provider_taxonomy_code_5
                WHEN healthcare_provider_primary_taxonomy_switch_6 = 'Y' THEN healthcare_provider_taxonomy_code_6
                WHEN healthcare_provider_primary_taxonomy_switch_7 = 'Y' THEN healthcare_provider_taxonomy_code_7
                WHEN healthcare_provider_primary_taxonomy_switch_8 = 'Y' THEN healthcare_provider_taxonomy_code_8
                WHEN healthcare_provider_primary_taxonomy_switch_9 = 'Y' THEN healthcare_provider_taxonomy_code_9
                WHEN healthcare_provider_primary_taxonomy_switch_10 = 'Y' THEN healthcare_provider_taxonomy_code_10
                WHEN healthcare_provider_primary_taxonomy_switch_11 = 'Y' THEN healthcare_provider_taxonomy_code_11
                WHEN healthcare_provider_primary_taxonomy_switch_12 = 'Y' THEN healthcare_provider_taxonomy_code_12
                WHEN healthcare_provider_primary_taxonomy_switch_13 = 'Y' THEN healthcare_provider_taxonomy_code_13
                WHEN healthcare_provider_primary_taxonomy_switch_14 = 'Y' THEN healthcare_provider_taxonomy_code_14
                WHEN healthcare_provider_primary_taxonomy_switch_15 = 'Y' THEN healthcare_provider_taxonomy_code_15
                ELSE NULL
            END AS primary_taxonomy_code
        FROM nppes_providers
    ),
    provider_with_name AS (
        SELECT
            npi,
            entity_type_code,
            primary_taxonomy_code,
            CASE 
                WHEN entity_type_code = 1 THEN 
                    TRIM(CONCAT_WS(' ',
                        provider_name_prefix,
                        provider_first_name,
                        provider_middle_name,
                        provider_last_name,
                        provider_name_suffix,
                        provider_credential
                    ))
                WHEN entity_type_code = 2 THEN 
                    provider_organization_name
                ELSE 'Unknown'
            END AS entity_name,
            provider_location_address_1,
            provider_location_address_2,
            provider_city,
            provider_state,
            provider_postal_code
        FROM primary_taxonomy
    )
    SELECT 
        pwn.npi,
        CASE 
            WHEN pwn.entity_type_code = 1 THEN 'Provider'
            WHEN pwn.entity_type_code = 2 THEN 'Facility'
            ELSE 'Unknown'
        END AS entity_type,
        pwn.entity_name,
        pwn.provider_location_address_1,
        pwn.provider_location_address_2,
        pwn.provider_city,
        pwn.provider_state,
        pwn.provider_postal_code,
        pwn.primary_taxonomy_code,
        nt.grouping,
        nt.classification,
        nt.specialization,
        CURRENT_TIMESTAMP
    FROM provider_with_name pwn
    LEFT JOIN nucc_taxonomy nt ON pwn.primary_taxonomy_code = nt.code
    
    -- UPSERT: Insert new records or update existing ones
    ON CONFLICT (npi) DO UPDATE SET
        entity_type = EXCLUDED.entity_type,
        entity_name = EXCLUDED.entity_name,
        provider_location_address_1 = EXCLUDED.provider_location_address_1,
        provider_location_address_2 = EXCLUDED.provider_location_address_2,
        provider_city = EXCLUDED.provider_city,
        provider_state = EXCLUDED.provider_state,
        provider_postal_code = EXCLUDED.provider_postal_code,
        primary_taxonomy_code = EXCLUDED.primary_taxonomy_code,
        grouping = EXCLUDED.grouping,
        classification = EXCLUDED.classification,
        specialization = EXCLUDED.specialization,
        updated_at = CURRENT_TIMESTAMP;
    
    -- Get count of affected rows
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    
    -- Log completion
    RAISE NOTICE 'Provider taxonomy export completed at %. Records processed: %', NOW(), updated_count;
END;
$$;