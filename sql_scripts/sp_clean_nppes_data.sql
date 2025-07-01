-- =====================================================
-- Enhanced Data Cleaning Stored Procedure for NPPES
-- =====================================================

-- Create the cleaned NPPES providers table with proper constraints
CREATE TABLE IF NOT EXISTS nppes_providers_clean (
    npi VARCHAR(10) PRIMARY KEY,
    entity_type_code INTEGER NOT NULL CHECK (entity_type_code IN (1, 2)),
    entity_type VARCHAR(20) NOT NULL,
    entity_name VARCHAR(200),
    provider_organization_name VARCHAR(200),
    provider_last_name VARCHAR(100),
    provider_first_name VARCHAR(100),
    provider_middle_name VARCHAR(100),
    provider_name_prefix VARCHAR(10),
    provider_name_suffix VARCHAR(10),
    provider_credential VARCHAR(50),
    provider_other_organization_name VARCHAR(200),
    
    -- Address fields
    provider_location_address_1 VARCHAR(100),
    provider_location_address_2 VARCHAR(100),
    provider_city VARCHAR(100),
    provider_state VARCHAR(50),
    provider_postal_code VARCHAR(10),
    provider_postal_code_clean VARCHAR(5), -- 5-digit ZIP
    
    -- Primary taxonomy (most relevant for analytics)
    primary_taxonomy_code VARCHAR(20),
    primary_taxonomy_switch VARCHAR(1),
    
    -- Enriched taxonomy data
    taxonomy_grouping VARCHAR(100),
    taxonomy_classification VARCHAR(100),
    taxonomy_specialization VARCHAR(100),
    
    -- Data quality flags
    has_complete_address BOOLEAN DEFAULT FALSE,
    has_valid_zip BOOLEAN DEFAULT FALSE,
    has_primary_taxonomy BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    data_quality_score INTEGER DEFAULT 0, -- 0-100 score
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create performance indexes
CREATE INDEX IF NOT EXISTS idx_nppes_clean_state_city ON nppes_providers_clean (provider_state, provider_city);
CREATE INDEX IF NOT EXISTS idx_nppes_clean_entity_type ON nppes_providers_clean (entity_type_code);
CREATE INDEX IF NOT EXISTS idx_nppes_clean_taxonomy ON nppes_providers_clean (primary_taxonomy_code);
CREATE INDEX IF NOT EXISTS idx_nppes_clean_zip ON nppes_providers_clean (provider_postal_code_clean);
CREATE INDEX IF NOT EXISTS idx_nppes_clean_quality ON nppes_providers_clean (data_quality_score);

-- =====================================================
-- Helper Function: Extract Primary Taxonomy (Optimized)
-- =====================================================
CREATE OR REPLACE FUNCTION get_primary_taxonomy_code(
    code_1 VARCHAR, switch_1 VARCHAR,
    code_2 VARCHAR, switch_2 VARCHAR,
    code_3 VARCHAR, switch_3 VARCHAR,
    code_4 VARCHAR, switch_4 VARCHAR,
    code_5 VARCHAR, switch_5 VARCHAR,
    code_6 VARCHAR, switch_6 VARCHAR,
    code_7 VARCHAR, switch_7 VARCHAR,
    code_8 VARCHAR, switch_8 VARCHAR,
    code_9 VARCHAR, switch_9 VARCHAR,
    code_10 VARCHAR, switch_10 VARCHAR,
    code_11 VARCHAR, switch_11 VARCHAR,
    code_12 VARCHAR, switch_12 VARCHAR,
    code_13 VARCHAR, switch_13 VARCHAR,
    code_14 VARCHAR, switch_14 VARCHAR,
    code_15 VARCHAR, switch_15 VARCHAR
) RETURNS VARCHAR AS $$
BEGIN
    -- Return the first taxonomy code with 'Y' switch
    RETURN CASE
        WHEN switch_1 = 'Y' THEN code_1
        WHEN switch_2 = 'Y' THEN code_2
        WHEN switch_3 = 'Y' THEN code_3
        WHEN switch_4 = 'Y' THEN code_4
        WHEN switch_5 = 'Y' THEN code_5
        WHEN switch_6 = 'Y' THEN code_6
        WHEN switch_7 = 'Y' THEN code_7
        WHEN switch_8 = 'Y' THEN code_8
        WHEN switch_9 = 'Y' THEN code_9
        WHEN switch_10 = 'Y' THEN code_10
        WHEN switch_11 = 'Y' THEN code_11
        WHEN switch_12 = 'Y' THEN code_12
        WHEN switch_13 = 'Y' THEN code_13
        WHEN switch_14 = 'Y' THEN code_14
        WHEN switch_15 = 'Y' THEN code_15
        ELSE COALESCE(code_1, code_2, code_3) -- Fallback to first available
    END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- =====================================================
-- Main Cleaning Stored Procedure
-- =====================================================
CREATE OR REPLACE PROCEDURE clean_and_populate_nppes_data()
LANGUAGE plpgsql
AS $$
DECLARE
    processed_count INTEGER;
    start_time TIMESTAMP;
BEGIN
    start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE 'Starting NPPES data cleaning at %', start_time;
    
    -- Truncate clean table for fresh load
    TRUNCATE TABLE nppes_providers_clean;
    
    -- Main cleaning and transformation with optimized CTEs
    WITH cleaned_raw_data AS (
        SELECT 
            TRIM(npi) AS npi,
            CASE 
                WHEN TRIM(entity_type_code) = '' OR entity_type_code IS NULL THEN NULL
                ELSE entity_type_code::INTEGER 
            END AS entity_type_code,
            LEFT(TRIM(provider_organization_name), 200) AS provider_organization_name,
            LEFT(TRIM(provider_last_name), 100) AS provider_last_name,
            LEFT(TRIM(provider_first_name), 100) AS provider_first_name,
            LEFT(TRIM(provider_middle_name), 100) AS provider_middle_name,
            LEFT(TRIM(provider_name_prefix), 10) AS provider_name_prefix,
            LEFT(TRIM(provider_name_suffix), 10) AS provider_name_suffix,
            LEFT(TRIM(provider_credential), 50) AS provider_credential,
            LEFT(TRIM(provider_other_organization_name), 200) AS provider_other_organization_name,
            
            -- Address cleaning
            LEFT(TRIM(provider_location_address_1), 100) AS provider_location_address_1,
            LEFT(TRIM(provider_location_address_2), 100) AS provider_location_address_2,
            LEFT(TRIM(provider_city), 100) AS provider_city,
            LEFT(TRIM(provider_state), 50) AS provider_state,
            LEFT(TRIM(provider_postal_code), 10) AS provider_postal_code,
            
            -- Extract 5-digit ZIP
            SUBSTRING(TRIM(provider_postal_code) FROM '^(\d{5})') AS provider_postal_code_clean,
            
            -- Get primary taxonomy using helper function
            get_primary_taxonomy_code(
                healthcare_provider_taxonomy_code_1, healthcare_provider_primary_taxonomy_switch_1,
                healthcare_provider_taxonomy_code_2, healthcare_provider_primary_taxonomy_switch_2,
                healthcare_provider_taxonomy_code_3, healthcare_provider_primary_taxonomy_switch_3,
                healthcare_provider_taxonomy_code_4, healthcare_provider_primary_taxonomy_switch_4,
                healthcare_provider_taxonomy_code_5, healthcare_provider_primary_taxonomy_switch_5,
                healthcare_provider_taxonomy_code_6, healthcare_provider_primary_taxonomy_switch_6,
                healthcare_provider_taxonomy_code_7, healthcare_provider_primary_taxonomy_switch_7,
                healthcare_provider_taxonomy_code_8, healthcare_provider_primary_taxonomy_switch_8,
                healthcare_provider_taxonomy_code_9, healthcare_provider_primary_taxonomy_switch_9,
                healthcare_provider_taxonomy_code_10, healthcare_provider_primary_taxonomy_switch_10,
                healthcare_provider_taxonomy_code_11, healthcare_provider_primary_taxonomy_switch_11,
                healthcare_provider_taxonomy_code_12, healthcare_provider_primary_taxonomy_switch_12,
                healthcare_provider_taxonomy_code_13, healthcare_provider_primary_taxonomy_switch_13,
                healthcare_provider_taxonomy_code_14, healthcare_provider_primary_taxonomy_switch_14,
                healthcare_provider_taxonomy_code_15, healthcare_provider_primary_taxonomy_switch_15
            ) AS primary_taxonomy_code
            
        FROM nppes_providers
        WHERE TRIM(npi) IS NOT NULL 
          AND TRIM(npi) != ''
          AND LENGTH(TRIM(npi)) = 10
          AND TRIM(npi) ~ '^\d{10}$' -- Only numeric NPIs
          AND TRIM(entity_type_code) IS NOT NULL
          AND TRIM(entity_type_code) != ''
          AND TRIM(entity_type_code) ~ '^\d+$' -- Only numeric entity codes
    ),
    enriched_data AS (
        SELECT 
            crd.*,
            
            -- Entity type standardization
            CASE 
                WHEN crd.entity_type_code = 1 THEN 'Individual'
                WHEN crd.entity_type_code = 2 THEN 'Organization'
                ELSE 'Unknown'
            END AS entity_type,
            
            -- Construct entity name
            LEFT(CASE 
                WHEN crd.entity_type_code = 1 THEN 
                    TRIM(CONCAT_WS(' ',
                        NULLIF(crd.provider_name_prefix, ''),
                        NULLIF(crd.provider_first_name, ''),
                        NULLIF(crd.provider_middle_name, ''),
                        NULLIF(crd.provider_last_name, ''),
                        NULLIF(crd.provider_name_suffix, ''),
                        NULLIF(crd.provider_credential, '')
                    ))
                WHEN crd.entity_type_code = 2 THEN 
                    COALESCE(NULLIF(crd.provider_organization_name, ''), 'Unknown Organization')
                ELSE 'Unknown'
            END, 200) AS entity_name,
            
            -- Data quality flags
            (crd.provider_location_address_1 IS NOT NULL 
             AND crd.provider_city IS NOT NULL 
             AND crd.provider_state IS NOT NULL) AS has_complete_address,
             
            (crd.provider_postal_code_clean IS NOT NULL 
             AND LENGTH(crd.provider_postal_code_clean) = 5) AS has_valid_zip,
             
            (crd.primary_taxonomy_code IS NOT NULL 
             AND crd.primary_taxonomy_code != '') AS has_primary_taxonomy,
            
            -- Join taxonomy reference data
            LEFT(nt.grouping, 100) AS taxonomy_grouping,
            LEFT(nt.classification, 100) AS taxonomy_classification,
            LEFT(nt.specialization, 100) AS taxonomy_specialization
            
        FROM cleaned_raw_data crd
        LEFT JOIN nucc_taxonomy nt ON crd.primary_taxonomy_code = nt.code
    ),
    scored_data AS (
        SELECT 
            *,
            -- Calculate data quality score (0-100)
            (
                CASE WHEN entity_name IS NOT NULL AND entity_name != '' THEN 25 ELSE 0 END +
                CASE WHEN has_complete_address THEN 25 ELSE 0 END +
                CASE WHEN has_valid_zip THEN 25 ELSE 0 END +
                CASE WHEN has_primary_taxonomy THEN 25 ELSE 0 END
            ) AS data_quality_score
        FROM enriched_data
    )
    
    -- Insert into clean table with UPSERT
    INSERT INTO nppes_providers_clean (
        npi, entity_type_code, entity_type, entity_name,
        provider_organization_name, provider_last_name, provider_first_name,
        provider_middle_name, provider_name_prefix, provider_name_suffix,
        provider_credential, provider_other_organization_name,
        provider_location_address_1, provider_location_address_2,
        provider_city, provider_state, provider_postal_code, provider_postal_code_clean,
        primary_taxonomy_code, taxonomy_grouping, taxonomy_classification, taxonomy_specialization,
        has_complete_address, has_valid_zip, has_primary_taxonomy,
        data_quality_score, updated_at
    )
    SELECT 
        npi, entity_type_code, entity_type, entity_name,
        provider_organization_name, provider_last_name, provider_first_name,
        provider_middle_name, provider_name_prefix, provider_name_suffix,
        provider_credential, provider_other_organization_name,
        provider_location_address_1, provider_location_address_2,
        provider_city, provider_state, provider_postal_code, provider_postal_code_clean,
        primary_taxonomy_code, taxonomy_grouping, taxonomy_classification, taxonomy_specialization,
        has_complete_address, has_valid_zip, has_primary_taxonomy,
        data_quality_score, CURRENT_TIMESTAMP
    FROM scored_data
    
    ON CONFLICT (npi) DO UPDATE SET
        entity_type_code = EXCLUDED.entity_type_code,
        entity_type = EXCLUDED.entity_type,
        entity_name = EXCLUDED.entity_name,
        provider_organization_name = EXCLUDED.provider_organization_name,
        provider_last_name = EXCLUDED.provider_last_name,
        provider_first_name = EXCLUDED.provider_first_name,
        provider_middle_name = EXCLUDED.provider_middle_name,
        provider_name_prefix = EXCLUDED.provider_name_prefix,
        provider_name_suffix = EXCLUDED.provider_name_suffix,
        provider_credential = EXCLUDED.provider_credential,
        provider_other_organization_name = EXCLUDED.provider_other_organization_name,
        provider_location_address_1 = EXCLUDED.provider_location_address_1,
        provider_location_address_2 = EXCLUDED.provider_location_address_2,
        provider_city = EXCLUDED.provider_city,
        provider_state = EXCLUDED.provider_state,
        provider_postal_code = EXCLUDED.provider_postal_code,
        provider_postal_code_clean = EXCLUDED.provider_postal_code_clean,
        primary_taxonomy_code = EXCLUDED.primary_taxonomy_code,
        taxonomy_grouping = EXCLUDED.taxonomy_grouping,
        taxonomy_classification = EXCLUDED.taxonomy_classification,
        taxonomy_specialization = EXCLUDED.taxonomy_specialization,
        has_complete_address = EXCLUDED.has_complete_address,
        has_valid_zip = EXCLUDED.has_valid_zip,
        has_primary_taxonomy = EXCLUDED.has_primary_taxonomy,
        data_quality_score = EXCLUDED.data_quality_score,
        updated_at = CURRENT_TIMESTAMP;
    
    GET DIAGNOSTICS processed_count = ROW_COUNT;
    
    -- Update statistics
    ANALYZE nppes_providers_clean;
    
    RAISE NOTICE 'NPPES data cleaning completed at %. Records processed: %. Duration: %', 
        CLOCK_TIMESTAMP(), 
        processed_count,
        CLOCK_TIMESTAMP() - start_time;
        
END;
$$;

