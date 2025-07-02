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
    county_enriched_data AS (
        -- Add county information using ZIP-county crosswalk
        -- For ZIPs that span multiple counties, select the one with highest ratio (largest population)
        SELECT 
            ed.*,
            zc.county AS county_fips,
            sfc.countyname_fips AS county_name,
            sfc.state_name,
            zc.tot_ratio AS zip_county_ratio,
            
            -- County info quality flag
            (zc.county IS NOT NULL AND sfc.countyname_fips IS NOT NULL) AS has_county_info,
            
            -- Rank counties by ratio for each ZIP (to handle multi-county ZIPs)
            ROW_NUMBER() OVER (
                PARTITION BY ed.npi 
                ORDER BY zc.tot_ratio DESC NULLS LAST
            ) AS county_rank
            
        FROM enriched_data ed
        LEFT JOIN zip_county zc ON ed.provider_postal_code_clean = zc.zip
        LEFT JOIN ssa_fips_state_county sfc ON zc.county = sfc.fipscounty
    ),
    final_enriched_data AS (
        -- Select only the primary county for each provider (highest ratio)
        SELECT 
            ced.*
        FROM county_enriched_data ced
        WHERE ced.county_rank = 1
    ),
    scored_data AS (
        SELECT 
            *,
            -- Calculate data quality score (0-100) - updated for Part 3
            (
                CASE WHEN entity_name IS NOT NULL AND entity_name != '' THEN 20 ELSE 0 END +
                CASE WHEN has_complete_address THEN 20 ELSE 0 END +
                CASE WHEN has_valid_zip THEN 20 ELSE 0 END +
                CASE WHEN has_primary_taxonomy THEN 20 ELSE 0 END +
                CASE WHEN has_county_info THEN 20 ELSE 0 END -- New county criterion
            ) AS data_quality_score
        FROM final_enriched_data
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
        county_fips, county_name, state_name, zip_county_ratio,
        has_complete_address, has_valid_zip, has_primary_taxonomy, has_county_info,
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
        county_fips, county_name, state_name, zip_county_ratio,
        has_complete_address, has_valid_zip, has_primary_taxonomy, has_county_info,
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
        county_fips = EXCLUDED.county_fips,
        county_name = EXCLUDED.county_name,
        state_name = EXCLUDED.state_name,
        zip_county_ratio = EXCLUDED.zip_county_ratio,
        has_complete_address = EXCLUDED.has_complete_address,
        has_valid_zip = EXCLUDED.has_valid_zip,
        has_primary_taxonomy = EXCLUDED.has_primary_taxonomy,
        has_county_info = EXCLUDED.has_county_info,
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

