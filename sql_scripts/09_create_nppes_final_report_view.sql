CREATE OR REPLACE VIEW nppes_final_export AS
SELECT 
    -- 1. NPI
    npi,
    
    -- 2. Entity Type (decoded from code)
    CASE 
        WHEN entity_type_code = 1 THEN 'Provider'
        WHEN entity_type_code = 2 THEN 'Facility'
        ELSE 'Unknown'
    END AS entity_type,
    
    -- 3. Entity Name (constructed based on entity type)
    entity_name,
    
    -- 4. Practice Address (Business Practice Location)
    provider_location_address_1, 
    provider_location_address_2,
    provider_city,
    provider_state,
    provider_postal_code,
    county_name,
    state_name,
    
    -- 5. Primary Taxonomy Code
    primary_taxonomy_code,
    
    -- 6. Taxonomy Details from NUCC file
    taxonomy_grouping,
    taxonomy_classification,
    taxonomy_specialization,

    --7. Data Quality Flags
    has_complete_address,
    has_valid_zip,
    has_primary_taxonomy,
    has_county_info,
    
    -- 8. Data Quality Score (0-100)
    data_quality_score

FROM nppes_providers_clean
ORDER BY state_name, county_name, entity_name;