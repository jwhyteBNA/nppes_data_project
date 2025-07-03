-- Create a stored procedure to get chunked export data
CREATE OR REPLACE FUNCTION get_export_chunk(
    chunk_size INTEGER,
    chunk_offset INTEGER
)
RETURNS TABLE(
    npi VARCHAR(10),
    entity_type VARCHAR(20),
    entity_name VARCHAR(200),
    provider_location_address_1 VARCHAR(100),
    provider_location_address_2 VARCHAR(100),
    provider_city VARCHAR(100),
    provider_state VARCHAR(50),
    provider_postal_code_clean VARCHAR(5),
    county_name VARCHAR(100),
    state_name VARCHAR(50),
    primary_taxonomy_code VARCHAR(10),
    taxonomy_grouping VARCHAR(100),
    taxonomy_classification VARCHAR(100),
    taxonomy_specialization VARCHAR(100),
    data_quality_score INTEGER
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.npi, 
        c.entity_type, 
        c.entity_name,
        c.provider_location_address_1, 
        c.provider_location_address_2,
        c.provider_city, 
        c.provider_state, 
        c.provider_postal_code_clean, 
        c.county_name, 
        c.state_name,
        c.primary_taxonomy_code, 
        c.taxonomy_grouping, 
        c.taxonomy_classification, 
        c.taxonomy_specialization,
        c.data_quality_score
    FROM nppes_providers_clean c
    ORDER BY c.npi
    LIMIT chunk_size OFFSET chunk_offset;
END;
$$;