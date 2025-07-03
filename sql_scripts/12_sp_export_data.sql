-- =====================================================
-- Stored Procedure to Export Data to CSV
-- =====================================================
-- This procedure exports data from the clean table to a CSV file
-- using PostgreSQL's COPY command for optimal performance

CREATE OR REPLACE PROCEDURE export_nppes_data_to_csv(
    export_file_path TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Export data directly to CSV using COPY command
    EXECUTE FORMAT(
        'COPY (
            SELECT 
                npi, 
                entity_type, 
                entity_name,
                provider_location_address_1, 
                provider_location_address_2,
                provider_city, 
                provider_state, 
                provider_postal_code_clean, 
                county_name, 
                state_name,
                primary_taxonomy_code, 
                taxonomy_grouping, 
                taxonomy_classification, 
                taxonomy_specialization,
                data_quality_score
            FROM nppes_providers_clean 
            ORDER BY npi
        ) TO %L WITH (
            FORMAT CSV, 
            HEADER TRUE, 
            DELIMITER %L,
            ENCODING ''UTF8''
        )',
        export_file_path,
        ','
    );
    
    RAISE NOTICE 'Data exported to CSV file: %', export_file_path;
END;
$$;





