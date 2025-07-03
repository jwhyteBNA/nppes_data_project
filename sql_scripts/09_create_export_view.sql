-- =====================================================
-- Export View for CSV Generation
-- =====================================================
CREATE OR REPLACE VIEW nppes_export_view AS
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
ORDER BY npi;

-- Add comment for documentation
COMMENT ON VIEW nppes_export_view IS 'View for exporting clean NPPES data to CSV format with all required fields';
