
-- Create a function to get export statistics
CREATE OR REPLACE FUNCTION get_export_statistics()
RETURNS TABLE(
    total_records BIGINT,
    records_with_county BIGINT,
    records_without_county BIGINT,
    county_coverage_percentage DECIMAL(5,2)
) 
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN county_name IS NOT NULL THEN 1 END) as records_with_county,
        COUNT(CASE WHEN county_name IS NULL THEN 1 END) as records_without_county,
        ROUND(
            (COUNT(CASE WHEN county_name IS NOT NULL THEN 1 END) * 100.0) / COUNT(*), 
            2
        ) as county_coverage_percentage
    FROM nppes_providers_clean;
END;
$$;
