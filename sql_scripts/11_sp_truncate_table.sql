-- =====================================================
-- Stored Procedure to Truncate Table
-- =====================================================
CREATE OR REPLACE PROCEDURE truncate_table(table_name TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE format('TRUNCATE TABLE %I', table_name);
    RAISE NOTICE 'Table % truncated successfully', table_name;
END;
$$;
