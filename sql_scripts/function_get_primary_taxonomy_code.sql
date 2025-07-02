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