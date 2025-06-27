    CREATE TABLE IF NOT EXISTS nppes_providers (
        npi VARCHAR(10) PRIMARY KEY,
        entity_type_code INTEGER NOT NULL,
        provider_organization_name TEXT,
        provider_last_name VARCHAR(100),
        provider_first_name VARCHAR(100),
        provider_middle_name VARCHAR(100),
        provider_name_prefix VARCHAR(20),
        provider_name_suffix VARCHAR(20),
        provider_credential VARCHAR(100),
        provider_other_organization_name TEXT,
        provider_location_address_1 TEXT,
        provider_location_address_2 TEXT,
        provider_city VARCHAR(100),
        provider_state VARCHAR(50),
        provider_postal_code VARCHAR(20),
        healthcare_provider_taxonomy_code_1 VARCHAR(20),
        healthcare_provider_primary_taxonomy_switch_1 VARCHAR(1),
        healthcare_provider_taxonomy_code_2 VARCHAR(20),
        healthcare_provider_primary_taxonomy_switch_2 VARCHAR(1),
        healthcare_provider_taxonomy_code_3 VARCHAR(20),
        healthcare_provider_primary_taxonomy_switch_3 VARCHAR(1),
        healthcare_provider_taxonomy_code_4 VARCHAR(20),
        healthcare_provider_primary_taxonomy_switch_4 VARCHAR(1),
        healthcare_provider_taxonomy_code_5 VARCHAR(20),
        healthcare_provider_primary_taxonomy_switch_5 VARCHAR(1),
        healthcare_provider_taxonomy_code_6 VARCHAR(20),
        healthcare_provider_primary_taxonomy_switch_6 VARCHAR(1),
        healthcare_provider_taxonomy_code_7 VARCHAR(20),
        healthcare_provider_primary_taxonomy_switch_7 VARCHAR(1),
        healthcare_provider_taxonomy_code_8 VARCHAR(20),
        healthcare_provider_primary_taxonomy_switch_8 VARCHAR(1),
        healthcare_provider_taxonomy_code_9 VARCHAR(20),
        healthcare_provider_primary_taxonomy_switch_9 VARCHAR(1),
        healthcare_provider_taxonomy_code_10 VARCHAR(20),
        healthcare_provider_primary_taxonomy_switch_10 VARCHAR(1),
        healthcare_provider_taxonomy_code_11 VARCHAR(20),
        healthcare_provider_primary_taxonomy_switch_11 VARCHAR(1),
        healthcare_provider_taxonomy_code_12 VARCHAR(20),
        healthcare_provider_primary_taxonomy_switch_12 VARCHAR(1),
        healthcare_provider_taxonomy_code_13 VARCHAR(20),
        healthcare_provider_primary_taxonomy_switch_13 VARCHAR(1),
        healthcare_provider_taxonomy_code_14 VARCHAR(20),
        healthcare_provider_primary_taxonomy_switch_14 VARCHAR(1),
        healthcare_provider_taxonomy_code_15 VARCHAR(20),
        healthcare_provider_primary_taxonomy_switch_15 VARCHAR(1),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_nppes_entity_type ON nppes_providers(entity_type_code);
CREATE INDEX IF NOT EXISTS idx_nppes_state ON nppes_providers(provider_state);
CREATE INDEX IF NOT EXISTS idx_nppes_postal_code ON nppes_providers(provider_postal_code);
CREATE INDEX IF NOT EXISTS idx_nppes_last_name ON nppes_providers(provider_last_name);
CREATE INDEX IF NOT EXISTS idx_nppes_organization ON nppes_providers(provider_organization_name);
CREATE INDEX IF NOT EXISTS idx_nppes_primary_taxonomy ON nppes_providers(primary_taxonomy_code);

-- Add comments
COMMENT ON TABLE nppes_providers IS 'Healthcare providers from NPPES data dissemination file';
COMMENT ON COLUMN nppes_providers.npi IS 'National Provider Identifier - unique 10-digit number';
COMMENT ON COLUMN nppes_providers.entity_type_code IS '1 = Individual Provider, 2 = Organization Provider';
COMMENT ON COLUMN nppes_providers.primary_taxonomy_code IS 'Primary healthcare provider taxonomy code (derived from taxonomy fields where primary switch = Y)';