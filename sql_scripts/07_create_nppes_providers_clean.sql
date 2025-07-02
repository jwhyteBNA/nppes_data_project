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
    
    -- County information (Part 3)
    county_fips VARCHAR(5),
    county_name VARCHAR(100),
    state_name VARCHAR(50),
    zip_county_ratio DECIMAL(5,4), -- Ratio of ZIP in this county
    
    -- Data quality flags
    has_complete_address BOOLEAN DEFAULT FALSE,
    has_valid_zip BOOLEAN DEFAULT FALSE,
    has_primary_taxonomy BOOLEAN DEFAULT FALSE,
    has_county_info BOOLEAN DEFAULT FALSE,
    
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
CREATE INDEX IF NOT EXISTS idx_nppes_clean_county ON nppes_providers_clean (county_fips);
CREATE INDEX IF NOT EXISTS idx_nppes_clean_state ON nppes_providers_clean (state_name);
