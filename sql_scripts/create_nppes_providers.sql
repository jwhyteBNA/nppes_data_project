    -- NPPES Providers Raw Data Table
-- Stores healthcare provider information from NPPES data dissemination file
-- This is the staging table for raw data before cleaning and transformation
CREATE TABLE IF NOT EXISTS nppes_providers (
        -- Core Provider Identity
        npi VARCHAR(10) PRIMARY KEY,                    -- National Provider Identifier (10-digit unique number)
        entity_type_code VARCHAR(10),                   -- 1 = Individual Provider, 2 = Organization Provider
        
        -- Organization Information (for entity type 2)
        provider_organization_name VARCHAR(100),        -- Legal business name for organizations
        provider_other_organization_name VARCHAR(100),  -- Alternative organization name
        
        -- Individual Provider Information (for entity type 1)
        provider_last_name VARCHAR(100),               -- Individual provider's last name
        provider_first_name VARCHAR(100),              -- Individual provider's first name
        provider_middle_name VARCHAR(100),             -- Individual provider's middle name
        provider_name_prefix VARCHAR(20),              -- Name prefix (Dr., Mr., etc.)
        provider_name_suffix VARCHAR(20),              -- Name suffix (Jr., Sr., etc.)
        provider_credential VARCHAR(100),              -- Professional credentials (MD, RN, etc.)
        
        -- Business Practice Location Address (not mailing address)
        provider_location_address_1 VARCHAR(100),      -- First line of practice address
        provider_location_address_2 VARCHAR(100),      -- Second line of practice address
        provider_city VARCHAR(100),                    -- Practice location city
        provider_state VARCHAR(50),                    -- Practice location state
        provider_postal_code VARCHAR(20),              -- Practice location ZIP code
        
        -- Healthcare Provider Taxonomy Codes (up to 15 allowed)
        -- Each provider can have multiple taxonomy codes indicating their specialties/classifications
        healthcare_provider_taxonomy_code_1 VARCHAR(20),        -- First taxonomy code
        healthcare_provider_primary_taxonomy_switch_1 VARCHAR(10),  -- Y if this is the primary taxonomy
        healthcare_provider_taxonomy_code_2 VARCHAR(20),        -- Second taxonomy code
        healthcare_provider_primary_taxonomy_switch_2 VARCHAR(10),  -- Y if this is the primary taxonomy
        healthcare_provider_taxonomy_code_3 VARCHAR(20),        -- Third taxonomy code
        healthcare_provider_primary_taxonomy_switch_3 VARCHAR(10),  -- Y if this is the primary taxonomy
        healthcare_provider_taxonomy_code_4 VARCHAR(20),        -- Fourth taxonomy code
        healthcare_provider_primary_taxonomy_switch_4 VARCHAR(10),  -- Y if this is the primary taxonomy
        healthcare_provider_taxonomy_code_5 VARCHAR(20),        -- Fifth taxonomy code
        healthcare_provider_primary_taxonomy_switch_5 VARCHAR(10),  -- Y if this is the primary taxonomy
        healthcare_provider_taxonomy_code_6 VARCHAR(20),        -- Sixth taxonomy code
        healthcare_provider_primary_taxonomy_switch_6 VARCHAR(10),  -- Y if this is the primary taxonomy
        healthcare_provider_taxonomy_code_7 VARCHAR(20),        -- Seventh taxonomy code
        healthcare_provider_primary_taxonomy_switch_7 VARCHAR(10),  -- Y if this is the primary taxonomy
        healthcare_provider_taxonomy_code_8 VARCHAR(20),        -- Eighth taxonomy code
        healthcare_provider_primary_taxonomy_switch_8 VARCHAR(10),  -- Y if this is the primary taxonomy
        healthcare_provider_taxonomy_code_9 VARCHAR(20),        -- Ninth taxonomy code
        healthcare_provider_primary_taxonomy_switch_9 VARCHAR(10),  -- Y if this is the primary taxonomy
        healthcare_provider_taxonomy_code_10 VARCHAR(20),       -- Tenth taxonomy code
        healthcare_provider_primary_taxonomy_switch_10 VARCHAR(10), -- Y if this is the primary taxonomy
        healthcare_provider_taxonomy_code_11 VARCHAR(20),       -- Eleventh taxonomy code
        healthcare_provider_primary_taxonomy_switch_11 VARCHAR(10), -- Y if this is the primary taxonomy
        healthcare_provider_taxonomy_code_12 VARCHAR(20),       -- Twelfth taxonomy code
        healthcare_provider_primary_taxonomy_switch_12 VARCHAR(10), -- Y if this is the primary taxonomy
        healthcare_provider_taxonomy_code_13 VARCHAR(20),       -- Thirteenth taxonomy code
        healthcare_provider_primary_taxonomy_switch_13 VARCHAR(10), -- Y if this is the primary taxonomy
        healthcare_provider_taxonomy_code_14 VARCHAR(20),       -- Fourteenth taxonomy code
        healthcare_provider_primary_taxonomy_switch_14 VARCHAR(10), -- Y if this is the primary taxonomy
        healthcare_provider_taxonomy_code_15 VARCHAR(20),       -- Fifteenth taxonomy code
        healthcare_provider_primary_taxonomy_switch_15 VARCHAR(10), -- Y if this is the primary taxonomy
        
        -- Audit Fields
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Record creation timestamp
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Record last update timestamp
    );

-- Performance Indexes
-- These indexes improve query performance for common search patterns
CREATE INDEX IF NOT EXISTS idx_nppes_entity_type ON nppes_providers(entity_type_code);        -- Filter by provider type
CREATE INDEX IF NOT EXISTS idx_nppes_state ON nppes_providers(provider_state);                -- Geographic queries by state
CREATE INDEX IF NOT EXISTS idx_nppes_postal_code ON nppes_providers(provider_postal_code);    -- Geographic queries by ZIP
CREATE INDEX IF NOT EXISTS idx_nppes_last_name ON nppes_providers(provider_last_name);        -- Search individual providers by name
CREATE INDEX IF NOT EXISTS idx_nppes_organization ON nppes_providers(provider_organization_name); -- Search organizations by name
CREATE INDEX IF NOT EXISTS idx_nppes_primary_taxonomy ON nppes_providers(primary_taxonomy_code);   -- Filter by specialty/classification

-- Table and Column Documentation
COMMENT ON TABLE nppes_providers IS 'Raw staging table for healthcare providers from NPPES data dissemination file. Contains unprocessed provider information before cleaning and transformation.';
COMMENT ON COLUMN nppes_providers.npi IS 'National Provider Identifier - unique 10-digit number assigned to healthcare providers';
COMMENT ON COLUMN nppes_providers.entity_type_code IS '1 = Individual Provider (doctors, nurses, etc.), 2 = Organization Provider (hospitals, clinics, etc.)';
COMMENT ON COLUMN nppes_providers.provider_postal_code IS 'ZIP code for business practice location (not mailing address). May need standardization for county mapping.';
COMMENT ON COLUMN nppes_providers.primary_taxonomy_code IS 'Primary healthcare provider taxonomy code (derived from taxonomy fields where primary switch = Y). Used for specialty classification.';