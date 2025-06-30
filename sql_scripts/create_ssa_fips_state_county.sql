CREATE TABLE IF NOT EXISTS ssa_fips_state_county (
    id SERIAL PRIMARY KEY,
    fipscounty VARCHAR(7),
    countyname_fips VARCHAR(128),
    state VARCHAR(2),
    cbsa_code VARCHAR(10),
    cbsa_name VARCHAR(100),
    ssa_code VARCHAR(7),
    state_name VARCHAR(50),
    countyname_rate VARCHAR(100)
);