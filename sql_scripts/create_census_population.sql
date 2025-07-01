CREATE TABLE IF NOT EXISTS census_county_population (
    id SERIAL PRIMARY KEY,
    name VARCHAR,
    population INTEGER,
    state_fips VARCHAR(2),
    county_fips VARCHAR(3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);