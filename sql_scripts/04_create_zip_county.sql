CREATE TABLE IF NOT EXISTS zip_county (
    id SERIAL PRIMARY KEY,
    zip VARCHAR(7),
    county VARCHAR(7),
    usps_zip_pref_city VARCHAR(100),
    usps_zip_pref_state VARCHAR(2),
    res_ratio DECIMAL(15,10),
    bus_ratio DECIMAL(15,10),
    oth_ratio DECIMAL(15,10),
    tot_ratio DECIMAL(15,10)
);