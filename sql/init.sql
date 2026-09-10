CREATE TABLE IF NOT EXISTS weather (
    id SERIAL PRIMARY KEY,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    observation_time TIMESTAMP NOT NULL,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(latitude, longitude, observation_time)
);