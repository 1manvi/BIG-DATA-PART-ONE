CREATE TABLE sensors (
    id INTEGER PRIMARY KEY,
    name TEXT,
    district TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);

CREATE TABLE readings (
    sensor_id INTEGER NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    intensity DOUBLE PRECISION,
    occupancy DOUBLE PRECISION,
    load DOUBLE PRECISION,
    speed DOUBLE PRECISION,
    error TEXT,

    PRIMARY KEY (sensor_id, timestamp),

    FOREIGN KEY (sensor_id)
        REFERENCES sensors(id)
);

CREATE TABLE weather (
    timestamp TIMESTAMP PRIMARY KEY,
    temperature DOUBLE PRECISION,
    rain DOUBLE PRECISION
);

CREATE TABLE calendar (
    date DATE PRIMARY KEY,
    weekday_name TEXT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    month INTEGER NOT NULL
);