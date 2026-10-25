CREATE TABLE IF NOT EXISTS observations (
    step INTEGER NOT NULL,
    sensor_id INTEGER NOT NULL,
    event_time TEXT NOT NULL,
    flow DOUBLE PRECISION NOT NULL,
    occupancy DOUBLE PRECISION NOT NULL,
    speed DOUBLE PRECISION NOT NULL,
    density DOUBLE PRECISION NOT NULL,
    synthetic BOOLEAN NOT NULL DEFAULT FALSE,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (step, sensor_id)
);

CREATE TABLE IF NOT EXISTS forecasts (
    origin_step INTEGER NOT NULL,
    target_step INTEGER NOT NULL,
    sensor_id INTEGER NOT NULL,
    horizon_min INTEGER NOT NULL,
    predicted_density DOUBLE PRECISION NOT NULL,
    origin_density DOUBLE PRECISION NOT NULL,
    absolute_error DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (origin_step, sensor_id, horizon_min)
);

CREATE INDEX IF NOT EXISTS forecasts_target_idx ON forecasts(target_step, sensor_id);
CREATE INDEX IF NOT EXISTS forecasts_sensor_origin_idx ON forecasts(sensor_id, origin_step DESC);
CREATE INDEX IF NOT EXISTS observations_sensor_step_idx ON observations(sensor_id, step DESC);
