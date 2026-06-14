CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE fact_subway_spatial_stream (
    id BIGSERIAL PRIMARY KEY,
    train_id VARCHAR(100) NOT NULL,
    route_id VARCHAR(10), 
    coordinates GEOMETRY(Point, 4326) NOT NULL,        -- Native spatial geometry point
    current_status VARCHAR(50),
    potato_status VARCHAR(20) DEFAULT 'SAFE',
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL, -- Real-world time from the API
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- spatial and temporal performance indexes
CREATE INDEX idx_subway_stream_spatial ON fact_subway_spatial_stream USING GIST(coordinates);
CREATE INDEX idx_subway_stream_event_time ON fact_subway_spatial_stream(event_timestamp DESC);

ALTER TABLE nyc_spatial_db.public.fact_subway_spatial_stream ADD COLUMN current_stop_id VARCHAR(50);

-- staging table used as a ghost table
CREATE TABLE IF NOT EXISTS staging_subway_stream (
    train_id VARCHAR(100),
    route_id VARCHAR(50),
    current_status VARCHAR(100),
    current_stop_id VARCHAR(50),
    event_timestamp TIMESTAMP,
    final_lat DOUBLE PRECISION,
    final_lon DOUBLE PRECISION
);

-- Trigger function
CREATE OR REPLACE FUNCTION process_subway_staging_rows()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.final_lat IS NOT NULL AND NEW.final_lon IS NOT NULL THEN
        INSERT INTO fact_subway_spatial_stream (
            train_id, route_id, current_status, current_stop_id, event_timestamp, coordinates
        ) VALUES (
            NEW.train_id,
            NEW.route_id,
            NEW.current_status,
            NEW.current_stop_id,
            NEW.event_timestamp,
            ST_SetSRID(ST_MakePoint(NEW.final_lon, NEW.final_lat), 4326)
        );
    END IF;
    RETURN NULL; 
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_process_subway_staging ON staging_subway_stream;
CREATE TRIGGER trg_process_subway_staging
BEFORE INSERT ON staging_subway_stream
FOR EACH ROW
EXECUTE FUNCTION process_subway_staging_rows();