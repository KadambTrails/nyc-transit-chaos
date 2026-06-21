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

---------------------- gamified Trigger Alter -------------------------

CREATE TABLE IF NOT EXISTS game_multiplier_zones (
    zone_id SERIAL PRIMARY KEY,
    zone_name VARCHAR(100),
    multiplier INT,
    geom GEOMETRY(Polygon, 4326)
);

INSERT INTO nyc_spatial_db.public.game_multiplier_zones (zone_name, multiplier, geom)
VALUES (
    'Times Square Chaos Zone', 
    3,
    ST_GeomFromText('POLYGON((-73.992 40.750, -73.972 40.750, -73.972 40.765, -73.992 40.765, -73.992 40.750))', 4326)
) ON CONFLICT DO NOTHING;


select * from nyc_spatial_db.public.game_multiplier_zones 

ALTER TABLE fact_subway_spatial_stream ADD COLUMN IF NOT EXISTS current_score INT DEFAULT 0;
ALTER TABLE fact_subway_spatial_stream ADD COLUMN IF NOT EXISTS in_zone BOOLEAN DEFAULT FALSE;

CREATE OR REPLACE FUNCTION process_subway_game_logic()
RETURNS TRIGGER AS $$
DECLARE
    current_geom GEOMETRY;
    active_multiplier INT := 1;
    proximity_count INT := 0;
    final_calculated_points INT := 10;
    is_potato_holder BOOLEAN := FALSE;
BEGIN
    IF NEW.final_lat IS NOT NULL AND NEW.final_lon IS NOT NULL THEN
        current_geom := ST_SetSRID(ST_MakePoint(NEW.final_lon, NEW.final_lat), 4326);
        
        -- Game Rule Multipliers (Uses spatial index)
        SELECT COALESCE(MAX(multiplier), 1) INTO active_multiplier
        FROM game_multiplier_zones
        WHERE ST_Contains(geom, current_geom);
        
        -- Proximity checks (Optimized to use the spatial index directly without geography casting)
        -- 0.0027 degrees is approximately 300 meters in NYC latitude/longitude
        SELECT COUNT(*) INTO proximity_count
        FROM fact_subway_spatial_stream
        WHERE ST_DWithin(coordinates, current_geom, 0.0027)
          AND event_timestamp > NOW() - INTERVAL '2 minutes'
          AND train_id != NEW.train_id;

        final_calculated_points := (final_calculated_points + (proximity_count * 50)) * active_multiplier;

        IF proximity_count >= 2 THEN
            is_potato_holder := TRUE;
        END IF;

        -- Insert fresh data to production

        INSERT INTO fact_subway_spatial_stream (
            train_id, route_id, current_status, current_stop_id, event_timestamp, coordinates, current_score, in_zone
        ) VALUES (
            NEW.train_id, NEW.route_id, NEW.current_status, NEW.current_stop_id, NEW.event_timestamp, current_geom, final_calculated_points, is_potato_holder
        );

        
        -- Auto-pruning (Uses timestamp index)
        DELETE FROM fact_subway_spatial_stream 
        WHERE event_timestamp < NOW() - INTERVAL '10 minutes';
        
    END IF;
    
    RETURN NULL; 
END;
$$ LANGUAGE plpgsql;



DROP TRIGGER IF EXISTS trg_process_subway_staging ON staging_subway_stream;
CREATE TRIGGER trg_process_subway_staging
BEFORE INSERT ON staging_subway_stream
FOR EACH ROW
EXECUTE FUNCTION process_subway_game_logic();

--- A view for the live data rendering over the map
CREATE OR REPLACE VIEW live_subway_tracker AS
SELECT DISTINCT ON (train_id) 
    train_id, 
    route_id, 
    current_status, 
    current_stop_id, 
    event_timestamp, 
    coordinates, 
    current_score, 
    in_zone
FROM fact_subway_spatial_stream
ORDER BY train_id, event_timestamp DESC;