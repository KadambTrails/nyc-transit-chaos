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