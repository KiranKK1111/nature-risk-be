-- ============================================================================
-- Raster Tiles Schema — stores PNG tiles + manifests in PostgreSQL
-- Run after 01_schema.sql
-- ============================================================================

SET search_path TO nature_risk, public;

-- ============================================================================
-- RASTER_MANIFESTS - Stores manifest JSON (list of tiles with bbox/crs)
-- One row per manifest file (e.g., gfc-png/local_manifest.json)
-- ============================================================================
CREATE TABLE IF NOT EXISTS raster_manifests (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    manifest_path   VARCHAR(500) NOT NULL UNIQUE,  -- e.g., 'gfc-png/local_manifest.json'
    manifest_data   JSONB NOT NULL,                -- the full manifest JSON array
    tile_count      INTEGER,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raster_manifests_path ON raster_manifests(manifest_path);

-- ============================================================================
-- RASTER_TILES - Stores individual PNG tile bytes
-- One row per PNG file, keyed by its relative path
-- ============================================================================
CREATE TABLE IF NOT EXISTS raster_tiles (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tile_path       VARCHAR(500) NOT NULL UNIQUE,  -- e.g., 'gfc-png/Hansen_GFC-2024-v1.12_lossyear_00N_000E.png'
    tile_data       BYTEA NOT NULL,                -- raw PNG bytes
    file_size_bytes BIGINT,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raster_tiles_path ON raster_tiles(tile_path);
