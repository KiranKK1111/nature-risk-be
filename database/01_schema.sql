-- ============================================================================
-- NatureRisk Database Schema (PostgreSQL)
-- ============================================================================
-- ER Model Architecture (3-level hierarchy):
--
--   sectors ──< client_groups ──< clients ──< client_assets
--     │              │                │              │
--     │              │                │              └──< asset_environmental_metrics
--     │              │                │
--     │              │                ├──< heatmap_data ──> risk_categories
--     │              │                ├──< radar_data ──> planetary_boundaries
--     │              │                ├──< grid_data ──> planetary_boundaries
--     │              │                └──< client_topojson
--     │              │
--     │              │   Example: Sector="Metals & Minings"
--     │              │            Group="Glencore"
--     │              │            Client="Glencore 01" → loads topojson + all data
--     │              │
--   sectors ──< nature_thematics (fetched PER SECTOR)
--                    │
--                    └──< thematic_use_of_proceeds
--                    └──< thematic_kpis
--
--   planetary_boundaries ──< planetary_boundary_indicators
--
--   topojson_layers (standalone reference geospatial data)
--
-- ============================================================================

-- Create the nature_risk schema (all tables live here, not in public)
CREATE SCHEMA IF NOT EXISTS nature_risk;
SET search_path TO nature_risk, public;

-- Enable UUID extension (in public schema, shared by all)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp" SCHEMA public;

-- ============================================================================
-- 1. SECTORS - Industry sectors (e.g., Mining, Energy, Agriculture)
-- ============================================================================
CREATE TABLE sectors (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name            VARCHAR(255) NOT NULL UNIQUE,
    description     TEXT,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_sectors_name ON sectors(name);

-- ============================================================================
-- 2. CLIENT_GROUPS - Groups within a sector (e.g., "Glencore" under "Metals & Minings")
--    Hierarchy: Sector → Group → Client
-- ============================================================================
CREATE TABLE client_groups (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sector_id       UUID NOT NULL REFERENCES sectors(id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    display_name    VARCHAR(255) NOT NULL,
    description     TEXT,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(sector_id, name)
);

CREATE INDEX idx_client_groups_sector_id ON client_groups(sector_id);
CREATE INDEX idx_client_groups_name ON client_groups(name);

-- ============================================================================
-- 3. CLIENTS - Individual client entities within a group
--    (e.g., "Glencore 01" under group "Glencore")
--    Selecting a client loads its topojson + all associated data
-- ============================================================================
CREATE TABLE clients (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    group_id        UUID NOT NULL REFERENCES client_groups(id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    display_name    VARCHAR(255) NOT NULL,
    description     TEXT,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(group_id, name)
);

CREATE INDEX idx_clients_group_id ON clients(group_id);
CREATE INDEX idx_clients_name ON clients(name);

-- ============================================================================
-- 4. CLIENT_ASSETS - Asset locations from client topojson data
--    (e.g., Glencore mines, smelters, plants)
-- ============================================================================
CREATE TABLE client_assets (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    client_id           UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    asset_name          VARCHAR(500) NOT NULL,
    parent_name         VARCHAR(500),
    country             VARCHAR(255),
    asset_type          VARCHAR(255),
    asset_activity      VARCHAR(255),
    latitude            DOUBLE PRECISION NOT NULL,
    longitude           DOUBLE PRECISION NOT NULL,
    minimum_distance    DOUBLE PRECISION,
    sensitive_area      VARCHAR(500),
    -- GeoJSON point geometry stored as JSONB for flexible querying
    geojson             JSONB,
    -- Raw properties from the original topojson feature
    properties          JSONB,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_client_assets_client_id ON client_assets(client_id);
CREATE INDEX idx_client_assets_coords ON client_assets(latitude, longitude);
CREATE INDEX idx_client_assets_parent ON client_assets(parent_name);
CREATE INDEX idx_client_assets_country ON client_assets(country);

-- ============================================================================
-- 4. ASSET_ENVIRONMENTAL_METRICS - Precomputed environmental metrics per asset
--    (deforestation, MSA, land use, emissions, proximity to sensitive areas)
-- ============================================================================
CREATE TABLE asset_environmental_metrics (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    asset_id            UUID NOT NULL REFERENCES client_assets(id) ON DELETE CASCADE,
    -- Deforestation metrics (tree cover loss at various radii)
    def_100             DOUBLE PRECISION,
    def_1000            DOUBLE PRECISION,
    def_5000            DOUBLE PRECISION,
    def_10000           DOUBLE PRECISION,
    def_25000           DOUBLE PRECISION,
    def_50000           DOUBLE PRECISION,
    -- Mean Species Abundance
    msa_100             DOUBLE PRECISION,
    -- Land use category
    lu_cat_2020         INTEGER,
    -- Water quality
    bod                 DOUBLE PRECISION,
    tds                 DOUBLE PRECISION,
    -- Air emissions (2022)
    pm25_2022           DOUBLE PRECISION,
    co_2022             DOUBLE PRECISION,
    nh3_2022            DOUBLE PRECISION,
    so2_2022            DOUBLE PRECISION,
    nox_2022            DOUBLE PRECISION,
    hg_2022             DOUBLE PRECISION,
    -- IUCN proximity categories
    iucn_ia             DOUBLE PRECISION,
    iucn_ib             DOUBLE PRECISION,
    iucn_ii             DOUBLE PRECISION,
    iucn_iii            DOUBLE PRECISION,
    -- Other proximity metrics
    aze                 DOUBLE PRECISION,
    ramsar              DOUBLE PRECISION,
    world_heritage_site DOUBLE PRECISION,
    non_aze             DOUBLE PRECISION,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(asset_id)
);

CREATE INDEX idx_asset_env_metrics_asset_id ON asset_environmental_metrics(asset_id);

-- ============================================================================
-- 5. PLANETARY_BOUNDARIES - Reference table for the 8 planetary boundaries
-- ============================================================================
CREATE TABLE planetary_boundaries (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name            VARCHAR(255) NOT NULL UNIQUE,
    description     TEXT,
    sort_order      INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ============================================================================
-- 6. PLANETARY_BOUNDARY_INDICATORS - Pressure and dependency indicators
--    per planetary boundary (reference/lookup data)
-- ============================================================================
CREATE TABLE planetary_boundary_indicators (
    id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    planetary_boundary_id   UUID NOT NULL REFERENCES planetary_boundaries(id) ON DELETE CASCADE,
    indicator_type          VARCHAR(20) NOT NULL CHECK (indicator_type IN ('pressure', 'dependency')),
    indicator_name          VARCHAR(500) NOT NULL,
    sort_order              INTEGER NOT NULL DEFAULT 0,
    created_at              TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_pb_indicators_boundary ON planetary_boundary_indicators(planetary_boundary_id);
CREATE INDEX idx_pb_indicators_type ON planetary_boundary_indicators(indicator_type);

-- ============================================================================
-- 7. RISK_CATEGORIES - Column headers for heatmap views
--    (shared across clients, separated by category type)
-- ============================================================================
CREATE TABLE risk_categories (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    category_type   VARCHAR(20) NOT NULL CHECK (category_type IN ('PRESSURES', 'DEPENDENCIES')),
    name            VARCHAR(500) NOT NULL,
    sort_order      INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(category_type, name)
);

CREATE INDEX idx_risk_categories_type ON risk_categories(category_type);

-- ============================================================================
-- 8. HEATMAP_DATA - Risk heatmap values per client, per entity, per category
--    Fetched PER CLIENT
-- ============================================================================
CREATE TABLE heatmap_data (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    client_id           UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    category_type       VARCHAR(20) NOT NULL CHECK (category_type IN ('PRESSURES', 'DEPENDENCIES')),
    entity_name         VARCHAR(500) NOT NULL,  -- Company/asset entity (row)
    risk_category_id    UUID NOT NULL REFERENCES risk_categories(id) ON DELETE CASCADE,
    value               DOUBLE PRECISION NOT NULL CHECK (value >= 0 AND value <= 1),
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(client_id, category_type, entity_name, risk_category_id)
);

CREATE INDEX idx_heatmap_client ON heatmap_data(client_id);
CREATE INDEX idx_heatmap_category ON heatmap_data(category_type);
CREATE INDEX idx_heatmap_entity ON heatmap_data(entity_name);

-- ============================================================================
-- 9. RADAR_DATA - Radar chart data per client, per planetary boundary
--    Fetched PER CLIENT
-- ============================================================================
CREATE TABLE radar_data (
    id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    client_id               UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    planetary_boundary_id   UUID NOT NULL REFERENCES planetary_boundaries(id) ON DELETE CASCADE,
    exposure_type           VARCHAR(20) NOT NULL CHECK (exposure_type IN ('direct', 'indirect')),
    category                VARCHAR(20) NOT NULL CHECK (category IN ('PRESSURES', 'DEPENDENCIES', 'OVERALL')),
    pressure_value          DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (pressure_value >= 0 AND pressure_value <= 100),
    dependency_value        DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (dependency_value >= 0 AND dependency_value <= 100),
    created_at              TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at              TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(client_id, planetary_boundary_id, exposure_type, category)
);

CREATE INDEX idx_radar_client ON radar_data(client_id);
CREATE INDEX idx_radar_boundary ON radar_data(planetary_boundary_id);
CREATE INDEX idx_radar_exposure ON radar_data(exposure_type);

-- ============================================================================
-- 10. GRID_DATA - Exposure grid data per client, per planetary boundary
--     Fetched PER CLIENT
-- ============================================================================
CREATE TABLE grid_data (
    id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    client_id               UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    planetary_boundary_id   UUID NOT NULL REFERENCES planetary_boundaries(id) ON DELETE CASCADE,
    category                VARCHAR(20) NOT NULL CHECK (category IN ('Dependency', 'Pressure')),
    exposure_level          VARCHAR(20) NOT NULL CHECK (exposure_level IN ('direct', 'indirect')),
    exposure_value          DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (exposure_value >= 0 AND exposure_value <= 100),
    created_at              TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at              TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(client_id, planetary_boundary_id, category, exposure_level)
);

CREATE INDEX idx_grid_client ON grid_data(client_id);
CREATE INDEX idx_grid_boundary ON grid_data(planetary_boundary_id);
CREATE INDEX idx_grid_level ON grid_data(exposure_level);

-- ============================================================================
-- 11. NATURE_THEMATICS - Nature financing thematic table data
--     Fetched PER SECTOR
-- ============================================================================
CREATE TABLE nature_thematics (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sector_id       UUID NOT NULL REFERENCES sectors(id) ON DELETE CASCADE,
    category        TEXT NOT NULL,
    nature_thematic VARCHAR(500) NOT NULL,
    mdb_nature_finance      BOOLEAN NOT NULL DEFAULT FALSE,
    icma_sustainable_bonds  BOOLEAN NOT NULL DEFAULT FALSE,
    scb_green_sustainable   BOOLEAN NOT NULL DEFAULT FALSE,
    sort_order      INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_nature_thematics_sector ON nature_thematics(sector_id);

-- ============================================================================
-- 12. THEMATIC_USE_OF_PROCEEDS - Use of proceeds per nature thematic
-- ============================================================================
CREATE TABLE thematic_use_of_proceeds (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    nature_thematic_id  UUID NOT NULL REFERENCES nature_thematics(id) ON DELETE CASCADE,
    description         TEXT NOT NULL,
    sort_order          INTEGER NOT NULL DEFAULT 0,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_thematic_uop_thematic ON thematic_use_of_proceeds(nature_thematic_id);

-- ============================================================================
-- 13. THEMATIC_KPIS - KPIs per nature thematic
-- ============================================================================
CREATE TABLE thematic_kpis (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    nature_thematic_id  UUID NOT NULL REFERENCES nature_thematics(id) ON DELETE CASCADE,
    description         TEXT NOT NULL,
    sort_order          INTEGER NOT NULL DEFAULT 0,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_thematic_kpis_thematic ON thematic_kpis(nature_thematic_id);

-- ============================================================================
-- 14. TOPOJSON_LAYERS - Store reference/proximity geospatial layer data
--     (KBA, WDPA, RAMSAR, WHS, Aquaduct, etc.)
-- ============================================================================
CREATE TABLE topojson_layers (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    layer_key       VARCHAR(100) NOT NULL UNIQUE,  -- e.g., 'kba', 'iucn', 'ramsar', 'whs', 'bws_annual'
    layer_name      VARCHAR(255) NOT NULL,
    layer_group     VARCHAR(100) NOT NULL,          -- e.g., 'proximity', 'aquaduct', 'client'
    description     TEXT,
    -- Store the full TopoJSON as JSONB for direct serving
    topojson_data   JSONB NOT NULL,
    -- Metadata
    feature_count   INTEGER,
    bbox            JSONB,  -- Bounding box [west, south, east, north]
    file_size_bytes BIGINT,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_topojson_layers_key ON topojson_layers(layer_key);
CREATE INDEX idx_topojson_layers_group ON topojson_layers(layer_group);

-- ============================================================================
-- 15. CLIENT_TOPOJSON - Store client-specific topojson data
--     (e.g., glencore.topojson, sc_assets.topojson)
-- ============================================================================
CREATE TABLE client_topojson (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    client_id       UUID REFERENCES clients(id) ON DELETE CASCADE,
    layer_type      VARCHAR(50) NOT NULL CHECK (layer_type IN ('client_assets', 'sc_assets')),
    layer_name      VARCHAR(255) NOT NULL,
    topojson_data   JSONB NOT NULL,
    feature_count   INTEGER,
    bbox            JSONB,
    file_size_bytes BIGINT,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(client_id, layer_type)
);

CREATE INDEX idx_client_topojson_client ON client_topojson(client_id);
CREATE INDEX idx_client_topojson_type ON client_topojson(layer_type);

-- ============================================================================
-- TRIGGER: Auto-update updated_at timestamp
-- ============================================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply trigger to all tables with updated_at
DO $$
DECLARE
    tbl TEXT;
BEGIN
    FOR tbl IN
        SELECT unnest(ARRAY[
            'sectors', 'client_groups', 'clients', 'client_assets',
            'asset_environmental_metrics', 'heatmap_data',
            'radar_data', 'grid_data', 'nature_thematics',
            'topojson_layers', 'client_topojson'
        ])
    LOOP
        EXECUTE format(
            'CREATE TRIGGER trigger_update_%s_updated_at
             BEFORE UPDATE ON %I
             FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()',
            tbl, tbl
        );
    END LOOP;
END;
$$;

-- ============================================================================
-- VIEWS: Convenience views for common queries
-- ============================================================================

-- View: Full heatmap data with category names (per client)
CREATE OR REPLACE VIEW v_heatmap_full AS
SELECT
    h.id,
    c.name AS client_name,
    c.display_name AS client_display_name,
    h.category_type,
    h.entity_name,
    rc.name AS risk_category_name,
    rc.sort_order AS risk_category_order,
    h.value
FROM heatmap_data h
JOIN clients c ON c.id = h.client_id
JOIN risk_categories rc ON rc.id = h.risk_category_id
ORDER BY c.name, h.category_type, h.entity_name, rc.sort_order;

-- View: Full radar data with boundary names (per client)
CREATE OR REPLACE VIEW v_radar_full AS
SELECT
    r.id,
    c.name AS client_name,
    c.display_name AS client_display_name,
    pb.name AS planetary_boundary,
    pb.sort_order AS boundary_order,
    r.exposure_type,
    r.category,
    r.pressure_value,
    r.dependency_value
FROM radar_data r
JOIN clients c ON c.id = r.client_id
JOIN planetary_boundaries pb ON pb.id = r.planetary_boundary_id
ORDER BY c.name, r.exposure_type, r.category, pb.sort_order;

-- View: Full grid data with boundary names (per client)
CREATE OR REPLACE VIEW v_grid_full AS
SELECT
    g.id,
    c.name AS client_name,
    c.display_name AS client_display_name,
    pb.name AS planetary_boundary,
    pb.sort_order AS boundary_order,
    g.category,
    g.exposure_level,
    g.exposure_value
FROM grid_data g
JOIN clients c ON c.id = g.client_id
JOIN planetary_boundaries pb ON pb.id = g.planetary_boundary_id
ORDER BY c.name, g.exposure_level, g.category, pb.sort_order;

-- View: Full nature thematics with use of proceeds and KPIs (per sector)
CREATE OR REPLACE VIEW v_nature_thematics_full AS
SELECT
    nt.id AS thematic_id,
    s.name AS sector_name,
    nt.category,
    nt.nature_thematic,
    nt.mdb_nature_finance,
    nt.icma_sustainable_bonds,
    nt.scb_green_sustainable,
    nt.sort_order
FROM nature_thematics nt
JOIN sectors s ON s.id = nt.sector_id
ORDER BY s.name, nt.sort_order;

-- View: Full hierarchy - sectors → groups → clients
CREATE OR REPLACE VIEW v_hierarchy AS
SELECT
    s.id AS sector_id,
    s.name AS sector_name,
    cg.id AS group_id,
    cg.name AS group_name,
    cg.display_name AS group_display_name,
    c.id AS client_id,
    c.name AS client_name,
    c.display_name AS client_display_name,
    c.is_active
FROM sectors s
JOIN client_groups cg ON cg.sector_id = s.id
JOIN clients c ON c.group_id = cg.id
ORDER BY s.name, cg.display_name, c.display_name;

-- View: Client assets with environmental metrics
CREATE OR REPLACE VIEW v_client_assets_full AS
SELECT
    ca.id AS asset_id,
    c.name AS client_name,
    c.display_name AS client_display_name,
    cg.display_name AS group_name,
    s.name AS sector_name,
    ca.asset_name,
    ca.parent_name,
    ca.country,
    ca.asset_type,
    ca.asset_activity,
    ca.latitude,
    ca.longitude,
    ca.minimum_distance,
    ca.sensitive_area,
    aem.def_100, aem.def_1000, aem.def_5000, aem.def_10000, aem.def_25000, aem.def_50000,
    aem.msa_100, aem.lu_cat_2020,
    aem.bod, aem.tds,
    aem.pm25_2022, aem.co_2022, aem.nh3_2022, aem.so2_2022, aem.nox_2022, aem.hg_2022,
    aem.iucn_ia, aem.iucn_ib, aem.iucn_ii, aem.iucn_iii,
    aem.aze, aem.ramsar, aem.world_heritage_site, aem.non_aze
FROM client_assets ca
JOIN clients c ON c.id = ca.client_id
JOIN client_groups cg ON cg.id = c.group_id
JOIN sectors s ON s.id = cg.sector_id
LEFT JOIN asset_environmental_metrics aem ON aem.asset_id = ca.id
ORDER BY c.name, ca.asset_name;
