-- ============================================================================
-- Sample Queries for NatureRisk API Endpoints
-- ============================================================================
-- These queries demonstrate how to fetch data for each API endpoint.
-- Use these as reference when building the FastAPI database integration.
-- ============================================================================

SET search_path TO nature_risk, public;

-- ============================================================================
-- 1. GET HEATMAP DATA (per client)
-- Endpoint: GET /api/heatmap/{client_name}?category=PRESSURES
-- ============================================================================
SELECT
    h.category_type,
    h.entity_name,
    rc.name AS risk_category,
    rc.sort_order,
    h.value
FROM heatmap_data h
JOIN risk_categories rc ON rc.id = h.risk_category_id
JOIN clients c ON c.id = h.client_id
WHERE c.name = 'glencore_01'
  AND h.category_type = 'PRESSURES'
ORDER BY h.entity_name, rc.sort_order;

-- Pivot format (matching frontend HeatmapDataItem structure):
-- Returns entity_name as rows, risk_categories as columns
SELECT
    h.entity_name,
    jsonb_object_agg(rc.name, h.value ORDER BY rc.sort_order) AS values
FROM heatmap_data h
JOIN risk_categories rc ON rc.id = h.risk_category_id
JOIN clients c ON c.id = h.client_id
WHERE c.name = 'glencore_01'
  AND h.category_type = 'PRESSURES'
GROUP BY h.entity_name
ORDER BY h.entity_name;

-- ============================================================================
-- 2. GET RADAR DATA (per client)
-- Endpoint: GET /api/radar/{client_name}?exposure_type=direct
-- ============================================================================
SELECT
    pb.name AS subject,
    r.category,
    r.exposure_type,
    r.pressure_value AS "Pressures",
    r.dependency_value AS "Dependencies"
FROM radar_data r
JOIN planetary_boundaries pb ON pb.id = r.planetary_boundary_id
JOIN clients c ON c.id = r.client_id
WHERE c.name = 'glencore_01'
  AND r.exposure_type = 'direct'
ORDER BY r.category, pb.sort_order;

-- ============================================================================
-- 3. GET GRID DATA (per client)
-- Endpoint: GET /api/grid/{client_name}
-- ============================================================================
SELECT
    c.display_name AS company,
    g.category,
    pb.name AS variable,
    g.exposure_value AS exposure,
    g.exposure_level AS level
FROM grid_data g
JOIN planetary_boundaries pb ON pb.id = g.planetary_boundary_id
JOIN clients c ON c.id = g.client_id
WHERE c.name = 'glencore_01'
ORDER BY g.exposure_level, g.category, pb.sort_order;

-- ============================================================================
-- 4. GET TABLE DATA / NATURE THEMATICS (per sector)
-- Endpoint: GET /api/nature-thematics/{sector_name}
-- ============================================================================
SELECT
    nt.id,
    nt.category,
    nt.nature_thematic,
    nt.mdb_nature_finance,
    nt.icma_sustainable_bonds,
    nt.scb_green_sustainable,
    (
        SELECT json_agg(uop.description ORDER BY uop.sort_order)
        FROM thematic_use_of_proceeds uop
        WHERE uop.nature_thematic_id = nt.id
    ) AS use_of_proceeds,
    (
        SELECT json_agg(kpi.description ORDER BY kpi.sort_order)
        FROM thematic_kpis kpi
        WHERE kpi.nature_thematic_id = nt.id
    ) AS kpis
FROM nature_thematics nt
JOIN sectors s ON s.id = nt.sector_id
WHERE s.name = 'Metals & Minings'
ORDER BY nt.sort_order;

-- ============================================================================
-- 5. GET TOPOJSON LAYER (reference geospatial data)
-- Endpoint: GET /api/topojson/{layer_key}
-- ============================================================================
SELECT topojson_data
FROM topojson_layers
WHERE layer_key = 'kba';

-- ============================================================================
-- 6. GET CLIENT TOPOJSON (client assets)
-- Endpoint: GET /api/client-topojson/{client_name}/{layer_type}
-- ============================================================================
SELECT ct.topojson_data
FROM client_topojson ct
JOIN clients c ON c.id = ct.client_id
WHERE c.name = 'glencore_01'
  AND ct.layer_type = 'client_assets';

-- ============================================================================
-- 7. GET CLIENT ASSETS with Environmental Metrics
-- Endpoint: GET /api/client-assets/{client_name}
-- ============================================================================
SELECT
    ca.asset_name,
    ca.parent_name,
    ca.country,
    ca.asset_type,
    ca.asset_activity,
    ca.latitude,
    ca.longitude,
    ca.minimum_distance,
    aem.def_100, aem.def_1000, aem.msa_100,
    aem.pm25_2022, aem.co_2022, aem.bod, aem.tds
FROM client_assets ca
LEFT JOIN asset_environmental_metrics aem ON aem.asset_id = ca.id
JOIN clients c ON c.id = ca.client_id
WHERE c.name = 'glencore_01'
ORDER BY ca.asset_name;

-- ============================================================================
-- 8. GET PLANETARY BOUNDARY INDICATORS
-- Endpoint: GET /api/planetary-boundaries
-- ============================================================================
SELECT
    pb.name AS boundary,
    (
        SELECT json_agg(pbi.indicator_name ORDER BY pbi.sort_order)
        FROM planetary_boundary_indicators pbi
        WHERE pbi.planetary_boundary_id = pb.id
          AND pbi.indicator_type = 'pressure'
    ) AS pressures,
    (
        SELECT json_agg(pbi.indicator_name ORDER BY pbi.sort_order)
        FROM planetary_boundary_indicators pbi
        WHERE pbi.planetary_boundary_id = pb.id
          AND pbi.indicator_type = 'dependency'
    ) AS dependencies
FROM planetary_boundaries pb
ORDER BY pb.sort_order;

-- ============================================================================
-- 9. GET GROUPS FOR A SECTOR (dropdown level 2)
-- Endpoint: GET /api/sectors/{sector_name}/groups
-- ============================================================================
SELECT
    cg.name,
    cg.display_name,
    cg.is_active
FROM client_groups cg
JOIN sectors s ON s.id = cg.sector_id
WHERE s.name = 'Metals & Minings'
  AND cg.is_active = TRUE
ORDER BY cg.display_name;

-- ============================================================================
-- 10. GET CLIENTS FOR A GROUP (dropdown level 3)
-- Endpoint: GET /api/groups/{group_name}/clients
-- ============================================================================
SELECT
    c.name,
    c.display_name,
    c.is_active
FROM clients c
JOIN client_groups cg ON cg.id = c.group_id
WHERE cg.name = 'glencore'
  AND c.is_active = TRUE
ORDER BY c.display_name;

-- ============================================================================
-- 11. GET FULL HIERARCHY (for sidebar tree)
-- Endpoint: GET /api/hierarchy
-- ============================================================================
SELECT
    s.name AS sector_name,
    cg.name AS group_name,
    cg.display_name AS group_display_name,
    c.name AS client_name,
    c.display_name AS client_display_name
FROM sectors s
JOIN client_groups cg ON cg.sector_id = s.id
JOIN clients c ON c.group_id = cg.id
WHERE c.is_active = TRUE
ORDER BY s.name, cg.display_name, c.display_name;

-- ============================================================================
-- 12. GET SECTOR LIST
-- Endpoint: GET /api/sectors
-- ============================================================================
SELECT name, description FROM sectors ORDER BY name;
