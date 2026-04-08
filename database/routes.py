"""
FastAPI routes for serving NatureRisk data from PostgreSQL.

All data that was previously hardcoded in the frontend or fetched from CDN
is now served from the database via these endpoints.
"""

import json
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse, Response
from .db import get_db

router = APIRouter(prefix="/api")

# Legacy router — serves the same endpoints the frontend already calls,
# but backed by PostgreSQL instead of the CDN.
legacy_router = APIRouter()


# ============================================================================
# HIERARCHY ENDPOINTS (Sidebar dropdowns)
# ============================================================================

@router.get("/sectors")
async def get_sectors(db=Depends(get_db)):
    """Level 1: Get all sectors."""
    rows = await db.fetch("SELECT name, description FROM sectors ORDER BY name")
    return [dict(r) for r in rows]


@router.get("/sectors/{sector_name}/groups")
async def get_groups(sector_name: str, db=Depends(get_db)):
    """Level 2: Get groups for a sector."""
    rows = await db.fetch("""
        SELECT cg.name, cg.display_name
        FROM client_groups cg
        JOIN sectors s ON s.id = cg.sector_id
        WHERE s.name = $1 AND cg.is_active = TRUE
        ORDER BY cg.display_name
    """, sector_name)
    if not rows:
        raise HTTPException(status_code=404, detail=f"Sector '{sector_name}' not found or has no groups")
    return [dict(r) for r in rows]


@router.get("/groups/{group_name}/clients")
async def get_clients(group_name: str, db=Depends(get_db)):
    """Level 3: Get clients for a group."""
    rows = await db.fetch("""
        SELECT c.name, c.display_name
        FROM clients c
        JOIN client_groups cg ON cg.id = c.group_id
        WHERE cg.name = $1 AND c.is_active = TRUE
        ORDER BY c.display_name
    """, group_name)
    if not rows:
        raise HTTPException(status_code=404, detail=f"Group '{group_name}' not found or has no clients")
    return [dict(r) for r in rows]


# ============================================================================
# TOPOJSON ENDPOINTS (replaces CDN fetching)
# ============================================================================

@router.get("/topojson/{layer_key}")
async def get_topojson_layer(layer_key: str, db=Depends(get_db)):
    """Serve reference TopoJSON layers (KBA, WDPA, RAMSAR, WHS, Aquaduct) from DB."""
    row = await db.fetchrow(
        "SELECT topojson_data FROM topojson_layers WHERE layer_key = $1",
        layer_key
    )
    if not row:
        raise HTTPException(status_code=404, detail=f"TopoJSON layer '{layer_key}' not found")
    # Return raw JSON directly for best performance
    return Response(content=row["topojson_data"], media_type="application/json")


@router.get("/client-topojson/{client_name}/{layer_type}")
async def get_client_topojson(client_name: str, layer_type: str, db=Depends(get_db)):
    """Serve client-specific TopoJSON (client_assets, sc_assets) from DB."""
    row = await db.fetchrow("""
        SELECT ct.topojson_data
        FROM client_topojson ct
        JOIN clients c ON c.id = ct.client_id
        WHERE c.display_name = $1 AND ct.layer_type = $2
    """, client_name, layer_type)
    if not row:
        raise HTTPException(status_code=404, detail=f"Client TopoJSON not found for '{client_name}/{layer_type}'")
    return Response(content=row["topojson_data"], media_type="application/json")


# ============================================================================
# HEATMAP DATA (per client)
# ============================================================================

@router.get("/clients/{client_name}/heatmap")
async def get_heatmap_data(
    client_name: str,
    category: str = Query(None, description="PRESSURES or DEPENDENCIES"),
    db=Depends(get_db)
):
    """Get heatmap data for a client. Returns data in the frontend HeatmapDataItem format."""
    # Build query
    query = """
        SELECT
            h.category_type,
            h.entity_name,
            rc.name AS risk_category,
            rc.sort_order,
            h.value
        FROM heatmap_data h
        JOIN risk_categories rc ON rc.id = h.risk_category_id
        JOIN clients c ON c.id = h.client_id
        WHERE c.display_name = $1
    """
    params = [client_name]

    if category:
        query += " AND h.category_type = $2"
        params.append(category.upper())

    query += " ORDER BY h.category_type, h.entity_name, rc.sort_order"
    rows = await db.fetch(query, *params)

    if not rows:
        raise HTTPException(status_code=404, detail=f"No heatmap data for client '{client_name}'")

    # Transform to frontend HeatmapDataItem format
    result = {}
    for row in rows:
        cat = row["category_type"]
        if cat not in result:
            result[cat] = {
                "category": cat,
                "label": f"Client View: Nature Risk {cat.title()} Heatmap Across Top 20 Entities",
                "subLabel": "",
                "riskCategories": [],
                "companies": [],
                "values": {},
            }

        item = result[cat]
        entity = row["entity_name"]
        risk_cat = row["risk_category"]

        if risk_cat not in item["riskCategories"]:
            item["riskCategories"].append(risk_cat)
        if entity not in item["companies"]:
            item["companies"].append(entity)

        if entity not in item["values"]:
            item["values"][entity] = []
        item["values"][entity].append(row["value"])

    return list(result.values())


# ============================================================================
# RADAR DATA (per client)
# ============================================================================

@router.get("/clients/{client_name}/radar")
async def get_radar_data(
    client_name: str,
    exposure_type: str = Query(None, description="direct or indirect"),
    db=Depends(get_db)
):
    """Get radar data for a client. Returns data in the frontend RadarDataItem format."""
    query = """
        SELECT
            pb.name AS subject,
            pb.sort_order,
            r.category,
            r.exposure_type,
            r.pressure_value,
            r.dependency_value
        FROM radar_data r
        JOIN planetary_boundaries pb ON pb.id = r.planetary_boundary_id
        JOIN clients c ON c.id = r.client_id
        WHERE c.display_name = $1
    """
    params = [client_name]

    if exposure_type:
        query += " AND r.exposure_type = $2"
        params.append(exposure_type)

    query += " ORDER BY r.exposure_type, r.category, pb.sort_order"
    rows = await db.fetch(query, *params)

    if not rows:
        raise HTTPException(status_code=404, detail=f"No radar data for client '{client_name}'")

    # Transform to frontend RadarDataItem[] format
    result = []
    grouped = {}
    for row in rows:
        key = (row["exposure_type"], row["category"])
        if key not in grouped:
            grouped[key] = {
                "category": row["category"],
                "label": f"Nature-related {row['category'].title()}",
                "exposureType": row["exposure_type"],
                "data": [],
            }
        grouped[key]["data"].append({
            "subject": row["subject"],
            "Pressures": float(row["pressure_value"]),
            "Dependencies": float(row["dependency_value"]),
        })

    return list(grouped.values())


# ============================================================================
# GRID DATA (per client)
# ============================================================================

@router.get("/clients/{client_name}/grid")
async def get_grid_data(client_name: str, db=Depends(get_db)):
    """Get grid data for a client. Returns data in the frontend GridDataRow[] format."""
    rows = await db.fetch("""
        SELECT
            c.display_name AS company,
            g.category,
            pb.name AS variable,
            g.exposure_value AS exposure,
            g.exposure_level AS level
        FROM grid_data g
        JOIN planetary_boundaries pb ON pb.id = g.planetary_boundary_id
        JOIN clients c ON c.id = g.client_id
        WHERE c.display_name = $1
        ORDER BY g.exposure_level, g.category, pb.sort_order
    """, client_name)

    if not rows:
        raise HTTPException(status_code=404, detail=f"No grid data for client '{client_name}'")

    return [dict(r) for r in rows]


# ============================================================================
# NATURE THEMATICS / TABLE DATA (per sector)
# ============================================================================

@router.get("/sectors/{sector_name}/nature-thematics")
async def get_nature_thematics(sector_name: str, db=Depends(get_db)):
    """Get nature thematics table data for a sector. Returns frontend NatureTableRow[] format."""
    rows = await db.fetch("""
        SELECT
            nt.id,
            nt.category,
            nt.nature_thematic,
            nt.mdb_nature_finance,
            nt.icma_sustainable_bonds,
            nt.scb_green_sustainable,
            nt.sort_order
        FROM nature_thematics nt
        JOIN sectors s ON s.id = nt.sector_id
        WHERE s.name = $1
        ORDER BY nt.sort_order
    """, sector_name)

    if not rows:
        raise HTTPException(status_code=404, detail=f"No nature thematics for sector '{sector_name}'")

    result = []
    for row in rows:
        thematic_id = row["id"]

        # Fetch use of proceeds
        uop_rows = await db.fetch(
            "SELECT description FROM thematic_use_of_proceeds WHERE nature_thematic_id = $1 ORDER BY sort_order",
            thematic_id
        )
        # Fetch KPIs
        kpi_rows = await db.fetch(
            "SELECT description FROM thematic_kpis WHERE nature_thematic_id = $1 ORDER BY sort_order",
            thematic_id
        )

        result.append({
            "id": row["sort_order"],
            "category": row["category"],
            "natureThematic": row["nature_thematic"],
            "useOfProceeds": [r["description"] for r in uop_rows],
            "kpis": [r["description"] for r in kpi_rows],
            "mdbNatureFinance": row["mdb_nature_finance"],
            "icmaSustainableBonds": row["icma_sustainable_bonds"],
            "scbGreenSustainable": row["scb_green_sustainable"],
        })

    return result


# ============================================================================
# PLANETARY BOUNDARIES (reference data)
# ============================================================================

@router.get("/planetary-boundaries")
async def get_planetary_boundaries(db=Depends(get_db)):
    """Get planetary boundary indicators table."""
    rows = await db.fetch("""
        SELECT pb.name AS boundary, pb.sort_order
        FROM planetary_boundaries pb
        ORDER BY pb.sort_order
    """)

    result = []
    for row in rows:
        boundary_name = row["boundary"]

        pressures = await db.fetch("""
            SELECT indicator_name FROM planetary_boundary_indicators
            WHERE planetary_boundary_id = (SELECT id FROM planetary_boundaries WHERE name = $1)
            AND indicator_type = 'pressure'
            ORDER BY sort_order
        """, boundary_name)

        dependencies = await db.fetch("""
            SELECT indicator_name FROM planetary_boundary_indicators
            WHERE planetary_boundary_id = (SELECT id FROM planetary_boundaries WHERE name = $1)
            AND indicator_type = 'dependency'
            ORDER BY sort_order
        """, boundary_name)

        result.append({
            "boundary": boundary_name,
            "pressures": [r["indicator_name"] for r in pressures],
            "dependencies": [r["indicator_name"] for r in dependencies],
        })

    return result


# ============================================================================
# LEGACY ENDPOINTS (same URLs the frontend already calls, now served from DB)
# ============================================================================

# Maps the selectedValue query param to a topojson_layers.layer_key
_IBAT_MAPPING = {
    "KBAPOL2024STREAM": "kba",
    "WDPA00STREAM": "iucn",
    "RAMSARSTREAM": "ramsar",
    "WHS-STREAM": "whs",
}


@legacy_router.get("/get_scassets")
async def legacy_get_scassets(selectedValue: str = Query(...), db=Depends(get_db)):
    """Legacy endpoint — serves client/sc_assets TopoJSON from DB."""
    if selectedValue == "ClientAssetLocation":
        layer_type = "client_assets"
    elif selectedValue == "AssetLocation":
        layer_type = "sc_assets"
    else:
        raise HTTPException(status_code=400, detail="Invalid selectedValue")

    # For now, serve the first client that has this layer_type.
    # In future, pass client name from the frontend.
    row = await db.fetchrow(
        "SELECT topojson_data FROM client_topojson WHERE layer_type = $1 LIMIT 1",
        layer_type,
    )
    if not row:
        raise HTTPException(status_code=404, detail=f"No {layer_type} TopoJSON found in database")
    return Response(content=row["topojson_data"], media_type="application/json")


@legacy_router.get("/loadIbatData_kba_stream")
async def legacy_load_ibat(selectedValue: str = Query(...), db=Depends(get_db)):
    """Legacy endpoint — serves proximity TopoJSON layers from DB."""
    layer_key = _IBAT_MAPPING.get(selectedValue)
    if layer_key is None:
        raise HTTPException(status_code=400, detail="Unknown selectedValue")

    row = await db.fetchrow(
        "SELECT topojson_data FROM topojson_layers WHERE layer_key = $1",
        layer_key,
    )
    if not row:
        raise HTTPException(status_code=404, detail=f"TopoJSON layer '{layer_key}' not found in database")
    return Response(content=row["topojson_data"], media_type="application/json")


@legacy_router.get("/load_aquaduct_bassline_data")
async def legacy_load_aquaduct(db=Depends(get_db)):
    """Legacy endpoint — serves aquaduct baseline TopoJSON from DB."""
    row = await db.fetchrow(
        "SELECT topojson_data FROM topojson_layers WHERE layer_key = 'bws_annual'"
    )
    if not row:
        raise HTTPException(status_code=404, detail="Aquaduct TopoJSON not found in database")
    return Response(content=row["topojson_data"], media_type="application/json")


# ============================================================================
# RASTER ENDPOINTS (PNG tiles + manifests from DB)
# ============================================================================

@legacy_router.get("/getManifest")
async def get_manifest(path: str = Query(...), db=Depends(get_db)):
    """Serve raster manifest JSON from DB."""
    row = await db.fetchrow(
        "SELECT manifest_data FROM raster_manifests WHERE manifest_path = $1",
        path,
    )
    if not row:
        raise HTTPException(status_code=404, detail=f"Manifest '{path}' not found in database")
    # manifest_data is JSONB — asyncpg returns it as a Python list/dict already
    data = row["manifest_data"]
    if isinstance(data, str):
        data = json.loads(data)
    return JSONResponse(content=data)


@legacy_router.get("/getPng")
async def get_png(path: str = Query(...), db=Depends(get_db)):
    """Serve raster PNG tile from DB."""
    row = await db.fetchrow(
        "SELECT tile_data FROM raster_tiles WHERE tile_path = $1",
        path,
    )
    if not row:
        raise HTTPException(status_code=404, detail=f"PNG '{path}' not found in database")
    return Response(content=bytes(row["tile_data"]), media_type="image/png")
