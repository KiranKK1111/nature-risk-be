"""
Load TopoJSON files from the data directory into PostgreSQL.
Run this after executing 01_schema.sql and 02_seed_data.sql.

Usage:
    python database/03_load_topojson.py

Environment variables:
    DATABASE_URL - PostgreSQL connection string
                   Default: postgresql://postgres:postgres@localhost:5432/naturerisk
"""

import json
import os
import sys
import psycopg2
from psycopg2.extras import Json

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:kittu@localhost:5432/postgres"
)

SCHEMA = "nature_risk"

# TopoJSON reference layers to load
REFERENCE_LAYERS = [
    {
        "layer_key": "kba",
        "layer_name": "Key Biodiversity Areas (KBA)",
        "layer_group": "proximity",
        "description": "Key Biodiversity Areas 2024",
        "file_path": "data/proximity/kba.topojson",
    },
    {
        "layer_key": "iucn",
        "layer_name": "IUCN Protected Areas (WDPA)",
        "layer_group": "proximity",
        "description": "World Database of Protected Areas",
        "file_path": "data/proximity/iucn.topojson",
    },
    {
        "layer_key": "ramsar",
        "layer_name": "Ramsar Wetlands",
        "layer_group": "proximity",
        "description": "Ramsar Convention Wetlands",
        "file_path": "data/proximity/ramsar.topojson",
    },
    {
        "layer_key": "whs",
        "layer_name": "World Heritage Sites",
        "layer_group": "proximity",
        "description": "UNESCO World Heritage Sites",
        "file_path": "data/proximity/whs.topojson",
    },
    {
        "layer_key": "bws_annual",
        "layer_name": "Baseline Water Stress",
        "layer_group": "aquaduct",
        "description": "Aqueduct Baseline Water Stress Annual",
        "file_path": "data/aquaduct/bws_annual.topojson",
    },
]

# Client-specific TopoJSON layers
CLIENT_LAYERS = [
    {
        "client_name": "glencore_01",
        "layer_type": "client_assets",
        "layer_name": "Glencore Asset Locations",
        "file_path": "data/client/glencore.topojson",
    },
    {
        "client_name": "glencore_01",
        "layer_type": "sc_assets",
        "layer_name": "SC Asset Locations",
        "file_path": "data/client/sc_assets.topojson",
    },
]


def load_topojson_file(file_path: str) -> dict:
    """Load and parse a TopoJSON file."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    full_path = os.path.join(base_dir, file_path)

    if not os.path.exists(full_path):
        print(f"  WARNING: File not found: {full_path}")
        return None

    file_size = os.path.getsize(full_path)
    print(f"  Loading {full_path} ({file_size / 1024 / 1024:.1f} MB)...")

    with open(full_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    return data, file_size


def get_feature_count(topojson_data: dict) -> int:
    """Count features in a TopoJSON object."""
    count = 0
    if "objects" in topojson_data:
        for obj_name, obj in topojson_data["objects"].items():
            if "geometries" in obj:
                count += len(obj["geometries"])
    return count


def get_bbox(topojson_data: dict):
    """Extract bounding box from TopoJSON."""
    return topojson_data.get("bbox")


def main():
    print("Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute(f"SET search_path TO {SCHEMA}, public")

    # Load reference layers
    print("\n=== Loading Reference TopoJSON Layers ===")
    for layer in REFERENCE_LAYERS:
        print(f"\nProcessing: {layer['layer_name']}")
        result = load_topojson_file(layer["file_path"])
        if result is None:
            continue

        data, file_size = result
        feature_count = get_feature_count(data)
        bbox = get_bbox(data)

        cur.execute("""
            INSERT INTO topojson_layers (layer_key, layer_name, layer_group, description,
                                         topojson_data, feature_count, bbox, file_size_bytes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (layer_key) DO UPDATE SET
                topojson_data = EXCLUDED.topojson_data,
                feature_count = EXCLUDED.feature_count,
                bbox = EXCLUDED.bbox,
                file_size_bytes = EXCLUDED.file_size_bytes,
                updated_at = NOW()
        """, (
            layer["layer_key"], layer["layer_name"], layer["layer_group"],
            layer["description"], Json(data), feature_count,
            Json(bbox) if bbox else None, file_size
        ))
        print(f"  Loaded {feature_count} features ({file_size / 1024 / 1024:.1f} MB)")

    # Load client-specific layers
    print("\n=== Loading Client TopoJSON Layers ===")
    for layer in CLIENT_LAYERS:
        print(f"\nProcessing: {layer['layer_name']}")
        result = load_topojson_file(layer["file_path"])
        if result is None:
            continue

        data, file_size = result
        feature_count = get_feature_count(data)
        bbox = get_bbox(data)

        # Get client_id (lookup via client name)
        cur.execute("SELECT id FROM clients WHERE name = %s", (layer["client_name"],))
        row = cur.fetchone()
        if not row:
            print(f"  WARNING: Client '{layer['client_name']}' not found in database")
            continue
        client_id = row[0]

        cur.execute("""
            INSERT INTO client_topojson (client_id, layer_type, layer_name,
                                         topojson_data, feature_count, bbox, file_size_bytes)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (client_id, layer_type) DO UPDATE SET
                topojson_data = EXCLUDED.topojson_data,
                feature_count = EXCLUDED.feature_count,
                bbox = EXCLUDED.bbox,
                file_size_bytes = EXCLUDED.file_size_bytes,
                updated_at = NOW()
        """, (
            client_id, layer["layer_type"], layer["layer_name"],
            Json(data), feature_count, Json(bbox) if bbox else None, file_size
        ))
        print(f"  Loaded {feature_count} features ({file_size / 1024 / 1024:.1f} MB)")

    conn.commit()
    cur.close()
    conn.close()
    print("\n=== Done! All TopoJSON data loaded successfully. ===")


if __name__ == "__main__":
    main()
