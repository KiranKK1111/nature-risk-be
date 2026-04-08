"""
Load raster PNG tiles and manifests from the data directory into PostgreSQL.
Run after 05_raster_schema.sql.

Usage:
    python database/06_load_raster.py

~1,573 PNGs, ~674 MB total. Takes a few minutes.
"""

import json
import os
import sys
import psycopg2

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:kittu@localhost:5432/postgres"
)
SCHEMA = "nature_risk"
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

# All manifest files to load
MANIFEST_FILES = [
    "gfc-png/local_manifest.json",
    "globio/lu_pngs/1992/tile_manifest.json",
    "globio/lu_pngs/1995/tile_manifest.json",
    "globio/lu_pngs/2000/tile_manifest.json",
    "globio/lu_pngs/2005/tile_manifest.json",
    "globio/lu_pngs/2010/tile_manifest.json",
    "globio/lu_pngs/2015/tile_manifest.json",
    "globio/lu_pngs/2020/tile_manifest.json",
    "globio/msa_pngs/SSP1/tile_manifest.json",
    "globio/msa_pngs/SSP3/tile_manifest.json",
    "globio/msa_pngs/SSP5/tile_manifest.json",
]

# Directories containing standalone PNG files (not in manifests)
STANDALONE_PNG_DIRS = [
    "dynqual/bod_png_3857",
    "dynqual/tds_png_3857",
    "edgar/co_png_3857",
    "edgar/nh3_png_3857",
    "edgar/nox_png_3857",
    "edgar/pm25_png_3857",
    "edgar/so2_png_3857",
    "edgar/tox_hg_png_3857",
]


def main():
    print("Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute(f"SET search_path TO {SCHEMA}, public")

    loaded_tiles = 0
    loaded_manifests = 0

    # ── Load manifests + their referenced tiles ──────────────────────────
    print("\n=== Loading Manifests + Tiles ===")
    for manifest_rel in MANIFEST_FILES:
        manifest_path = os.path.join(DATA_DIR, manifest_rel)
        if not os.path.exists(manifest_path):
            print(f"  SKIP (not found): {manifest_rel}")
            continue

        with open(manifest_path, "r") as f:
            manifest_data = json.load(f)

        # Insert manifest
        cur.execute("""
            INSERT INTO raster_manifests (manifest_path, manifest_data, tile_count)
            VALUES (%s, %s, %s)
            ON CONFLICT (manifest_path) DO UPDATE SET
                manifest_data = EXCLUDED.manifest_data,
                tile_count = EXCLUDED.tile_count
        """, (manifest_rel, json.dumps(manifest_data), len(manifest_data)))
        loaded_manifests += 1
        print(f"\n  Manifest: {manifest_rel} ({len(manifest_data)} tiles)")

        # Insert each tile referenced by the manifest
        for i, entry in enumerate(manifest_data):
            tile_rel = entry.get("url", "")
            if not tile_rel:
                continue
            tile_path = os.path.join(DATA_DIR, tile_rel)
            if not os.path.exists(tile_path):
                continue

            file_size = os.path.getsize(tile_path)
            with open(tile_path, "rb") as tf:
                tile_bytes = tf.read()

            cur.execute("""
                INSERT INTO raster_tiles (tile_path, tile_data, file_size_bytes)
                VALUES (%s, %s, %s)
                ON CONFLICT (tile_path) DO NOTHING
            """, (tile_rel, psycopg2.Binary(tile_bytes), file_size))
            loaded_tiles += 1

            if (i + 1) % 50 == 0 or (i + 1) == len(manifest_data):
                print(f"    {i+1}/{len(manifest_data)} tiles loaded", end="\r")
                conn.commit()

        print()
        conn.commit()

    # ── Load standalone PNGs (edgar, dynqual) ────────────────────────────
    print("\n=== Loading Standalone PNGs ===")
    for dir_rel in STANDALONE_PNG_DIRS:
        dir_path = os.path.join(DATA_DIR, dir_rel)
        if not os.path.isdir(dir_path):
            print(f"  SKIP (not found): {dir_rel}")
            continue

        for fname in os.listdir(dir_path):
            if not fname.endswith(".png"):
                continue
            tile_rel = f"{dir_rel}/{fname}"
            tile_path = os.path.join(dir_path, fname)
            file_size = os.path.getsize(tile_path)

            with open(tile_path, "rb") as tf:
                tile_bytes = tf.read()

            cur.execute("""
                INSERT INTO raster_tiles (tile_path, tile_data, file_size_bytes)
                VALUES (%s, %s, %s)
                ON CONFLICT (tile_path) DO NOTHING
            """, (tile_rel, psycopg2.Binary(tile_bytes), file_size))
            loaded_tiles += 1
            print(f"  Loaded: {tile_rel} ({file_size / 1024:.0f} KB)")

    conn.commit()
    cur.close()
    conn.close()

    print(f"\n=== Done! ===")
    print(f"  Manifests loaded: {loaded_manifests}")
    print(f"  Tiles loaded:     {loaded_tiles}")


if __name__ == "__main__":
    main()
