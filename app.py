from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import uvicorn
import json
from fastapi.middleware.cors import CORSMiddleware
import logging
import psutil
from config import settings

# Configure logging
logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL, logging.INFO))
logger = logging.getLogger(__name__)

# Create a FastAPI app
app = FastAPI(root_path=settings.ROOT_PATH)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Database — ALL data served from PostgreSQL ───────────────────────────────
from database.db import init_db, close_db
from database.routes import router as db_router, legacy_router
app.include_router(db_router)
app.include_router(legacy_router)

@app.on_event("startup")
async def startup_db():
    await init_db()
    logger.info("Database connection pool initialized. All data served from PostgreSQL.")

@app.on_event("shutdown")
async def shutdown_db():
    await close_db()
    logger.info("Database connection pool closed.")


# ── CPU monitoring endpoints ─────────────────────────────────────────────────

@app.get("/cpu-info")
async def cpu_info():
    total_cores = psutil.cpu_count(logical=True)
    physical_cores = psutil.cpu_count(logical=False)
    cpu_percent = psutil.cpu_percent(interval=1)
    mem = psutil.virtual_memory()
    return JSONResponse(content={
        "total_cores": total_cores,
        "physical_cores": physical_cores,
        "cpu_percent": cpu_percent,
        "ram": {"total": mem.total, "available": mem.available, "used": mem.used, "percent": mem.percent},
    })


@app.get("/cpu-stream")
async def cpu_stream():
    """Returns a single JSON snapshot of CPU/RAM usage.
    The frontend polls this every 2 seconds via streamCpuUsage()."""
    total_cores = psutil.cpu_count(logical=True)
    physical_cores = psutil.cpu_count(logical=False)
    cpu_percent = psutil.cpu_percent(interval=0)
    mem = psutil.virtual_memory()
    return JSONResponse(content={
        "total_cores": total_cores,
        "physical_cores": physical_cores,
        "cpu_percent": cpu_percent,
        "ram": {"total": mem.total, "available": mem.available, "used": mem.used, "percent": mem.percent},
    })


# ── Raster pixel sampling for breach detection (reads PNGs from DB) ─────────
import math
from PIL import Image
import io
from typing import Dict
import psycopg2

# Sync DB connection for raster sampling (PIL is sync, can't use asyncpg)
_sync_conn = None


def _get_sync_conn():
    global _sync_conn
    if _sync_conn is None or _sync_conn.closed:
        _sync_conn = psycopg2.connect(settings.DATABASE_URL)
        _sync_conn.autocommit = True
        cur = _sync_conn.cursor()
        cur.execute(f"SET search_path TO {settings.DB_SCHEMA}, public")
        cur.close()
    return _sync_conn


# In-memory caches for raster sampling (avoid repeated DB reads)
_raster_image_cache: Dict[str, Image.Image] = {}
_manifest_cache: Dict[str, list] = {}


def _load_tile_from_db(rel_path: str) -> bytes:
    """Load a PNG tile from the raster_tiles table."""
    conn = _get_sync_conn()
    cur = conn.cursor()
    cur.execute("SELECT tile_data FROM raster_tiles WHERE tile_path = %s", (rel_path,))
    row = cur.fetchone()
    cur.close()
    if not row:
        raise FileNotFoundError(f"Tile '{rel_path}' not found in database")
    return bytes(row[0])


def _load_manifest_from_db(rel_path: str) -> list:
    """Load a manifest from the raster_manifests table."""
    if rel_path in _manifest_cache:
        return _manifest_cache[rel_path]
    conn = _get_sync_conn()
    cur = conn.cursor()
    cur.execute("SELECT manifest_data FROM raster_manifests WHERE manifest_path = %s", (rel_path,))
    row = cur.fetchone()
    cur.close()
    if not row:
        raise FileNotFoundError(f"Manifest '{rel_path}' not found in database")
    data = row[0] if isinstance(row[0], list) else json.loads(row[0])
    _manifest_cache[rel_path] = data
    return data


def _open_cached_image(rel_path: str) -> Image.Image:
    if rel_path not in _raster_image_cache:
        raw = _load_tile_from_db(rel_path)
        _raster_image_cache[rel_path] = Image.open(io.BytesIO(raw)).convert("RGBA")
    return _raster_image_cache[rel_path]


def _latlng_to_3857(lat: float, lng: float):
    R = 6378137.0
    x = math.radians(lng) * R
    y = math.log(math.tan(math.pi / 4 + math.radians(lat) / 2)) * R
    return x, y


_MANIFEST_LAYERS = {"Tree Cover Loss": "gfc-png/local_manifest.json"}
_GLOBIO_LAYERS = {
    "Land Use": "globio/lu_pngs/2020/tile_manifest.json",
    "MSA":      "globio/msa_pngs/SSP5/tile_manifest.json",
}
_GLOBAL_EXTENT = (-85.04, 85.04, -179.99, 179.99)
_SINGLE_PNG_LAYERS = {
    "BOD":   "dynqual/bod_png_3857/BODload_annualAvg_1980_2019_2019_3857.png",
    "TDS":   "dynqual/tds_png_3857/TDSload_annualAvg_1980_2019_2019_3857.png",
    "PM2.5": "edgar/pm25_png_3857/v8.1_FT2022_AP_PM2.5_2022_TOTALS_emi_3857.png",
    "CO":    "edgar/co_png_3857/v8.1_FT2022_AP_CO_2022_TOTALS_emi_3857.png",
    "NH3":   "edgar/nh3_png_3857/v8.1_FT2022_AP_NH3_2022_TOTALS_emi_3857.png",
    "SO2":   "edgar/so2_png_3857/v8.1_FT2022_AP_SO2_2022_TOTALS_emi_3857.png",
    "NOx":   "edgar/nox_png_3857/v8.1_FT2022_AP_NOx_2022_TOTALS_emi_3857.png",
    "Hg":    "edgar/tox_hg_png_3857/v8.1_FT2022_TOX_Hg_2022_TOTALS_emi_3857.png",
}


def _sample_manifest_3857(manifest_path: str, lat: float, lng: float) -> bool:
    entries = _load_manifest_from_db(manifest_path)
    px, py = _latlng_to_3857(lat, lng)
    for entry in entries:
        west, north, east, south = entry["bbox"]
        min_x, max_x = min(west, east), max(west, east)
        min_y, max_y = min(south, north), max(south, north)
        if not (min_x <= px <= max_x and min_y <= py <= max_y):
            continue
        img = _open_cached_image(entry["url"])
        w, h = img.size
        frac_x = (px - min_x) / (max_x - min_x)
        frac_y = (max_y - py) / (max_y - min_y)
        ix = min(int(frac_x * w), w - 1)
        iy = min(int(frac_y * h), h - 1)
        r, g, b, a = img.getpixel((ix, iy))
        if a > 0 and (r + g + b) > 0:
            return True
    return False


def _sample_globio(manifest_path: str, lat: float, lng: float) -> bool:
    entries = _load_manifest_from_db(manifest_path)
    for entry in entries:
        bbox = entry["bbox"]
        is_meters = any(abs(v) > 360 for v in bbox)
        if is_meters:
            min_x, min_y, max_x, max_y = bbox
            px, py = _latlng_to_3857(lat, lng)
        else:
            south, north, west, east = bbox
            min_x, max_x = min(west, east), max(west, east)
            min_y, max_y = min(south, north), max(south, north)
            px, py = lng, lat
        if not (min_x <= px <= max_x and min_y <= py <= max_y):
            continue
        img = _open_cached_image(entry["url"])
        w, h = img.size
        frac_x = (px - min_x) / (max_x - min_x)
        frac_y = (max_y - py) / (max_y - min_y)
        ix = min(int(frac_x * w), w - 1)
        iy = min(int(frac_y * h), h - 1)
        r, g, b, a = img.getpixel((ix, iy))
        if a > 0 and (r + g + b) > 0:
            return True
    return False


def _sample_single_png(rel_path: str, lat: float, lng: float) -> bool:
    south, north, west, east = _GLOBAL_EXTENT
    if not (west <= lng <= east and south <= lat <= north):
        return False
    img = _open_cached_image(rel_path)
    w, h = img.size
    frac_x = (lng - west) / (east - west)
    frac_y = (north - lat) / (north - south)
    ix = min(int(frac_x * w), w - 1)
    iy = min(int(frac_y * h), h - 1)
    r, g, b, a = img.getpixel((ix, iy))
    return a > 0 and (r + g + b) > 0


@app.get("/sample_raster")
async def sample_raster(lat: float = Query(...), lng: float = Query(...)):
    """Return list of raster layer names that have non-transparent data at the given coordinate."""
    breaches: list[str] = []
    for name, manifest in _MANIFEST_LAYERS.items():
        try:
            if _sample_manifest_3857(manifest, lat, lng):
                breaches.append(name)
        except Exception as e:
            logger.warning(f"sample_raster {name}: {e}")
    for name, manifest in _GLOBIO_LAYERS.items():
        try:
            if _sample_globio(manifest, lat, lng):
                breaches.append(name)
        except Exception as e:
            logger.warning(f"sample_raster {name}: {e}")
    for name, png_path in _SINGLE_PNG_LAYERS.items():
        try:
            if _sample_single_png(png_path, lat, lng):
                breaches.append(name)
        except Exception as e:
            logger.warning(f"sample_raster {name}: {e}")
    return JSONResponse(content={"breaches": breaches})


# ── Run ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run(
        "app:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=False,
        workers=settings.WORKERS,
    )
