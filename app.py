import os
from fastapi import FastAPI, Query, HTTPException
from requests.exceptions import RequestException
from fastapi.responses import StreamingResponse, JSONResponse, Response
import uvicorn
import requests
import json
from fastapi.middleware.cors import CORSMiddleware
import threading
from concurrent.futures import ThreadPoolExecutor
import logging
import psutil

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create a FastAPI app
app = FastAPI(root_path="/esg/nature-risk")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust this to restrict origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Define the base URL for CDN file access
# BASE_URL = "http://cc-esg-svc-clhs-esg-sa-10.apps.colt-np2.ocp.dev.net/esg/clhs/downloadlocationcsv?filename=/wb4_nfs357/imftusr051/cra/alignmentdata/"

BASE_URL = "http://edfinhub.com/files/download?filename=/alignmentdata/"

os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Create a persistent session with connection pooling for better performance
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(
    pool_connections=20,
    pool_maxsize=50,
    max_retries=3,
    pool_block=False
)
session.mount('http://', adapter)
session.mount('https://', adapter)

# Thread pool for parallel prefetching (20 concurrent downloads)
prefetch_executor = ThreadPoolExecutor(max_workers=20)

# In-memory progress store: {session_id: {"total": int, "sent": int}}
progress_store = {}

# ── In-memory file cache ──────────────────────────────────────────────
# Cache never expires while server is running. Restart server to refresh.
file_cache = {}  # {relative_path: bytes}
_prefetching = set()  # tracks paths currently being prefetched

def get_cached_or_fetch(relative_path: str) -> bytes:
    """Return file bytes from cache or fetch from VPS and cache."""
    cached = file_cache.get(relative_path)
    if cached is not None:
        return cached

    url = BASE_URL + relative_path.lstrip("/")
    logger.info(f"Cache MISS, fetching: {url}")
    response = session.get(url, timeout=(10, 300), headers={'Accept-Encoding': 'gzip, deflate'})
    response.raise_for_status()
    data = response.content
    file_cache[relative_path] = data
    logger.info(f"Cached: {relative_path} ({len(data) / (1024*1024):.1f} MB)")
    return data

def prefetch_file(relative_path: str):
    """Prefetch a single file."""
    try:
        get_cached_or_fetch(relative_path)
    except Exception as e:
        logger.error(f"Prefetch failed for {relative_path}: {e}")

def prefetch_manifest_pngs(manifest_path: str):
    """After a manifest is fetched, prefetch all its PNGs in background."""
    if manifest_path in _prefetching:
        return
    _prefetching.add(manifest_path)

    def _do_prefetch():
        try:
            manifest_data = file_cache.get(manifest_path)
            if not manifest_data:
                return
            entries = json.loads(manifest_data)
            png_paths = [e["url"] for e in entries if "url" in e]
            uncached = [p for p in png_paths if p not in file_cache]
            if not uncached:
                return
            logger.info(f"Prefetching {len(uncached)} PNGs from {manifest_path}...")
            list(prefetch_executor.map(prefetch_file, uncached))
            logger.info(f"Prefetch done for {manifest_path}: {len(uncached)} PNGs cached.")
        except Exception as e:
            logger.error(f"PNG prefetch error for {manifest_path}: {e}")

    threading.Thread(target=_do_prefetch, daemon=True).start()

# Prefetch commonly used files on startup
PREFETCH_FILES = [
    "client/glencore.topojson",
    "client/sc_assets.topojson",
    "proximity/kba.topojson",
    "proximity/iucn.topojson",
    "proximity/ramsar.topojson",
    "proximity/whs.topojson",
    "aquaduct/bws_annual.topojson",
]

@app.on_event("startup")
def startup_prefetch():
    """Prefetch all common files in parallel on startup. Blocks until all are cached."""
    logger.info("Starting prefetch of common files...")
    list(prefetch_executor.map(prefetch_file, PREFETCH_FILES))
    logger.info(f"Prefetch complete — {len(PREFETCH_FILES)} files cached in memory.")

@app.get("/get_scassets")
async def get_scassets(selectedValue: str = Query(...)):
    if selectedValue == "ClientAssetLocation":
        rel_path = "client/glencore.topojson"
    elif selectedValue == "AssetLocation":
        rel_path = "client/sc_assets.topojson"
    else:
        return {"error": "scassets file not found."}

    try:
        data = get_cached_or_fetch(rel_path)
        return Response(content=data, media_type="application/json")
    except RequestException as e:
        return {"error": f"Failed to fetch file: {str(e)}"}

# New endpoint for CPU info
@app.get("/cpu-info")
async def cpu_info():
    # CPU info
    total_cores = psutil.cpu_count(logical=True)
    physical_cores = psutil.cpu_count(logical=False)
    cpu_percent = psutil.cpu_percent(interval=1)
    # RAM info
    mem = psutil.virtual_memory()
    ram_info = {
        "total": mem.total,
        "available": mem.available,
        "used": mem.used,
        "percent": mem.percent
    }
    info = {
        "total_cores": total_cores,
        "physical_cores": physical_cores,
        "cpu_percent": cpu_percent,
        "ram": ram_info
    }
    return JSONResponse(content=info)


# SSE endpoint for real-time CPU and RAM usage
@app.get("/cpu-stream")
async def cpu_stream():
    def event_stream():
        while True:
            total_cores = psutil.cpu_count(logical=True)
            physical_cores = psutil.cpu_count(logical=False)
            cpu_percent = psutil.cpu_percent(interval=1)
            mem = psutil.virtual_memory()
            ram_info = {
                "total": mem.total,
                "available": mem.available,
                "used": mem.used,
                "percent": mem.percent
            }
            data = json.dumps({
                "total_cores": total_cores,
                "physical_cores": physical_cores,
                "cpu_percent": cpu_percent,
                "ram": ram_info
            })
            yield f"data: {data}\n\n"
    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.get("/loadIbatData_kba_stream")
def loadIbatData_kba_stream(selectedValue: str = Query(...)):
    mapping = {
        "KBAPOL2024STREAM": "proximity/kba.topojson",
        "WDPA00STREAM": "proximity/iucn.topojson",
        "RAMSARSTREAM": "proximity/ramsar.topojson",
        "WHS-STREAM": "proximity/whs.topojson",
    }
    rel_path = mapping.get(selectedValue)
    if rel_path is None:
        return {"Error": "TopoJSON file not found."}

    try:
        data = get_cached_or_fetch(rel_path)
        return Response(content=data, media_type="application/json")
    except RequestException as e:
        return {"error": f"Failed to fetch file: {str(e)}"}
    
@app.get("/load_aquaduct_bassline_data")
async def load_aquaduct_bassline_data():
    try:
        data = get_cached_or_fetch("aquaduct/bws_annual.topojson")
        return Response(content=data, media_type="application/json")
    except RequestException as e:
        return {"error": f"Failed to fetch file: {str(e)}"}

@app.get("/getManifest")
async def fetch_manifest(path: str = Query(..., description="Relative path to manifest (e.g. gfc-png/manifest.json)")):
    try:
        data = get_cached_or_fetch(path)
        # Trigger background prefetch of all PNGs in this manifest
        prefetch_manifest_pngs(path)
        return Response(content=data, media_type="application/json")
    except RequestException as e:
        raise HTTPException(status_code=404, detail=f"Manifest not found: {str(e)}")

@app.get("/getPng")
async def get_png(path: str = Query(..., description="Relative path to PNG file")):
    try:
        data = get_cached_or_fetch(path)
        return Response(content=data, media_type="image/png")
    except RequestException as e:
        raise HTTPException(status_code=404, detail=f"PNG not found: {str(e)}")

# ── Raster pixel sampling for breach detection ─────────────────────────────────
import math
from PIL import Image
import io
from typing import Dict

# Cache opened PIL images keyed by relative path.
_raster_image_cache: Dict[str, Image.Image] = {}

def _open_cached_prod(rel_path: str) -> Image.Image:
    if rel_path not in _raster_image_cache:
        raw = get_cached_or_fetch(rel_path)
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
_manifest_cache_prod: Dict[str, list] = {}

def _load_manifest_prod(rel_path: str) -> list:
    if rel_path not in _manifest_cache_prod:
        raw = get_cached_or_fetch(rel_path)
        _manifest_cache_prod[rel_path] = json.loads(raw)
    return _manifest_cache_prod[rel_path]

def _sample_manifest_3857_prod(manifest_path: str, lat: float, lng: float) -> bool:
    entries = _load_manifest_prod(manifest_path)
    px, py = _latlng_to_3857(lat, lng)
    for entry in entries:
        west, north, east, south = entry["bbox"]
        min_x, max_x = min(west, east), max(west, east)
        min_y, max_y = min(south, north), max(south, north)
        if not (min_x <= px <= max_x and min_y <= py <= max_y):
            continue
        img = _open_cached_prod(entry["url"])
        w, h = img.size
        frac_x = (px - min_x) / (max_x - min_x)
        frac_y = (max_y - py) / (max_y - min_y)
        ix = min(int(frac_x * w), w - 1)
        iy = min(int(frac_y * h), h - 1)
        r, g, b, a = img.getpixel((ix, iy))
        if a > 0 and (r + g + b) > 0:
            return True
    return False

def _sample_globio_prod(manifest_path: str, lat: float, lng: float) -> bool:
    entries = _load_manifest_prod(manifest_path)
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
        img = _open_cached_prod(entry["url"])
        w, h = img.size
        frac_x = (px - min_x) / (max_x - min_x)
        frac_y = (max_y - py) / (max_y - min_y)
        ix = min(int(frac_x * w), w - 1)
        iy = min(int(frac_y * h), h - 1)
        r, g, b, a = img.getpixel((ix, iy))
        if a > 0 and (r + g + b) > 0:
            return True
    return False

def _sample_single_png_prod(rel_path: str, lat: float, lng: float) -> bool:
    south, north, west, east = _GLOBAL_EXTENT
    if not (west <= lng <= east and south <= lat <= north):
        return False
    img = _open_cached_prod(rel_path)
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
            if _sample_manifest_3857_prod(manifest, lat, lng):
                breaches.append(name)
        except Exception as e:
            logger.warning(f"sample_raster {name}: {e}")
    for name, manifest in _GLOBIO_LAYERS.items():
        try:
            if _sample_globio_prod(manifest, lat, lng):
                breaches.append(name)
        except Exception as e:
            logger.warning(f"sample_raster {name}: {e}")
    for name, png_path in _SINGLE_PNG_LAYERS.items():
        try:
            if _sample_single_png_prod(png_path, lat, lng):
                breaches.append(name)
        except Exception as e:
            logger.warning(f"sample_raster {name}: {e}")
    return JSONResponse(content={"breaches": breaches})


# Run FastAPI
if __name__ == "__main__":
    # Get port from environment variable or default to 8000
    port = int(os.environ.get("PORT", 8510))
    
    # Determine if we're in production (OpenShift) or development
    is_production = os.environ.get("ENV", "development").lower() in ["production", "prod", "prd"]
    
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        workers=1
    )