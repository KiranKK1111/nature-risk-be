import os
from fastapi import FastAPI, File, UploadFile, Query, Response, BackgroundTasks, HTTPException, Request
from rio_tiler.io import Reader
from fastapi.responses import StreamingResponse, JSONResponse, HTMLResponse, FileResponse
import uvicorn
import requests
import json
from fastapi.middleware.cors import CORSMiddleware
import time
from fastapi.responses import StreamingResponse
import numpy as np
from PIL import Image
import io
import os
import subprocess
import uuid
import sys
import paramiko
from queue import Queue, Empty
import threading

from typing import Optional, Dict
from datetime import datetime
import logging
import zipfile
from pydantic import BaseModel

# For progress tracking
import uuid as py_uuid
from sse_starlette.sse import EventSourceResponse

# For system info
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
BASE_URL = "http://cc-esg-svc-clhs-esg-sa-10.apps.colt-np2.ocp.dev.net/esg/clhs/downloadlocationcsv?filename=/wb4_nfs357/imftusr051/cra/alignmentdata/"

os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Create a persistent session with connection pooling for better performance
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(
    pool_connections=10,  # Number of connection pools
    pool_maxsize=20,      # Max connections in pool
    max_retries=3,        # Retry on failure
    pool_block=False
)
session.mount('http://', adapter)
session.mount('https://', adapter)

# In-memory progress store: {session_id: {"total": int, "sent": int}}
progress_store = {}


# Endpoint to get scassets
@app.get("/get_scassets")
async def get_scassets(selectedValue: str = Query(...)):
    print("get_scassets: Selected Value : ", selectedValue)

    if selectedValue == "ClientAssetLocation":
        file_path = os.path.join("data", "client", "glencore.topojson")
    elif selectedValue == "AssetLocation":
        file_path = os.path.join("data", "client", "sc_assets.topojson")
    else:
        file_path = None

    if not file_path or not os.path.isfile(file_path):
        return {"error": "scassets file not found."}

    # Use FileResponse for efficient local file serving
    return FileResponse(
        file_path,
        media_type="application/json",
        headers={"Access-Control-Allow-Origin": "*"}
    )

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


# FastAPI endpoint to stream the jsonString    
@app.get("/loadIbatData_kba_stream")
def loadIbatData_kba_stream(selectedValue: str = Query(...)):
    if selectedValue == "KBAPOL2024STREAM":
        file_path = os.path.join("data", "proximity", "kba.topojson")
    elif selectedValue == "WDPA00STREAM":
        file_path = os.path.join("data", "proximity", "iucn.topojson")
    elif selectedValue == "RAMSARSTREAM":
        file_path = os.path.join("data", "proximity", "ramsar.topojson")
    elif selectedValue == "WHS-STREAM":
        file_path = os.path.join("data", "proximity", "whs.topojson")
    else:
        file_path = None

    if not file_path or not os.path.isfile(file_path):
        return {"Error": "TopoJSON file not found."}

    return FileResponse(
        file_path,
        media_type="application/json",
        headers={"Access-Control-Allow-Origin": "*"}
    )

# Layer for aquaduct bassline data
@app.get("/load_aquaduct_bassline_data")
async def load_aquaduct_bassline_data():
    file_path = os.path.join("data", "aquaduct", "bws_annual.topojson")
    if not os.path.isfile(file_path):
        return {"error": "TopoJSON file not found."}
    return FileResponse(
        file_path,
        media_type="application/json",
        headers={"Access-Control-Allow-Origin": "*"}
    )


MANIFEST_URL = os.environ.get("manifest_host_path", "http://hostname/path/manifest.json")

@app.get("/getManifest")
async def fetch_manifest(path: str = Query(..., description="Relative path to manifest (e.g. gfc-png/manifest.json)")):
    file_path = os.path.join("data", path.lstrip("/"))
    if not os.path.isfile(file_path):
        raise HTTPException(status_code=404, detail="Manifest not found")
    return FileResponse(
        file_path,
        media_type="application/json",
        headers={"Access-Control-Allow-Origin": "*"}
    )

@app.get("/getPng")
async def get_png(path: str = Query(..., description="Relative path to PNG file (e.g. gfc-png/Hansen_GFC-2024-v1.12_lossyear_00N_000E.png)")):
    file_path = os.path.join("data", path.lstrip("/"))
    if not os.path.isfile(file_path):
        raise HTTPException(status_code=404, detail="PNG not found")
    return FileResponse(
        file_path,
        media_type="image/png",
        headers={"Access-Control-Allow-Origin": "*"}
    )

# ── Raster pixel sampling for breach detection ─────────────────────────────────
import math

# Cache opened PIL images so we only read from disk once per process lifetime.
_raster_image_cache: Dict[str, Image.Image] = {}

def _open_cached(path: str) -> Image.Image:
    if path not in _raster_image_cache:
        _raster_image_cache[path] = Image.open(path).convert("RGBA")
    return _raster_image_cache[path]

# EPSG:3857 helpers
def _latlng_to_3857(lat: float, lng: float):
    R = 6378137.0
    x = math.radians(lng) * R
    y = math.log(math.tan(math.pi / 4 + math.radians(lat) / 2)) * R
    return x, y

# Raster layers with manifest-based tiles (GFC has many tiles)
_MANIFEST_LAYERS = {
    "Tree Cover Loss": "gfc-png/local_manifest.json",
}

# Raster layers with manifest-based tiles (GLOBIO single-tile manifests)
_GLOBIO_LAYERS = {
    "Land Use":  "globio/lu_pngs/2020/tile_manifest.json",
    "MSA":       "globio/msa_pngs/SSP5/tile_manifest.json",
}

# Single-PNG global layers (EDGAR + DynQual) — fixed extent near-global
_GLOBAL_EXTENT = (-85.04, 85.04, -179.99, 179.99)  # south, north, west, east

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

# Cache manifests
_manifest_cache: Dict[str, list] = {}

def _load_manifest(rel_path: str) -> list:
    if rel_path not in _manifest_cache:
        fp = os.path.join("data", rel_path)
        with open(fp) as f:
            _manifest_cache[rel_path] = json.load(f)
    return _manifest_cache[rel_path]


def _sample_manifest_3857(manifest_path: str, lat: float, lng: float) -> bool:
    """Sample a manifest-based layer (GFC) where bbox is in EPSG:3857 meters [west, north, east, south]."""
    entries = _load_manifest(manifest_path)
    px, py = _latlng_to_3857(lat, lng)
    for entry in entries:
        west, north, east, south = entry["bbox"]
        min_x, max_x = min(west, east), max(west, east)
        min_y, max_y = min(south, north), max(south, north)
        if not (min_x <= px <= max_x and min_y <= py <= max_y):
            continue
        img_path = os.path.join("data", entry["url"])
        if not os.path.isfile(img_path):
            continue
        img = _open_cached(img_path)
        w, h = img.size
        frac_x = (px - min_x) / (max_x - min_x)
        frac_y = (max_y - py) / (max_y - min_y)  # y flipped (top=0)
        ix = min(int(frac_x * w), w - 1)
        iy = min(int(frac_y * h), h - 1)
        r, g, b, a = img.getpixel((ix, iy))
        if a > 0 and (r + g + b) > 0:
            return True
    return False


def _sample_globio(manifest_path: str, lat: float, lng: float) -> bool:
    """Sample GLOBIO layers — auto-detect degree vs meter bbox."""
    entries = _load_manifest(manifest_path)
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
        img_path = os.path.join("data", entry["url"])
        if not os.path.isfile(img_path):
            continue
        img = _open_cached(img_path)
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
    """Sample a single global PNG (EDGAR/DynQual) with fixed near-global extent in degrees."""
    img_path = os.path.join("data", rel_path)
    if not os.path.isfile(img_path):
        return False
    south, north, west, east = _GLOBAL_EXTENT
    if not (west <= lng <= east and south <= lat <= north):
        return False
    img = _open_cached(img_path)
    w, h = img.size
    frac_x = (lng - west) / (east - west)
    frac_y = (north - lat) / (north - south)  # y flipped
    ix = min(int(frac_x * w), w - 1)
    iy = min(int(frac_y * h), h - 1)
    r, g, b, a = img.getpixel((ix, iy))
    return a > 0 and (r + g + b) > 0


@app.get("/sample_raster")
async def sample_raster(lat: float = Query(...), lng: float = Query(...)):
    """Return list of raster layer names that have non-transparent data at the given coordinate."""
    breaches: list[str] = []

    # Manifest-based layers (GFC — EPSG:3857 meters bbox)
    for name, manifest in _MANIFEST_LAYERS.items():
        try:
            if _sample_manifest_3857(manifest, lat, lng):
                breaches.append(name)
        except Exception as e:
            logger.warning(f"sample_raster {name}: {e}")

    # GLOBIO layers (auto-detect bbox format)
    for name, manifest in _GLOBIO_LAYERS.items():
        try:
            if _sample_globio(manifest, lat, lng):
                breaches.append(name)
        except Exception as e:
            logger.warning(f"sample_raster {name}: {e}")

    # Single-PNG global layers (EDGAR + DynQual)
    for name, png_path in _SINGLE_PNG_LAYERS.items():
        try:
            if _sample_single_png(png_path, lat, lng):
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


