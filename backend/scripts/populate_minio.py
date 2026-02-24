"""Utility script to seed MinIO with a small example Zarr dataset.

Run from the backend directory after MinIO is up:

    python scripts/populate_minio.py

It will create a tiny 10-day hourly temperature field and write it to the
"pcmip-archive/zarr/obs/era5/single-levels" path that the API expects.

In a real deployment you'd push full observational/model data instead.
"""
import numpy as np
import xarray as xr
import pandas as pd
import s3fs

from config.settings import get_settings


def main():
    settings = get_settings()
    fs = s3fs.S3FileSystem(**settings.s3_storage_options)
    store_path = "pcmip-archive/zarr/obs/era5/single-levels"

    print(f"creating demo Zarr store at {store_path}")

    times = pd.date_range("2024-06-01", periods=10, freq="D")
    lats = np.array([51.5])
    lons = np.array([-0.1])
    data = np.zeros((len(times), len(lats), len(lons)), dtype=float)

    ds = xr.Dataset(
        {"tas": (("time", "latitude", "longitude"), data)},
        coords={"time": times, "latitude": lats, "longitude": lons},
        attrs={
            "source_id": "ERA5",
            "tracking_id": "demo-0001",
            "pcmip_raw_hash": "demo",
            "pcmip_ingest_ts": "2024-01-01T00:00:00",
            "grid_label": "demo-grid",
        },
    )

    ds.to_zarr(f"s3://{store_path}", mode="w", storage_options=settings.s3_storage_options)
    print("store written")


if __name__ == "__main__":
    main()
