from __future__ import annotations

import os
from pathlib import Path
from typing import Callable

from google.cloud import storage


def parse_gs_uri(uri: str) -> tuple[str, str]:
    value = (uri or "").strip()
    if not value.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {uri}")
    without_scheme = value[5:]
    if "/" not in without_scheme:
        return without_scheme, ""
    bucket, blob = without_scheme.split("/", 1)
    return bucket, blob


def resolve_adc_credentials_from_env(
    resolve_path_fn: Callable[[str], Path],
) -> Path | None:
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    credentials_root = os.getenv("GOOGLE_CREDENTIALS_PATH", "").strip()
    if not credentials_path:
        return None

    candidate = Path(credentials_path)
    if candidate.is_absolute():
        if candidate.exists():
            return candidate
        if credentials_root:
            root_path = resolve_path_fn(credentials_root)
            joined = root_path / credentials_path.lstrip("/")
            if joined.exists():
                return joined
        return candidate

    if credentials_root:
        root_path = resolve_path_fn(credentials_root)
        return root_path / credentials_path
    return resolve_path_fn(credentials_path)


def upload_file_to_gcs_blob(
    *,
    client: storage.Client,
    bucket_name: str,
    blob_name: str,
    local_file: Path,
) -> str:
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(str(local_file))
    if not blob.exists(client=client):
        raise RuntimeError(f"Uploaded object not found after upload: gs://{bucket_name}/{blob_name}")
    return f"gs://{bucket_name}/{blob_name}"


def upload_file_to_gcs_uri(
    *,
    client: storage.Client,
    local_file: Path,
    target_uri: str,
) -> str:
    bucket_name, blob_name = parse_gs_uri(target_uri)
    return upload_file_to_gcs_blob(
        client=client,
        bucket_name=bucket_name,
        blob_name=blob_name,
        local_file=local_file,
    )
