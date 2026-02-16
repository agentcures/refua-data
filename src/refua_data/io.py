"""Input readers for tabular dataset files."""

from __future__ import annotations

import io
import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import Literal

import pandas as pd

from .models import DatasetDefinition


def infer_delimiter(dataset: DatasetDefinition, raw_path: Path) -> str:
    """Infer delimiter from dataset metadata and filename."""
    if dataset.delimiter:
        return dataset.delimiter

    name = raw_path.name.lower()
    if dataset.file_format == "tsv":
        return "\t"
    if name.endswith(".tsv") or name.endswith(".txt"):
        return "\t"
    return ","


def iter_dataset_chunks(
    raw_path: Path,
    *,
    dataset: DatasetDefinition,
    chunksize: int,
) -> Iterator[pd.DataFrame]:
    """Yield DataFrame chunks from a dataset raw file."""
    if dataset.file_format == "jsonl":
        yield from _iter_jsonl_chunks(raw_path, chunksize=chunksize)
        return

    delimiter = infer_delimiter(dataset, raw_path)
    lower_name = raw_path.name.lower()
    if lower_name.endswith(".zip"):
        yield from _iter_csv_like_from_zip(raw_path, delimiter=delimiter, chunksize=chunksize)
        return

    compression: Literal["infer", "gzip"] | None = "infer"
    if dataset.compression == "none":
        compression = None
    elif dataset.compression == "gzip":
        compression = "gzip"

    reader = pd.read_csv(
        raw_path,
        sep=delimiter,
        compression=compression,
        chunksize=chunksize,
        low_memory=False,
    )
    for chunk in reader:
        yield prepare_dataframe(chunk)


def _iter_jsonl_chunks(raw_path: Path, *, chunksize: int) -> Iterator[pd.DataFrame]:
    reader = pd.read_json(raw_path, lines=True, compression="infer", chunksize=chunksize)
    for chunk in reader:
        yield prepare_dataframe(chunk)


def _iter_csv_like_from_zip(
    raw_path: Path,
    *,
    delimiter: str,
    chunksize: int,
) -> Iterator[pd.DataFrame]:
    with zipfile.ZipFile(raw_path, mode="r") as archive:
        member_name = _choose_zip_member(archive)
        with archive.open(member_name, mode="r") as zipped_file:
            text_handle = io.TextIOWrapper(zipped_file, encoding="utf-8")
            reader = pd.read_csv(
                text_handle,
                sep=delimiter,
                chunksize=chunksize,
                low_memory=False,
            )
            for chunk in reader:
                yield prepare_dataframe(chunk)


def _choose_zip_member(archive: zipfile.ZipFile) -> str:
    preferred_suffixes = (".csv", ".tsv", ".txt", ".jsonl")
    candidates = [
        name for name in archive.namelist() if not name.endswith("/")
    ]
    if not candidates:
        raise ValueError("Zip archive does not contain files.")

    for suffix in preferred_suffixes:
        for candidate in candidates:
            if candidate.lower().endswith(suffix):
                return candidate

    # Fallback to first file when extension hints are unavailable.
    return candidates[0]


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize DataFrame dtypes for reliable parquet serialization."""
    normalized = df.copy()
    for column, dtype in normalized.dtypes.items():
        if str(dtype) == "object":
            normalized[column] = normalized[column].astype("string")
    return normalized
