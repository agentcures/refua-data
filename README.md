# refua-data

`refua-data` is the Refua data layer for drug discovery. It provides a curated dataset catalog, intelligent local caching, and parquet materialization optimized for downstream modeling and campaign workflows.

## What it provides

- A built-in catalog of useful drug-discovery datasets.
- Dataset-aware download pipeline with cache reuse and metadata tracking.
- Pluggable cache backend architecture (filesystem cache by default).
- API dataset ingestion for paginated JSON endpoints (for example ChEMBL and UniProt).
- HTTP conditional refresh support (`ETag` / `Last-Modified`) when enabled.
- Incremental parquet materialization (chunked processing + partitioned parquet parts).
- CLI for listing, fetching, and materializing datasets.
- Source health checks via `validate-sources` for CI and environment diagnostics.
- Rich dataset metadata snapshots (description + usage notes) persisted in cache metadata.

## Included datasets

The default catalog includes local-file/HTTP datasets plus API presets useful in drug discovery, including **ZINC**, **ChEMBL**, and **UniProt**.

1. `zinc15_250k` (ZINC)
2. `zinc15_tranche_druglike_instock` (ZINC tranche)
3. `zinc15_tranche_druglike_agent` (ZINC tranche)
4. `zinc15_tranche_druglike_wait_ok` (ZINC tranche)
5. `zinc15_tranche_druglike_boutique` (ZINC tranche)
6. `zinc15_tranche_druglike_annotated` (ZINC tranche)
7. `tox21`
8. `bbbp`
9. `bace`
10. `clintox`
11. `sider`
12. `hiv`
13. `muv`
14. `esol`
15. `freesolv`
16. `lipophilicity`
17. `pcba`
18. `chembl_activity_ki_human`
19. `chembl_activity_ic50_human`
20. `chembl_activity_kd_human`
21. `chembl_activity_ec50_human`
22. `chembl_assays_binding_human`
23. `chembl_assays_functional_human`
24. `chembl_targets_human_single_protein`
25. `chembl_molecules_phase3plus`
26. `chembl_molecules_black_box_warning`
27. `chembl_mechanism_phase2plus`
28. `chembl_drug_indications_phase2plus`
29. `chembl_drug_indications_phase3plus`
30. `uniprot_human_reviewed`
31. `uniprot_human_receptors`
32. `uniprot_human_kinases`
33. `uniprot_human_gpcr`
34. `uniprot_human_ion_channels`
35. `uniprot_human_transporters`
36. `uniprot_human_secreted`
37. `uniprot_human_transcription_factors`
38. `uniprot_human_enzymes`

Most of these are distributed through MoleculeNet/DeepChem mirrors and retain upstream licensing terms.
ChEMBL and UniProt presets are fetched through their public REST APIs and cached locally as JSONL.
ZINC tranche presets aggregate multiple tranche files per dataset (drug-like MW B-K and logP A-K bins,
reactivity A/B/C/E) into one cached tabular source during fetch.

## Install

```bash
cd refua-data
pip install -e .
```

## CLI quickstart

List datasets:

```bash
refua-data list
```

Validate all dataset sources:

```bash
refua-data validate-sources
```

Validate a subset and fail CI on probe failures:

```bash
refua-data validate-sources chembl_activity_ki_human uniprot_human_kinases --fail-on-error
```

JSON output for automation:

```bash
refua-data validate-sources --json --fail-on-error
```

For datasets with multiple mirrors, source validation succeeds when at least one configured source
is reachable. Failed fallback attempts are included in the result details.

Fetch raw data with cache:

```bash
refua-data fetch zinc15_250k
```

Fetch API-based presets:

```bash
refua-data fetch chembl_activity_ki_human
refua-data fetch uniprot_human_kinases
```

Materialize parquet:

```bash
refua-data materialize zinc15_250k
```

Refresh against remote metadata:

```bash
refua-data fetch zinc15_250k --refresh
```

For API datasets, `--refresh` re-runs the API query (with conditional headers on first page when available).

## Cache layout

By default, cache root is:

- `~/.cache/refua-data`

Override with:

- `REFUA_DATA_HOME=/custom/path`

Layout:

- `raw/<dataset>/<version>/...` downloaded source files
- `_meta/raw/<dataset>/<version>/...json` raw metadata (`etag`, `sha256`, API request signature, rows/pages, dataset description/usage metadata)
- `parquet/<dataset>/<version>/part-*.parquet` materialized parquet parts
- `_meta/parquet/<dataset>/<version>/manifest.json` parquet manifest metadata with dataset snapshot

## Python API

```python
from refua_data import DatasetManager

manager = DatasetManager()
manager.fetch("zinc15_250k")
manager.fetch("chembl_activity_ki_human")
result = manager.materialize("zinc15_250k")
print(result.parquet_dir)
```

`DataCache` is the default cache backend. You can pass a custom backend object that implements
the same interface (`ensure`, `raw_file`, `raw_meta`, `parquet_dir`, `parquet_manifest`,
`read_json`, `write_json`) to make storage pluggable.

## Licensing notes

- `refua-data` package code is MIT licensed.
- Dataset content licenses are dataset-specific and controlled by upstream providers.
- Always verify dataset licensing and allowed use before redistribution or commercial deployment.
