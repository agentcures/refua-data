from refua_data.catalog import get_default_catalog


def test_default_catalog_contains_core_and_api_datasets() -> None:
    catalog = get_default_catalog()
    datasets = catalog.list()
    ids = {dataset.dataset_id for dataset in datasets}

    assert "zinc15_250k" in ids
    assert "zinc15_tranche_druglike_wait_ok" in ids
    assert "chembl_activity_ki_human" in ids
    assert "uniprot_human_reviewed" in ids
    assert len(datasets) >= 25
