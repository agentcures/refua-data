from refua_data.catalog import get_default_catalog


def test_default_catalog_contains_core_and_api_datasets() -> None:
    catalog = get_default_catalog()
    datasets = catalog.list()
    ids = {dataset.dataset_id for dataset in datasets}

    assert "zinc15_250k" in ids
    assert "zinc15_tranche_druglike_wait_ok" in ids
    assert "chembl_activity_ki_human" in ids
    assert "chembl_activity_kd_human" in ids
    assert "chembl_activity_ec50_human" in ids
    assert "chembl_activity_ac50_human" in ids
    assert "chembl_assays_functional_human" in ids
    assert "chembl_assays_adme_human" in ids
    assert "chembl_molecules_phase4" in ids
    assert "chembl_molecules_black_box_warning" in ids
    assert "chembl_mechanism_phase2plus" in ids
    assert "chembl_drug_indications_phase2plus" in ids
    assert "chembl_drug_indications_phase3plus" in ids
    assert "uniprot_human_reviewed" in ids
    assert "uniprot_human_receptors" in ids
    assert "uniprot_human_membrane" in ids
    assert "uniprot_human_nucleus" in ids
    assert "uniprot_human_secreted" in ids
    assert "uniprot_human_transcription_factors" in ids
    assert "uniprot_human_enzymes" in ids
    assert "chembl_targets_human_protein_complex" in ids
    assert len(datasets) >= 42
