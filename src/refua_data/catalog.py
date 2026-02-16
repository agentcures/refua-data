"""Built-in dataset catalog for drug discovery workloads."""

from __future__ import annotations

from builtins import list as builtin_list
from dataclasses import dataclass

from .models import ApiDatasetConfig, DatasetDefinition


@dataclass(slots=True)
class DatasetCatalog:
    """In-memory dataset registry."""

    datasets: dict[str, DatasetDefinition]

    @classmethod
    def from_entries(cls, entries: list[DatasetDefinition]) -> DatasetCatalog:
        by_id = {entry.dataset_id: entry for entry in entries}
        if len(by_id) != len(entries):
            raise ValueError("Dataset IDs must be unique.")
        return cls(datasets=by_id)

    def list(self) -> list[DatasetDefinition]:
        """Return datasets sorted by ID."""
        return [self.datasets[key] for key in sorted(self.datasets)]

    def get(self, dataset_id: str) -> DatasetDefinition:
        """Get a dataset by id."""
        try:
            return self.datasets[dataset_id]
        except KeyError as exc:
            available = ", ".join(sorted(self.datasets))
            raise KeyError(
                f"Unknown dataset '{dataset_id}'. Available datasets: {available}"
            ) from exc

    def filter_by_tag(self, tag: str) -> builtin_list[DatasetDefinition]:
        """Filter datasets by a tag."""
        needle = tag.strip().lower()
        return [
            dataset
            for dataset in self.list()
            if needle in {value.lower() for value in dataset.tags}
        ]


_ZINC_DRUGLIKE_MWT_BINS = tuple("BCDEFGHIJK")
_ZINC_DRUGLIKE_LOGP_BINS = tuple("ABCDEFGHIJK")
_ZINC_DRUGLIKE_REACTIVITY_LEVELS = ("A", "B", "C", "E")


def _zinc_druglike_tranche_urls(
    *,
    purchasability: str,
    reactive_levels: tuple[str, ...] = _ZINC_DRUGLIKE_REACTIVITY_LEVELS,
) -> tuple[str, ...]:
    return tuple(
        f"https://files.docking.org/2D/{mwt}{logp}/{mwt}{logp}{reactive}{purchasability}.txt"
        for mwt in _ZINC_DRUGLIKE_MWT_BINS
        for logp in _ZINC_DRUGLIKE_LOGP_BINS
        for reactive in reactive_levels
    )


_DEFAULT_DATASETS = [
    DatasetDefinition(
        dataset_id="zinc15_250k",
        name="ZINC15 250K (2D)",
        description=(
            "A 250k compound subset from ZINC suitable for virtual "
            "screening and pretraining."
        ),
        source="ZINC via chemical_vae mirror",
        homepage="https://zinc.docking.org/",
        license_name="Upstream ZINC terms",
        license_url="https://zinc.docking.org/terms/",
        urls=(
            "https://raw.githubusercontent.com/aspuru-guzik-group/chemical_vae/main/models/zinc_properties/250k_rndm_zinc_drugs_clean_3.csv",
            "https://raw.githubusercontent.com/aspuru-guzik-group/chemical_vae/master/models/zinc_properties/250k_rndm_zinc_drugs_clean_3.csv",
        ),
        file_format="csv",
        category="compound_library",
        tags=("zinc", "virtual_screening", "small_molecules"),
    ),
    DatasetDefinition(
        dataset_id="zinc15_tranche_druglike_instock",
        name="ZINC15 Drug-Like In-Stock (2D, Multi-Tranche)",
        description=(
            "Multi-tranche drug-like subset from ZINC15 across MW bins B-K and "
            "logP bins A-K with up-to-standard reactivity (A/B/C/E) and in-stock "
            "purchasability."
        ),
        source="ZINC tranche download (multi-tranche)",
        homepage="https://zinc.docking.org/tranches/home/",
        license_name="Upstream ZINC terms",
        license_url="https://zinc.docking.org/terms/",
        urls=_zinc_druglike_tranche_urls(purchasability="B"),
        file_format="tsv",
        category="compound_library",
        url_mode="concat",
        tags=(
            "zinc",
            "tranche",
            "multi_tranche",
            "drug_like",
            "in_stock",
            "small_molecules",
        ),
    ),
    DatasetDefinition(
        dataset_id="zinc15_tranche_druglike_agent",
        name="ZINC15 Drug-Like Agent (2D, Multi-Tranche)",
        description=(
            "Multi-tranche drug-like subset from ZINC15 across MW bins B-K and "
            "logP bins A-K with up-to-standard reactivity (A/B/C/E) and "
            "agent-level "
            "purchasability."
        ),
        source="ZINC tranche download (multi-tranche)",
        homepage="https://zinc.docking.org/tranches/home/",
        license_name="Upstream ZINC terms",
        license_url="https://zinc.docking.org/terms/",
        urls=_zinc_druglike_tranche_urls(purchasability="C"),
        file_format="tsv",
        category="compound_library",
        url_mode="concat",
        tags=(
            "zinc",
            "tranche",
            "multi_tranche",
            "drug_like",
            "agent",
            "small_molecules",
        ),
    ),
    DatasetDefinition(
        dataset_id="zinc15_tranche_druglike_wait_ok",
        name="ZINC15 Drug-Like Wait-OK (2D, Multi-Tranche)",
        description=(
            "Multi-tranche drug-like subset from ZINC15 across MW bins B-K and "
            "logP bins A-K with up-to-standard reactivity (A/B/C/E) and wait-ok "
            "purchasability."
        ),
        source="ZINC tranche download (multi-tranche)",
        homepage="https://zinc.docking.org/tranches/home/",
        license_name="Upstream ZINC terms",
        license_url="https://zinc.docking.org/terms/",
        urls=_zinc_druglike_tranche_urls(purchasability="D"),
        file_format="tsv",
        category="compound_library",
        url_mode="concat",
        tags=(
            "zinc",
            "tranche",
            "multi_tranche",
            "drug_like",
            "wait_ok",
            "small_molecules",
        ),
    ),
    DatasetDefinition(
        dataset_id="zinc15_tranche_druglike_boutique",
        name="ZINC15 Drug-Like Boutique (2D, Multi-Tranche)",
        description=(
            "Multi-tranche drug-like subset from ZINC15 across MW bins B-K and "
            "logP bins A-K with up-to-standard reactivity (A/B/C/E) and boutique "
            "purchasability."
        ),
        source="ZINC tranche download (multi-tranche)",
        homepage="https://zinc.docking.org/tranches/home/",
        license_name="Upstream ZINC terms",
        license_url="https://zinc.docking.org/terms/",
        urls=_zinc_druglike_tranche_urls(purchasability="E"),
        file_format="tsv",
        category="compound_library",
        url_mode="concat",
        tags=(
            "zinc",
            "tranche",
            "multi_tranche",
            "drug_like",
            "boutique",
            "small_molecules",
        ),
    ),
    DatasetDefinition(
        dataset_id="zinc15_tranche_druglike_annotated",
        name="ZINC15 Drug-Like Annotated (2D, Multi-Tranche)",
        description=(
            "Multi-tranche drug-like subset from ZINC15 across MW bins B-K and "
            "logP bins A-K with up-to-standard reactivity (A/B/C/E) and annotated "
            "purchasability."
        ),
        source="ZINC tranche download (multi-tranche)",
        homepage="https://zinc.docking.org/tranches/home/",
        license_name="Upstream ZINC terms",
        license_url="https://zinc.docking.org/terms/",
        urls=_zinc_druglike_tranche_urls(purchasability="F"),
        file_format="tsv",
        category="compound_library",
        url_mode="concat",
        tags=(
            "zinc",
            "tranche",
            "multi_tranche",
            "drug_like",
            "annotated",
            "small_molecules",
        ),
    ),
    DatasetDefinition(
        dataset_id="tox21",
        name="Tox21",
        description="Nuclear receptor and stress response toxicity assays.",
        source="MoleculeNet/DeepChem",
        homepage="https://moleculenet.org/datasets-1",
        license_name="Dataset-specific upstream terms",
        license_url="https://moleculenet.org/",
        urls=("https://deepchemdata.s3-us-west-1.amazonaws.com/datasets/tox21.csv.gz",),
        file_format="csv",
        category="toxicity",
        tags=("toxicity", "classification", "admet"),
    ),
    DatasetDefinition(
        dataset_id="bbbp",
        name="BBBP",
        description="Blood-brain barrier penetration classification dataset.",
        source="MoleculeNet/DeepChem",
        homepage="https://moleculenet.org/datasets-1",
        license_name="Dataset-specific upstream terms",
        license_url="https://moleculenet.org/",
        urls=(
            "https://deepchemdata.s3-us-west-1.amazonaws.com/datasets/BBBP.csv",
        ),
        file_format="csv",
        category="admet",
        tags=("bbb", "classification", "admet"),
    ),
    DatasetDefinition(
        dataset_id="bace",
        name="BACE",
        description="Binding and inhibition labels for beta-secretase 1.",
        source="MoleculeNet/DeepChem",
        homepage="https://moleculenet.org/datasets-1",
        license_name="Dataset-specific upstream terms",
        license_url="https://moleculenet.org/",
        urls=("https://deepchemdata.s3-us-west-1.amazonaws.com/datasets/bace.csv",),
        file_format="csv",
        category="target_activity",
        tags=("target", "activity", "classification", "regression"),
    ),
    DatasetDefinition(
        dataset_id="clintox",
        name="ClinTox",
        description="Clinical toxicity labels for marketed and failed compounds.",
        source="MoleculeNet/DeepChem",
        homepage="https://moleculenet.org/datasets-1",
        license_name="Dataset-specific upstream terms",
        license_url="https://moleculenet.org/",
        urls=("https://deepchemdata.s3-us-west-1.amazonaws.com/datasets/clintox.csv.gz",),
        file_format="csv",
        category="toxicity",
        tags=("toxicity", "clinical", "admet"),
    ),
    DatasetDefinition(
        dataset_id="sider",
        name="SIDER",
        description="Side effect labels curated from marketed drugs.",
        source="MoleculeNet/DeepChem",
        homepage="https://moleculenet.org/datasets-1",
        license_name="Dataset-specific upstream terms",
        license_url="https://moleculenet.org/",
        urls=("https://deepchemdata.s3-us-west-1.amazonaws.com/datasets/sider.csv.gz",),
        file_format="csv",
        category="safety",
        tags=("side_effects", "safety", "multitask"),
    ),
    DatasetDefinition(
        dataset_id="hiv",
        name="HIV",
        description="HIV replication inhibition activity labels.",
        source="MoleculeNet/DeepChem",
        homepage="https://moleculenet.org/datasets-1",
        license_name="Dataset-specific upstream terms",
        license_url="https://moleculenet.org/",
        urls=(
            "https://deepchemdata.s3-us-west-1.amazonaws.com/datasets/HIV.csv",
        ),
        file_format="csv",
        category="target_activity",
        tags=("hiv", "classification", "bioactivity"),
    ),
    DatasetDefinition(
        dataset_id="muv",
        name="MUV",
        description="Maximum unbiased validation benchmark for virtual screening tasks.",
        source="MoleculeNet/DeepChem",
        homepage="https://moleculenet.org/datasets-1",
        license_name="Dataset-specific upstream terms",
        license_url="https://moleculenet.org/",
        urls=("https://deepchemdata.s3-us-west-1.amazonaws.com/datasets/muv.csv.gz",),
        file_format="csv",
        category="virtual_screening",
        tags=("virtual_screening", "classification", "hts"),
    ),
    DatasetDefinition(
        dataset_id="esol",
        name="ESOL",
        description="Aqueous solubility regression benchmark.",
        source="MoleculeNet/DeepChem",
        homepage="https://moleculenet.org/datasets-1",
        license_name="Dataset-specific upstream terms",
        license_url="https://moleculenet.org/",
        urls=("https://deepchemdata.s3-us-west-1.amazonaws.com/datasets/delaney-processed.csv",),
        file_format="csv",
        category="physchem",
        tags=("solubility", "regression", "admet"),
    ),
    DatasetDefinition(
        dataset_id="freesolv",
        name="FreeSolv",
        description="Hydration free energy regression set for small molecules.",
        source="MoleculeNet/DeepChem",
        homepage="https://moleculenet.org/datasets-1",
        license_name="Dataset-specific upstream terms",
        license_url="https://moleculenet.org/",
        urls=("https://deepchemdata.s3-us-west-1.amazonaws.com/datasets/SAMPL.csv",),
        file_format="csv",
        category="physchem",
        tags=("solvation", "regression", "qm"),
    ),
    DatasetDefinition(
        dataset_id="lipophilicity",
        name="Lipophilicity",
        description="Octanol/water distribution coefficient (logD) regression dataset.",
        source="MoleculeNet/DeepChem",
        homepage="https://moleculenet.org/datasets-1",
        license_name="Dataset-specific upstream terms",
        license_url="https://moleculenet.org/",
        urls=(
            "https://deepchemdata.s3-us-west-1.amazonaws.com/datasets/Lipophilicity.csv",
            "https://deepchemdata.s3-us-west-1.amazonaws.com/datasets/lipo.csv",
        ),
        file_format="csv",
        category="physchem",
        tags=("logd", "regression", "admet"),
    ),
    DatasetDefinition(
        dataset_id="pcba",
        name="PCBA",
        description="PubChem BioAssay multitask virtual screening benchmark.",
        source="MoleculeNet/DeepChem",
        homepage="https://moleculenet.org/datasets-1",
        license_name="Dataset-specific upstream terms",
        license_url="https://moleculenet.org/",
        urls=("https://deepchemdata.s3-us-west-1.amazonaws.com/datasets/pcba.csv.gz",),
        file_format="csv",
        category="virtual_screening",
        tags=("pcba", "hts", "multitask"),
    ),
    DatasetDefinition(
        dataset_id="chembl_activity_ki_human",
        name="ChEMBL Human Ki Activities",
        description=(
            "ChEMBL activity records for human targets with Ki and pChEMBL "
            "values, useful for potency modeling."
        ),
        source="ChEMBL REST API",
        homepage="https://www.ebi.ac.uk/chembl/",
        license_name="ChEMBL data terms",
        license_url="https://www.ebi.ac.uk/chembl/ws",
        file_format="jsonl",
        category="target_activity",
        api=ApiDatasetConfig(
            endpoint="https://www.ebi.ac.uk/chembl/api/data/activity.json",
            params={
                "target_organism": "Homo sapiens",
                "standard_type": "Ki",
                "pchembl_value__isnull": "false",
            },
            pagination="chembl",
            items_path="activities",
            page_size_param="limit",
            page_size=1000,
            max_pages=40,
            max_rows=25_000,
        ),
        tags=("api", "chembl", "human", "ki", "potency"),
    ),
    DatasetDefinition(
        dataset_id="chembl_activity_ic50_human",
        name="ChEMBL Human IC50 Activities",
        description=(
            "ChEMBL activity records for human targets with IC50 and pChEMBL "
            "values, useful for activity modeling."
        ),
        source="ChEMBL REST API",
        homepage="https://www.ebi.ac.uk/chembl/",
        license_name="ChEMBL data terms",
        license_url="https://www.ebi.ac.uk/chembl/ws",
        file_format="jsonl",
        category="target_activity",
        api=ApiDatasetConfig(
            endpoint="https://www.ebi.ac.uk/chembl/api/data/activity.json",
            params={
                "target_organism": "Homo sapiens",
                "standard_type": "IC50",
                "pchembl_value__isnull": "false",
            },
            pagination="chembl",
            items_path="activities",
            page_size_param="limit",
            page_size=1000,
            max_pages=40,
            max_rows=25_000,
        ),
        tags=("api", "chembl", "human", "ic50", "potency"),
    ),
    DatasetDefinition(
        dataset_id="chembl_assays_binding_human",
        name="ChEMBL Human Binding Assays",
        description=(
            "Binding-type ChEMBL assays for human targets, useful for assay "
            "context and panel design."
        ),
        source="ChEMBL REST API",
        homepage="https://www.ebi.ac.uk/chembl/",
        license_name="ChEMBL data terms",
        license_url="https://www.ebi.ac.uk/chembl/ws",
        file_format="jsonl",
        category="assays",
        api=ApiDatasetConfig(
            endpoint="https://www.ebi.ac.uk/chembl/api/data/assay.json",
            params={
                "assay_type": "B",
                "target_organism": "Homo sapiens",
            },
            pagination="chembl",
            items_path="assays",
            page_size_param="limit",
            page_size=1000,
            max_pages=20,
            max_rows=12_000,
        ),
        tags=("api", "chembl", "human", "assays", "binding"),
    ),
    DatasetDefinition(
        dataset_id="chembl_targets_human_single_protein",
        name="ChEMBL Human Single-Protein Targets",
        description=(
            "ChEMBL target records restricted to human single proteins for "
            "target universe definition."
        ),
        source="ChEMBL REST API",
        homepage="https://www.ebi.ac.uk/chembl/",
        license_name="ChEMBL data terms",
        license_url="https://www.ebi.ac.uk/chembl/ws",
        file_format="jsonl",
        category="targets",
        api=ApiDatasetConfig(
            endpoint="https://www.ebi.ac.uk/chembl/api/data/target.json",
            params={
                "target_type": "SINGLE PROTEIN",
                "organism": "Homo sapiens",
            },
            pagination="chembl",
            items_path="targets",
            page_size_param="limit",
            page_size=1000,
            max_pages=10,
            max_rows=8_000,
        ),
        tags=("api", "chembl", "human", "targets"),
    ),
    DatasetDefinition(
        dataset_id="chembl_molecules_phase3plus",
        name="ChEMBL Molecules Phase 3+",
        description=(
            "ChEMBL molecules with max clinical phase >= 3, useful for "
            "late-stage scaffold and property priors."
        ),
        source="ChEMBL REST API",
        homepage="https://www.ebi.ac.uk/chembl/",
        license_name="ChEMBL data terms",
        license_url="https://www.ebi.ac.uk/chembl/ws",
        file_format="jsonl",
        category="compound_library",
        api=ApiDatasetConfig(
            endpoint="https://www.ebi.ac.uk/chembl/api/data/molecule.json",
            params={"max_phase__gte": "3"},
            pagination="chembl",
            items_path="molecules",
            page_size_param="limit",
            page_size=1000,
            max_pages=30,
            max_rows=20_000,
        ),
        tags=("api", "chembl", "clinical", "phase3plus"),
    ),
    DatasetDefinition(
        dataset_id="uniprot_human_reviewed",
        name="UniProt Human Reviewed Proteome",
        description=(
            "Reviewed human UniProtKB entries (Swiss-Prot) for baseline target "
            "annotation and sequence features."
        ),
        source="UniProt REST API",
        homepage="https://www.uniprot.org/help/api_queries",
        license_name="UniProt terms",
        license_url="https://www.uniprot.org/help/license",
        file_format="jsonl",
        category="targets",
        api=ApiDatasetConfig(
            endpoint="https://rest.uniprot.org/uniprotkb/search",
            params={
                "query": "organism_id:9606 AND reviewed:true",
                "format": "json",
            },
            pagination="link_header",
            items_path="results",
            page_size_param="size",
            page_size=500,
            max_pages=40,
            max_rows=20_000,
        ),
        tags=("api", "uniprot", "human", "reviewed", "targets"),
    ),
    DatasetDefinition(
        dataset_id="uniprot_human_kinases",
        name="UniProt Human Kinases",
        description=(
            "Reviewed human proteins annotated as kinases for kinase-focused "
            "target campaigns."
        ),
        source="UniProt REST API",
        homepage="https://www.uniprot.org/help/api_queries",
        license_name="UniProt terms",
        license_url="https://www.uniprot.org/help/license",
        file_format="jsonl",
        category="target_families",
        api=ApiDatasetConfig(
            endpoint="https://rest.uniprot.org/uniprotkb/search",
            params={
                "query": "organism_id:9606 AND reviewed:true AND keyword:Kinase",
                "format": "json",
            },
            pagination="link_header",
            items_path="results",
            page_size_param="size",
            page_size=500,
            max_pages=20,
            max_rows=8_000,
        ),
        tags=("api", "uniprot", "human", "kinase", "target_family"),
    ),
    DatasetDefinition(
        dataset_id="uniprot_human_gpcr",
        name="UniProt Human GPCRs",
        description=(
            "Reviewed human GPCR proteins for receptor-focused target "
            "selection and annotation."
        ),
        source="UniProt REST API",
        homepage="https://www.uniprot.org/help/api_queries",
        license_name="UniProt terms",
        license_url="https://www.uniprot.org/help/license",
        file_format="jsonl",
        category="target_families",
        api=ApiDatasetConfig(
            endpoint="https://rest.uniprot.org/uniprotkb/search",
            params={
                "query": (
                    "organism_id:9606 AND reviewed:true AND "
                    "keyword:\"G-protein coupled receptor\""
                ),
                "format": "json",
            },
            pagination="link_header",
            items_path="results",
            page_size_param="size",
            page_size=500,
            max_pages=20,
            max_rows=8_000,
        ),
        tags=("api", "uniprot", "human", "gpcr", "target_family"),
    ),
    DatasetDefinition(
        dataset_id="uniprot_human_ion_channels",
        name="UniProt Human Ion Channels",
        description=(
            "Reviewed human ion channel proteins for ion-channel-focused "
            "campaign planning."
        ),
        source="UniProt REST API",
        homepage="https://www.uniprot.org/help/api_queries",
        license_name="UniProt terms",
        license_url="https://www.uniprot.org/help/license",
        file_format="jsonl",
        category="target_families",
        api=ApiDatasetConfig(
            endpoint="https://rest.uniprot.org/uniprotkb/search",
            params={
                "query": "organism_id:9606 AND reviewed:true AND keyword:\"Ion channel\"",
                "format": "json",
            },
            pagination="link_header",
            items_path="results",
            page_size_param="size",
            page_size=500,
            max_pages=20,
            max_rows=8_000,
        ),
        tags=("api", "uniprot", "human", "ion_channel", "target_family"),
    ),
    DatasetDefinition(
        dataset_id="uniprot_human_transporters",
        name="UniProt Human Transporters",
        description=(
            "Reviewed human transporter proteins for transporter liability and "
            "uptake/efflux modeling contexts."
        ),
        source="UniProt REST API",
        homepage="https://www.uniprot.org/help/api_queries",
        license_name="UniProt terms",
        license_url="https://www.uniprot.org/help/license",
        file_format="jsonl",
        category="target_families",
        api=ApiDatasetConfig(
            endpoint="https://rest.uniprot.org/uniprotkb/search",
            params={
                "query": "organism_id:9606 AND reviewed:true AND keyword:Transport",
                "format": "json",
            },
            pagination="link_header",
            items_path="results",
            page_size_param="size",
            page_size=500,
            max_pages=20,
            max_rows=8_000,
        ),
        tags=("api", "uniprot", "human", "transporters", "target_family"),
    ),
]

DEFAULT_CATALOG = DatasetCatalog.from_entries(_DEFAULT_DATASETS)


def get_default_catalog() -> DatasetCatalog:
    """Return the built-in dataset catalog."""
    return DEFAULT_CATALOG
