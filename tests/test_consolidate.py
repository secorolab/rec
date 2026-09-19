# SPDX-License-Identifier: MPL-2.0
# SPDX-FileCopyrightText: 2026 SECORO AG (secoro.uni-bremen.de)

import json
import os
from pathlib import Path

import pytest
from rdflib import Dataset, URIRef
from rdflib.namespace import PROV, RDF

from rec.consolidate import (
    GENERATION_GRAPH,
    INFERRED_GRAPH,
    REC_GRAPH,
    ConsolidationError,
    consolidate_dataset,
    consolidate_run,
)
from rec.observers import FileObserver
from rec.observers.graph_observer import OSLC_AUTO, PROV_EXT
from rec.run import Run

RUN_ID = "run-1"
RUN_IRI = URIRef(f"https://secorolab.github.io/motion-spec/provenance/run/{RUN_ID}")
MSPROV = "https://secorolab.github.io/motion-spec/provenance/"
DSLPROV = "https://secorolab.github.io/motion-spec-dsl/provenance/"

GENERATION_DOCUMENT = {
    "@context": [
        "https://secorolab.github.io/metamodels/prov.json",
        "https://secorolab.github.io/metamodels/prov-extension.json",
        {"msprov": MSPROV, "dslprov": DSLPROV},
    ],
    "@graph": [
        {
            "@id": "dslprov:document/dsl",
            "@graph": [
                {"@id": "msprov:entity/source/model.robmot", "@type": "prov:Entity"},
                {
                    "@id": "dslprov:activity/jsonld_generation/model",
                    "@type": ["prov:Activity", "prov-ext:Transformation"],
                    "used": "msprov:entity/source/model.robmot",
                },
            ],
        },
        {
            "@id": "msprov:document/motion-spec",
            "@graph": [
                {
                    "@id": "msprov:activity/code_generation",
                    "@type": ["prov:Activity", "prov-ext:Transformation"],
                }
            ],
        },
    ],
}


def metamodels_dir():
    """Locate the checked-out metamodels the axioms and shapes come from."""
    return Path(os.getenv("REC_METAMODELS_DIR", Path(__file__).resolve().parents[2] / "metamodels"))


def build_run(tmp_path, run_id=RUN_ID, run_iri=RUN_IRI, generation=False):
    """Write one archive-shaped run directory and return it."""
    run_dir = tmp_path / "runs" / run_id
    run_dir.mkdir(parents=True)
    if generation:
        generated = tmp_path / "generated"
        generated.mkdir(parents=True, exist_ok=True)
        (generated / "provenance.ld.json").write_text(json.dumps(GENERATION_DOCUMENT))
    observer = FileObserver(run_dir / "rec.ld.json", run_iri=run_iri)
    run = Run(observers=[observer], run_id=run_id)
    run._emit_started()
    run.add_agent("https://example.org/agent/operator", "prov:Person")
    run.log_sources({"path": "model.ld.json"})
    run._emit_completed()
    observer.close()
    return run_dir


def consolidated(run_dir):
    dataset = Dataset()
    dataset.parse(consolidate_run(run_dir, metamodels_dir=metamodels_dir()), format="trig")
    return dataset


def test_consolidation_writes_the_named_graphs(tmp_path):
    run_dir = build_run(tmp_path)
    path = consolidate_run(run_dir, metamodels_dir=metamodels_dir())
    assert path == run_dir / "provenance.trig"
    dataset = Dataset()
    dataset.parse(path, format="trig")
    names = {graph.identifier for graph in dataset.graphs()}
    assert names >= {REC_GRAPH, INFERRED_GRAPH}
    # The compiled layer is the derived design graph; no prospective document is consolidated.
    assert URIRef("urn:plan") not in names
    assert URIRef("urn:runtime") not in names
    assert URIRef("urn:bdd") not in names


def test_the_run_node_carries_its_lifecycle(tmp_path):
    dataset = consolidated(build_run(tmp_path))
    rec = dataset.graph(REC_GRAPH)
    assert (RUN_IRI, RDF.type, PROV_EXT.Execution) in rec
    assert rec.value(RUN_IRI, OSLC_AUTO.state) == OSLC_AUTO.complete
    assert rec.value(RUN_IRI, OSLC_AUTO.verdict) == OSLC_AUTO.passed


def test_subclass_entailment_types_the_run_as_an_activity(tmp_path):
    dataset = consolidated(build_run(tmp_path))
    assert (RUN_IRI, RDF.type, PROV.Activity) in dataset.graph(INFERRED_GRAPH)


def test_every_named_graph_of_the_generation_document_lands_in_one_graph(tmp_path):
    dataset = consolidated(build_run(tmp_path, generation=True))
    generation = dataset.graph(GENERATION_GRAPH)
    dsl = URIRef(f"{DSLPROV}activity/jsonld_generation/model")
    assert (dsl, PROV.used, URIRef(f"{MSPROV}entity/source/model.robmot")) in generation
    assert (URIRef(f"{MSPROV}activity/code_generation"), RDF.type, PROV_EXT.Transformation) in generation


def test_the_generation_transformations_entail_prov_activity(tmp_path):
    dataset = consolidated(build_run(tmp_path, generation=True))
    dsl = URIRef(f"{DSLPROV}activity/jsonld_generation/model")
    assert (dsl, RDF.type, PROV.Activity) in dataset.graph(INFERRED_GRAPH)


def test_consolidating_without_writing_returns_the_dataset(tmp_path):
    run_dir = build_run(tmp_path)
    dataset = consolidate_dataset(run_dir, metamodels_dir=metamodels_dir())
    assert dataset.graph(REC_GRAPH).value(RUN_IRI, OSLC_AUTO.state) == OSLC_AUTO.complete
    assert not (run_dir / "provenance.trig").exists()


def test_a_run_without_a_lifecycle_document_is_refused(tmp_path):
    run_dir = build_run(tmp_path)
    (run_dir / "rec.ld.json").unlink()
    with pytest.raises(ConsolidationError, match="nothing to consolidate"):
        consolidate_run(run_dir, metamodels_dir=metamodels_dir())


def test_a_run_without_an_agent_fails_validation(tmp_path):
    run_dir = build_run(tmp_path)
    graph = Dataset(default_union=True)
    graph.parse(run_dir / "rec.ld.json", format="json-ld")
    graph.remove((None, PROV.wasAssociatedWith, None))
    (run_dir / "rec.ld.json").write_text(graph.serialize(format="json-ld"))
    with pytest.raises(ConsolidationError, match="SHACL"):
        consolidate_run(run_dir, metamodels_dir=metamodels_dir())
