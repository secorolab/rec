# SPDX-License-Identifier: MPL-2.0
# SPDX-FileCopyrightText: 2026 SECORO AG (secoro.uni-bremen.de)

import os
from pathlib import Path

import pytest
from rdflib import Dataset, Namespace, URIRef
from rdflib.namespace import PROV, RDF, SOSA

from rec.consolidate import (
    BDD_GRAPH,
    INFERRED_GRAPH,
    REC_GRAPH,
    RUNTIME_GRAPH,
    ConsolidationError,
    consolidate_run,
)
from rec.observers import FileObserver
from rec.observers.graph_observer import OSLC_AUTO, PROV_EXT
from rec.run import Run

TIME = Namespace("http://www.w3.org/2006/time#")
RUN_ID = "run-1"
RUN_IRI = URIRef(f"https://secorolab.github.io/motion-spec/provenance/run/{RUN_ID}")

RUNTIME_TTL = """
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix prov-ext: <https://secorolab.github.io/metamodels/prov#> .
@prefix qkind: <http://qudt.org/vocab/quantitykind/> .
@prefix qudt: <http://qudt.org/schema/qudt/> .
@prefix sens: <https://secorolab.github.io/metamodels/robot/sensors#> .
@prefix time: <http://www.w3.org/2006/time#> .
@prefix unit: <http://qudt.org/vocab/unit/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix agent: <https://secorolab.github.io/motion-spec/provenance/agent/> .
@prefix ent: <https://secorolab.github.io/motion-spec/provenance/entity/run/run-1/> .
@prefix inst: <https://secorolab.github.io/motion-spec/runtime/instant/run-1/> .
@prefix run: <https://secorolab.github.io/motion-spec/provenance/run/> .
@prefix trs: <https://secorolab.github.io/motion-spec/runtime/trs/> .

run:%(run_id)s a prov-ext:Execution ;
    time:hasBeginning inst:0 ;
    time:hasEnd inst:2000 ;
    prov:used ent:model_jsonld ;
    prov:wasAssociatedWith agent:controller_process .

ent:model_jsonld a prov:Entity .
agent:controller_process a prov:Agent .

trs:%(run_id)s a time:TRS ;
    sens:update-rate <https://secorolab.github.io/motion-spec/runtime/quantity/run-1/tick_rate> .

<https://secorolab.github.io/motion-spec/runtime/quantity/run-1/tick_rate> a qudt:Quantity ;
    qudt:hasQuantityKind qkind:Frequency ;
    qudt:unit unit:HZ ;
    qudt:value 1000.0 .

inst:0 a time:Instant ;
    time:inTimePosition [ time:hasTRS trs:%(run_id)s ; time:numericPosition 0 ] .

inst:2000 a time:Instant ;
    time:inTimePosition [ time:hasTRS trs:%(run_id)s ; time:numericPosition 2000 ] .
"""

BDD_TTL = """
@prefix bdd: <https://secorolab.github.io/metamodels/acceptance-criteria/bdd#> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix sosa: <http://www.w3.org/ns/sosa/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix model: <https://secorolab.github.io/models/demo/> .

<https://secorolab.github.io/motion-spec/provenance/scenario-execution/s0> a bdd:ScenarioExecution ;
    prov:used model:variant-nominal ;
    prov:wasInformedBy <https://secorolab.github.io/motion-spec/provenance/run/run-1> .

[] a sosa:Observation ;
    sosa:hasFeatureOfInterest model:container-not-dropped ;
    sosa:hasSimpleResult "FALSE" ;
    sosa:resultTime "%(stamp)s"^^xsd:dateTime .
"""


def metamodels_dir():
    """Locate the checked-out metamodels the axioms and shapes come from."""
    return Path(os.getenv("REC_METAMODELS_DIR", Path(__file__).resolve().parents[2] / "metamodels"))


def build_run(tmp_path, run_id=RUN_ID, run_iri=RUN_IRI, runtime=True, bdd_stamp=None):
    """Write one archive-shaped run directory and return it."""
    run_dir = tmp_path / "runs" / run_id
    (run_dir / "runtime").mkdir(parents=True)
    if runtime:
        (run_dir / "runtime" / "runtime.ttl").write_text(RUNTIME_TTL % {"run_id": run_id})
    if bdd_stamp:
        (run_dir / "runtime" / "bdd-nominal.ttl").write_text(BDD_TTL % {"stamp": bdd_stamp})
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
    assert names >= {REC_GRAPH, RUNTIME_GRAPH, INFERRED_GRAPH}
    # The compiled layer is the derived design graph; no prospective document is consolidated.
    assert URIRef("urn:plan") not in names
    assert len(dataset.graph(BDD_GRAPH)) == 0


def test_one_run_node_carries_both_the_runtime_and_the_lifecycle(tmp_path):
    dataset = consolidated(build_run(tmp_path))
    assert (RUN_IRI, RDF.type, PROV_EXT.Execution) in dataset.graph(RUNTIME_GRAPH)
    assert dataset.graph(REC_GRAPH).value(RUN_IRI, OSLC_AUTO.state) == OSLC_AUTO.complete


def test_a_run_without_a_runtime_record_consolidates_from_its_lifecycle(tmp_path):
    dataset = consolidated(build_run(tmp_path, runtime=False))
    assert len(dataset.graph(RUNTIME_GRAPH)) == 0
    assert dataset.graph(REC_GRAPH).value(RUN_IRI, OSLC_AUTO.verdict) == OSLC_AUTO.passed


def test_subclass_entailment_types_the_run_as_an_activity(tmp_path):
    dataset = consolidated(build_run(tmp_path))
    runtime, inferred = dataset.graph(RUNTIME_GRAPH), dataset.graph(INFERRED_GRAPH)
    assert (RUN_IRI, RDF.type, PROV.Activity) not in runtime
    assert (RUN_IRI, RDF.type, PROV.Activity) in inferred


def test_acceptance_observations_gain_the_runs_tick_position(tmp_path):
    # A sim clock counts from the simulator's epoch, so 1.4 s of sim time is 1400 ticks at 1 kHz.
    dataset = consolidated(build_run(tmp_path, bdd_stamp="1970-01-01T00:00:01.400000+00:00"))
    inferred = dataset.graph(INFERRED_GRAPH)
    instant = next(inferred.objects(None, SOSA.phenomenonTime))
    position = inferred.value(instant, TIME.inTimePosition)
    assert int(inferred.value(position, TIME.numericPosition)) == 1400
    assert inferred.value(position, TIME.hasTRS) is not None


def test_a_wall_clock_observation_is_left_unpositioned(tmp_path):
    dataset = consolidated(build_run(tmp_path, bdd_stamp="2026-08-25T16:00:01.400000+00:00"))
    assert (None, SOSA.phenomenonTime, None) not in dataset.graph(INFERRED_GRAPH)


def test_an_old_vocabulary_runtime_graph_is_refused_with_the_migration_hint(tmp_path):
    run_dir = build_run(tmp_path)
    (run_dir / "runtime" / "runtime.ttl").write_text(
        "@prefix prov: <http://www.w3.org/ns/prov#> .\n"
        "<https://secorolab.github.io/motion-spec/runtime/run/run-1> a prov:Entity .\n"
    )
    with pytest.raises(ConsolidationError, match="--recover-runtime-ttl"):
        consolidate_run(run_dir, metamodels_dir=metamodels_dir())
    assert not (run_dir / "provenance.trig").exists()


def test_a_runtime_naming_another_run_is_reported_not_patched(tmp_path):
    run_dir = build_run(tmp_path, run_iri=URIRef("https://secorolab.github.io/rec/run/run-1"))
    with pytest.raises(ConsolidationError) as failure:
        consolidate_run(run_dir, metamodels_dir=metamodels_dir())
    message = str(failure.value)
    assert str(RUN_IRI) in message and "https://secorolab.github.io/rec/run/run-1" in message
    assert not (run_dir / "provenance.trig").exists()


def test_a_run_without_an_agent_fails_validation(tmp_path):
    run_dir = build_run(tmp_path, runtime=False)
    graph = Dataset(default_union=True)
    graph.parse(run_dir / "rec.ld.json", format="json-ld")
    graph.remove((None, PROV.wasAssociatedWith, None))
    (run_dir / "rec.ld.json").write_text(graph.serialize(format="json-ld"))
    with pytest.raises(ConsolidationError, match="SHACL"):
        consolidate_run(run_dir, metamodels_dir=metamodels_dir())
