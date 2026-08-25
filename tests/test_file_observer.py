# SPDX-License-Identifier: MPL-2.0
# SPDX-FileCopyrightText: 2026 SECORO AG (secoro.uni-bremen.de)
# Author: Vamsi Kalagaturu

import os
from pathlib import Path

from pyshacl import validate
from rdflib import Graph, Namespace, RDF, URIRef
from rdflib.namespace import PROV, RDFS

from rec.observers import FileObserver
from rec.observers.graph_observer import REC_CONTEXT, REC_RUN
from rec.run import Run

REC = Namespace("https://secorolab.github.io/metamodels/rec#")
AGENT = "https://example.org/agent/controller"
ACTIVITY = "https://example.org/activity/controller"


def recorded_graph(tmp_path, run_id="run-1", run_iri=None):
    """Create one completed REC graph with generic provenance."""
    path = tmp_path / "rec.jsonld"
    run = Run(observers=[FileObserver(path, run_iri=run_iri)], run_id=run_id)
    run._emit_started()
    run.add_agent(AGENT, "prov:SoftwareAgent")
    run.add_activity(ACTIVITY, "prov:Activity", associated_with=AGENT)
    run.add_resource("config.json", usage_activity=ACTIVITY)
    run.add_artefact("result.bin", gen_activity=ACTIVITY)
    run.log_scalar("frames", 1, step=0)
    run._emit_completed()
    return path, Graph().parse(path, format="json-ld")


def rec_metamodel_dir():
    """Locate the checked-out REC metamodel used for conformance tests."""
    return Path(os.getenv("REC_METAMODELS_DIR", Path(__file__).resolve().parents[2] / "metamodels")) / "rec"


def test_file_observer_writes_generic_provenance(tmp_path):
    path, graph = recorded_graph(tmp_path)
    run_node = REC_RUN["run-1"]
    assert (run_node, RDF.type, PROV.Activity) in graph
    assert (run_node, RDF.type, REC.CompletedRun) in graph
    location = graph.value(run_node, PROV.atLocation)
    assert (location, RDF.type, REC.PathLocation) in graph
    assert str(graph.value(location, REC.path)) == "rec.jsonld"
    assert (None, PROV.qualifiedUsage, None) in graph
    assert (None, PROV.qualifiedGeneration, None) in graph
    assert not any("observation#" in str(term) or "/bdd#" in str(term) for triple in graph for term in triple)
    assert FileObserver(path).run_id == "run-1"


def test_prov_relations_replace_the_rec_collections(tmp_path):
    """The run's agents, activities and files hang off PROV, not off rec predicates."""
    _path, graph = recorded_graph(tmp_path)
    run_node = REC_RUN["run-1"]
    assert (run_node, PROV.wasAssociatedWith, URIRef(AGENT)) in graph
    assert (URIRef(ACTIVITY), PROV.wasInformedBy, run_node) in graph
    assert (URIRef(ACTIVITY), PROV.used, REC_RUN["run-1/entity/config.json"]) in graph
    assert (
        REC_RUN["run-1/entity/result.bin"],
        PROV.wasGeneratedBy,
        URIRef(ACTIVITY),
    ) in graph
    for deleted in (
        "resources",
        "artefacts",
        "agents",
        "activities",
        "label",
        "url",
        "revision",
    ):
        assert (None, REC[deleted], None) not in graph
    assert str(graph.value(REC_RUN["run-1/metric/frames/0"], RDFS.label)) == "frames"


def test_instance_iris_are_run_scoped(tmp_path):
    """Two runs of the same work share no entity, metric or usage node."""
    first = _minted(recorded_graph(tmp_path / "a")[1])
    second = _minted(recorded_graph(tmp_path / "b", run_id="run-2")[1])
    assert first and not first & second


def _minted(graph):
    """The nodes rec itself minted -- never in the metamodel namespace, always run-scoped."""
    assert not any(str(subject).startswith(str(REC)) for subject in graph.subjects())
    return {subject for subject in graph.subjects() if str(subject).startswith(str(REC_RUN))}


def test_injected_run_iri_is_the_run_node(tmp_path):
    """A caller that already minted the run elsewhere gets that IRI, and reopening keeps it."""
    external = "https://secorolab.github.io/motion-spec/provenance/run/run-1"
    path, graph = recorded_graph(tmp_path, run_iri=external)
    assert (URIRef(external), RDF.type, REC.CompletedRun) in graph
    assert (REC_RUN["run-1"], RDF.type, REC.CompletedRun) not in graph
    assert FileObserver(path).run == URIRef(external)


def test_file_observer_conforms_to_rec_metamodel(tmp_path):
    path, _ = recorded_graph(tmp_path)
    metamodel = rec_metamodel_dir()
    assert REC_CONTEXT in path.read_text()
    graph = Graph().parse(path, format="json-ld")
    conforms, _, report = validate(graph, shacl_graph=Graph().parse(metamodel / "rec.shacl.ttl", format="turtle"))
    assert conforms, report
