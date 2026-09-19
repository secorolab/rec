# SPDX-License-Identifier: MPL-2.0
# SPDX-FileCopyrightText: 2026 SECORO AG (secoro.uni-bremen.de)
# Author: Vamsi Kalagaturu

import os
from pathlib import Path

from pyshacl import validate
from rdflib import Graph, Namespace, RDF, URIRef
from rdflib.namespace import DCAT, PROV, RDFS, SDO

from rec.observers import FileObserver
from rec.observers.graph_observer import OSLC_AUTO, PROV_EXT, REC_CONTEXT, REC_RUN, SPDX
from rec.run import Run

REC = Namespace("https://secorolab.github.io/metamodels/rec#")
AGENT = "https://example.org/agent/controller"
ACTIVITY = "https://example.org/activity/controller"


def recorded_graph(tmp_path, run_id="run-1", run_iri=None):
    """Create one completed REC graph with generic provenance."""
    path = tmp_path / "rec.jsonld"
    run = Run(observers=[FileObserver(path, run_iri=run_iri)], run_id=run_id)
    run._emit_started()
    run.add_agent(AGENT, "prov:SoftwareAgent", name="controller")
    run.add_activity(ACTIVITY, "prov:Activity", associated_with=AGENT)
    run.log_sources({"path": "model.ld.json"})
    run.add_resource("config.json", usage_activity=ACTIVITY)
    run.add_artefact("result.bin", gen_activity=ACTIVITY, sha256="deadbeef", size_bytes=4)
    run.log_repositories({"name": "motion-spec", "url": "git@github.com:secorolab/motion-spec.git", "commit": "0123abcd"})
    run.log_dependencies({"name": "rdflib", "version": "7.7.0"})
    run.log_scalar("frames", 1, step=0)
    run._emit_completed()
    return path, Graph().parse(path, format="json-ld")


def metamodels_dir():
    """Locate the checked-out metamodels used for conformance tests."""
    return Path(os.getenv("REC_METAMODELS_DIR", Path(__file__).resolve().parents[2] / "metamodels"))


def test_file_observer_writes_generic_provenance(tmp_path):
    path, graph = recorded_graph(tmp_path)
    run_node = REC_RUN["run-1"]
    assert (run_node, RDF.type, PROV.Activity) in graph
    assert (run_node, RDF.type, PROV_EXT.Execution) in graph
    assert graph.value(run_node, OSLC_AUTO.state) == OSLC_AUTO.complete
    assert graph.value(run_node, OSLC_AUTO.verdict) == OSLC_AUTO.passed
    # Written relative to the document, so parsing the file resolves it beside the file.
    assert URIRef(path.resolve().as_uri()) in set(graph.objects(run_node, PROV.atLocation))
    assert '"rec.jsonld"' in path.read_text()
    assert (None, PROV.qualifiedUsage, None) in graph
    assert (None, PROV.qualifiedGeneration, None) in graph
    assert not any("observation#" in str(term) or "/bdd#" in str(term) for triple in graph for term in triple)
    assert FileObserver(path).run_id == "run-1"


def test_prov_relations_replace_the_rec_collections(tmp_path):
    """The run's agents, activities, files and metrics hang off PROV, not off rec predicates."""
    _path, graph = recorded_graph(tmp_path)
    run_node = REC_RUN["run-1"]
    assert (run_node, PROV.wasAssociatedWith, URIRef(AGENT)) in graph
    assert (URIRef(ACTIVITY), PROV.wasInformedBy, run_node) in graph
    assert (URIRef(ACTIVITY), PROV.used, REC_RUN["run-1/entity/config.json"]) in graph
    assert (REC_RUN["run-1/entity/result.bin"], PROV.wasGeneratedBy, URIRef(ACTIVITY)) in graph
    assert (REC_RUN["run-1/metric/frames/0"], PROV.wasGeneratedBy, run_node) in graph
    for deleted in ("resources", "artefacts", "agents", "activities", "metrics", "repositories", "dependencies", "run-id", "label", "host-info", "sha256", "size-bytes", "path"):
        assert (None, REC[deleted], None) not in graph
    assert str(graph.value(REC_RUN["run-1/metric/frames/0"], RDFS.label)) == "frames"


def test_recording_a_file_hands_back_the_entity_it_minted(tmp_path):
    """The caller needs the entity IRI to relate the file to activities of its own."""
    run = Run(observers=[FileObserver(tmp_path / "rec.jsonld")], run_id="run-1")
    run._emit_started()
    assert run.add_resource("config.json") == REC_RUN["run-1/entity/config.json"]
    assert run.add_artefact("result.bin") == REC_RUN["run-1/entity/result.bin"]


def test_files_carry_a_checksum_and_size_in_dcat_form(tmp_path):
    _path, graph = recorded_graph(tmp_path)
    artefact = REC_RUN["run-1/entity/result.bin"]
    checksum = graph.value(artefact, SPDX.checksum)
    assert (checksum, RDF.type, SPDX.Checksum) in graph
    assert graph.value(checksum, SPDX.algorithm) == SPDX.checksumAlgorithm_sha256
    assert graph.value(checksum, SPDX.checksumValue).toPython() == bytes.fromhex("deadbeef")
    assert int(graph.value(artefact, DCAT.byteSize)) == 4
    assert graph.value(artefact, PROV.atLocation) == URIRef((tmp_path / "result.bin").resolve().as_uri())


def test_software_the_run_used_is_an_agent_it_is_associated_with(tmp_path):
    _path, graph = recorded_graph(tmp_path)
    run_node = REC_RUN["run-1"]
    repository = REC_RUN["run-1/repository/motion-spec"]
    dependency = REC_RUN["run-1/dependency/rdflib"]
    for agent in (repository, dependency):
        assert (run_node, PROV.wasAssociatedWith, agent) in graph
        assert (agent, RDF.type, PROV.SoftwareAgent) in graph
    assert str(graph.value(repository, SDO.name)) == "motion-spec"
    assert str(graph.value(repository, SDO.identifier)) == "0123abcd"
    assert graph.value(repository, SDO.codeRepository) == URIRef("https://github.com/secorolab/motion-spec.git")
    assert str(graph.value(dependency, SDO.softwareVersion)) == "7.7.0"


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
    assert graph.value(URIRef(external), OSLC_AUTO.state) == OSLC_AUTO.complete
    assert (REC_RUN["run-1"], OSLC_AUTO.state, None) not in graph
    reopened = FileObserver(path)
    assert reopened.run == URIRef(external)
    assert reopened.run_id == "run-1"


def test_file_observer_conforms_to_the_metamodels(tmp_path):
    path, _ = recorded_graph(tmp_path)
    assert REC_CONTEXT in path.read_text()
    graph = Graph().parse(path, format="json-ld")
    shapes = Graph()
    for name in ("prov.shacl.ttl", "prov-extension.shacl.ttl", "rec/rec.shacl.ttl"):
        shapes.parse(metamodels_dir() / name, format="turtle")
    conforms, _, report = validate(graph, shacl_graph=shapes, inference="rdfs")
    assert conforms, report
