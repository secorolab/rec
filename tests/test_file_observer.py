# SPDX-License-Identifier: MPL-2.0
# SPDX-FileCopyrightText: 2026 SECORO AG (secoro.uni-bremen.de)
# Author: Vamsi Kalagaturu

from rdflib import Graph, Namespace, RDF
from rdflib.namespace import PROV

from rec.observers import FileObserver
from rec.run import Run

REC = Namespace("https://secorolab.github.io/metamodels/rec#")


def test_file_observer_writes_generic_provenance(tmp_path):
    path = tmp_path / "rec.jsonld"
    run = Run(observers=[FileObserver(path)], run_id="run-1")
    run._emit_started()
    run.add_agent("rec:agent/controller", "prov:SoftwareAgent")
    run.add_activity("rec:activity/controller", "prov:Activity", associated_with="rec:agent/controller")
    run.add_resource("config.json", usage_activity="rec:activity/controller")
    run.add_artefact("result.bin", gen_activity="rec:activity/controller")
    run.log_scalar("frames", 1, step=0)
    run._emit_completed()

    graph = Graph().parse(path, format="json-ld")
    run_node = REC["activity/run-1"]
    assert (run_node, RDF.type, PROV.Activity) in graph
    assert (run_node, RDF.type, REC.CompletedRun) in graph
    file_id = graph.value(run_node, REC["file-id"])
    assert file_id
    assert (run_node, REC.status, None) not in graph
    assert (None, PROV.qualifiedUsage, None) in graph
    assert (None, PROV.qualifiedGeneration, None) in graph
    assert not any("execution-context#" in str(term) or "observation#" in str(term) or "/bdd#" in str(term) for triple in graph for term in triple)
    assert FileObserver(path).file_id == str(file_id)
