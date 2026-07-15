# SPDX-License-Identifier: MPL-2.0
# SPDX-FileCopyrightText: 2026 SECORO AG (secoro.uni-bremen.de)
# Author: Vamsi Kalagaturu

"""Shared direct-RDF implementation for REC storage backends."""

from datetime import datetime
from pathlib import Path

from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import PROV, RDF, XSD

from rec.observers.base import BaseObserver

REC = Namespace("https://secorolab.github.io/metamodels/rec#")
QUDT = Namespace("http://qudt.org/schema/qudt/")
QK = Namespace("http://qudt.org/vocab/quantitykind/")
UNIT = Namespace("http://qudt.org/vocab/unit/")
DCAT = Namespace("http://www.w3.org/ns/dcat#")
CONTEXT = {"prov": str(PROV), "rec": str(REC), "qudt": str(QUDT), "dcat": str(DCAT)}

RUN_TYPES = (
    REC.QueuedRun,
    REC.RunningRun,
    REC.CompletedRun,
    REC.FailedRun,
    REC.InterruptedRun,
    REC.CancelledRun,
)


class GraphObserver(BaseObserver):
    """Build the backend-independent REC graph directly from run events."""

    def __init__(self, run_id):
        self.run_id = str(run_id)
        self.graph = Graph()
        for prefix, namespace in CONTEXT.items():
            self.graph.bind(prefix, Namespace(namespace))

    @property
    def run(self):
        return REC[f"activity/{_safe(self.run_id)}"]

    def query_active_run(self):
        return self.run_id if (self.run, RDF.type, REC.RunningRun) in self.graph else None

    def log_queued_run(self, run_id: str):
        self._set_run(run_id, REC.QueuedRun)
        self._persist()

    def log_started_run(self, run_id: str, started_time: datetime) -> str:
        self._set_run(run_id, REC.RunningRun)
        self.graph.set((self.run, PROV.startedAtTime, _time(started_time)))
        self._persist()
        return self.run_id

    def log_run_heartbeat(self, beat_time: datetime, result: object | None):
        self.graph.set((self.run, REC["heartbeat-time"], _time(beat_time)))
        if result is not None:
            self.graph.set((self.run, REC.result, Literal(result)))
        self._persist()

    def log_cancelled_run(self, cancelled_time: datetime):
        self._finish(REC.CancelledRun, cancelled_time)

    def log_completed_run(self, completed_time: datetime):
        self._finish(REC.CompletedRun, completed_time)

    def log_interrupted_run(self, interrupted_time: datetime):
        self._finish(REC.InterruptedRun, interrupted_time)

    def log_failed_run(self, failed_time: datetime):
        self._finish(REC.FailedRun, failed_time)

    def log_sources(self, sources):
        for source in _rows(sources):
            self._entity(
                source.get("path"),
                REC.resources,
                source.get("label"),
                source.get("sha256"),
                source.get("size_bytes"),
                source.get("archive_path") or source.get("archivePath"),
                REC.SourceFile,
            )
        self._persist()

    def log_repositories(self, repositories):
        for row in _rows(repositories):
            name = row.get("name") or row.get("path")
            if not name:
                continue
            repository = REC[f"repository/{_safe(name)}"]
            self.graph.add((self.run, REC.repositories, repository))
            self.graph.add((repository, RDF.type, REC.Repository))
            self._literal(repository, REC.label, row.get("name"))
            self._literal(repository, REC.url, row.get("url") or row.get("path"))
            self._literal(repository, REC.revision, row.get("commit") or row.get("revision") or row.get("tag"))
        self._persist()

    def log_dependencies(self, dependencies):
        for row in _rows(dependencies):
            name = row.get("name") or row.get("label")
            if not name:
                continue
            dependency = REC[f"dependency/{_safe(name)}"]
            self.graph.add((self.run, REC.dependencies, dependency))
            self.graph.add((dependency, RDF.type, REC.Dependency))
            self._literal(dependency, REC.label, name)
            self._literal(dependency, DCAT.version, row.get("version") or row.get("hasVersion"))
        self._persist()

    def log_host_info(self, host_info):
        host = REC[f"entity/{_safe(self.run_id)}/host"]
        self.graph.add((self.run, REC["host-info"], host))
        self.graph.add((host, RDF.type, REC.Host))
        for key, predicate in {"hostname": REC.hostname, "os": REC.os, "python": REC.runtime}.items():
            self._literal(host, predicate, (host_info or {}).get(key))
        self._persist()

    def add_agent(self, agent_id, agent_type):
        agent = _iri(agent_id)
        self.graph.add((self.run, REC.agents, agent))
        self.graph.add((agent, RDF.type, PROV.Agent))
        for kind in _rows(agent_type):
            self.graph.add((agent, RDF.type, _iri(kind)))
        self._persist()

    def add_activity(self, activity_id, activity_type, associated_with=None):
        activity = _iri(activity_id)
        self.graph.add((self.run, REC.activities, activity))
        self.graph.add((activity, RDF.type, PROV.Activity))
        for kind in _rows(activity_type):
            self.graph.add((activity, RDF.type, _iri(kind)))
        if associated_with:
            self.graph.add((activity, PROV.wasAssociatedWith, _iri(associated_with)))
        self._persist()

    def add_resource(self, path, used_by, used_at, label=None, sha256=None, size_bytes=None, archive_path=None):
        entity = self._entity(path, REC.resources, label, sha256, size_bytes, archive_path)
        activity = _iri(used_by or self.run)
        usage = REC[f"usage/{_safe(self.run_id)}/{_safe(activity)}/{_safe(entity)}"]
        self.graph.add((activity, PROV.used, entity))
        self.graph.add((activity, PROV.qualifiedUsage, usage))
        self.graph.add((usage, RDF.type, PROV.Usage))
        self.graph.add((usage, PROV.entity, entity))
        self.graph.set((usage, PROV.atTime, _time(used_at)))
        self._persist()

    def add_artefact(self, path, generated_by, generated_at, label=None, sha256=None, size_bytes=None, archive_path=None):
        entity = self._entity(path, REC.artefacts, label, sha256, size_bytes, archive_path)
        activity = _iri(generated_by or self.run)
        generation = REC[f"generation/{_safe(entity)}"]
        self.graph.add((entity, PROV.wasGeneratedBy, activity))
        self.graph.add((entity, PROV.qualifiedGeneration, generation))
        self.graph.add((generation, RDF.type, PROV.Generation))
        self.graph.add((generation, PROV.activity, activity))
        self.graph.set((generation, PROV.atTime, _time(generated_at)))
        self._persist()

    def log_scalar(self, metric_name, value, step=None):
        metric = REC[f"metric/{_safe(metric_name)}" + (f"/{step}" if step is not None else "")]
        self.graph.add((self.run, REC.metrics, metric))
        self.graph.add((metric, RDF.type, REC.Metric))
        self.graph.add((metric, RDF.type, QUDT.Quantity))
        self.graph.set((metric, QUDT.hasQuantityKind, QK.Dimensionless))
        self.graph.set((metric, QUDT.value, Literal(value)))
        self.graph.set((metric, QUDT.unit, UNIT.UNITLESS))
        if step is not None:
            self.graph.set((metric, REC.step, Literal(step, datatype=XSD.integer)))
        self._persist()

    def close(self):
        self._persist()

    def serialize(self):
        return self.graph.serialize(format="json-ld", context=CONTEXT, auto_compact=True)

    def _persist(self):
        raise NotImplementedError

    def _set_run(self, run_id, run_type):
        if run_id is not None:
            self.run_id = str(run_id)
        self.graph.add((self.run, RDF.type, PROV.Activity))
        for state_type in RUN_TYPES:
            self.graph.remove((self.run, RDF.type, state_type))
        self.graph.add((self.run, RDF.type, run_type))
        self.graph.set((self.run, REC["run-id"], Literal(self.run_id, datatype=XSD.string)))

    def _finish(self, run_type, ended_at):
        self._set_run(None, run_type)
        self.graph.set((self.run, PROV.endedAtTime, _time(ended_at)))
        self._persist()

    def _entity(self, path, collection, label, sha256, size_bytes, archive_path=None, extra_type=None):
        location = archive_path or path
        entity = REC[f"entity/{_safe(location)}"]
        self.graph.add((self.run, collection, entity))
        self.graph.add((entity, RDF.type, PROV.Entity))
        if extra_type is not None:
            self.graph.add((entity, RDF.type, extra_type))
        self._literal(entity, REC.sha256, sha256)
        self._literal(entity, REC["size-bytes"], size_bytes, XSD.integer)
        self._literal(entity, REC.label, label)
        if location:
            loc = REC[f"location/{_safe(entity)}"]
            self.graph.add((entity, PROV.atLocation, loc))
            self.graph.add((loc, RDF.type, PROV.Location))
            self.graph.add((loc, RDF.type, REC.PathLocation))
            self._literal(loc, REC.path, _location(location))
        return entity

    def _literal(self, subject, predicate, value, datatype=XSD.string):
        if value is not None:
            self.graph.set((subject, predicate, Literal(value, datatype=datatype)))


def _rows(value):
    return value if isinstance(value, list) else [value]


def _iri(value):
    text = str(value)
    if text.startswith("rec:"):
        return REC[text.removeprefix("rec:")]
    if text.startswith("prov:"):
        return PROV[text.removeprefix("prov:")]
    return URIRef(text)


def _safe(value):
    return str(value).replace(":", "_").replace("/", "_").replace("#", "_")


def _time(value):
    return Literal(value.isoformat() if hasattr(value, "isoformat") else value, datatype=XSD.dateTime)


def _location(value):
    path = Path(value)
    return path.as_uri() if path.is_absolute() else str(path)
