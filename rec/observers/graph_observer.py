# SPDX-License-Identifier: MPL-2.0
# SPDX-FileCopyrightText: 2026 SECORO AG (secoro.uni-bremen.de)
# Author: Vamsi Kalagaturu

"""Shared direct-RDF implementation for REC storage backends."""

from datetime import UTC, datetime
from pathlib import Path

from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import PROV, RDF, XSD

from rec.observers.base import BaseObserver

REC = Namespace("https://secorolab.github.io/metamodels/rec#")
QUDT = Namespace("http://qudt.org/schema/qudt/")
QK = Namespace("http://qudt.org/vocab/quantitykind/")
UNIT = Namespace("http://qudt.org/vocab/unit/")
DCAT = Namespace("http://www.w3.org/ns/dcat#")
REC_CONTEXT = "https://secorolab.github.io/metamodels/rec/rec.json"
PREFIXES = {"prov": str(PROV), "rec": str(REC), "qudt": str(QUDT), "dcat": str(DCAT)}
CONTEXT = [REC_CONTEXT, {"prov": str(PROV), "qudt": str(QUDT), "dcat": str(DCAT)}]

HOST_FIELDS = {
    "hostname": REC.hostname,
    "os": REC.os,
    "python": REC.runtime,
    "cpu": REC.cpu,
}

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
        self._metric_steps = {}
        for prefix, namespace in PREFIXES.items():
            self.graph.bind(prefix, Namespace(namespace))

    @property
    def run(self):
        """Return the RDF subject for the current run."""
        return REC[f"activity/{_safe(self.run_id)}"]

    def query_active_run(self):
        """Return this observer's running ID, or ``None`` when it is not running."""
        return self.run_id if (self.run, RDF.type, REC.RunningRun) in self.graph else None

    def log_queued_run(self, run_id: str, queued_time: datetime):
        """Record ``run_id`` as a REC run queued at ``queued_time``."""
        self._set_run(run_id, REC.QueuedRun)
        self.graph.set((self.run, REC["queued-time"], _time(queued_time)))
        self._persist()

    def log_started_run(self, run_id: str, started_time: datetime, trigger=None, starter=None) -> str:
        """Record the start time, its optional trigger and starter, and return the run ID."""
        self._set_run(run_id, REC.RunningRun)
        self.graph.set((self.run, PROV.startedAtTime, _time(started_time)))
        if trigger is not None or starter is not None:
            self._start(trigger, starter, started_time)
        self._persist()
        return self.run_id

    def log_run_heartbeat(self, beat_time: datetime, result: object | None):
        """Record a heartbeat and its optional current result."""
        self.graph.set((self.run, REC["heartbeat-time"], _time(beat_time)))
        if result is not None:
            self.graph.set((self.run, REC.result, Literal(result)))
        self._persist()

    def log_cancelled_run(self, cancelled_time: datetime):
        """Record cancellation at ``cancelled_time``."""
        self._finish(REC.CancelledRun, cancelled_time)

    def log_completed_run(self, completed_time: datetime):
        """Record successful completion at ``completed_time``."""
        self._finish(REC.CompletedRun, completed_time)

    def log_interrupted_run(self, interrupted_time: datetime, fail_trace: str | None = None):
        """Record interruption at ``interrupted_time`` with its optional stacktrace."""
        self._finish(REC.InterruptedRun, interrupted_time, fail_trace)

    def log_failed_run(self, failed_time: datetime, fail_trace: str | None = None):
        """Record failure at ``failed_time`` with its optional stacktrace."""
        self._finish(REC.FailedRun, failed_time, fail_trace)

    def log_sources(self, sources):
        """Record source-file rows containing ``path`` and optional file metadata."""
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
        """Record repository rows with name, URL, and revision metadata."""
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
        """Record dependency rows with name and optional version metadata."""
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
        """Record the host's hostname, operating system, and runtime metadata."""
        host = REC[f"entity/{_safe(self.run_id)}/host"]
        self.graph.add((self.run, REC["host-info"], host))
        self.graph.add((host, RDF.type, REC.Host))
        for key, predicate in HOST_FIELDS.items():
            self._literal(host, predicate, (host_info or {}).get(key))
        self._persist()

    def add_agent(self, agent_id, agent_type):
        """Add a PROV agent to the run."""
        agent = _iri(agent_id)
        self.graph.add((self.run, REC.agents, agent))
        self.graph.add((agent, RDF.type, PROV.Agent))
        for kind in _rows(agent_type):
            self.graph.add((agent, RDF.type, _iri(kind)))
        self._persist()

    def add_activity(self, activity_id, activity_type, associated_with=None):
        """Add a PROV activity and optionally associate it with an agent."""
        activity = _iri(activity_id)
        self.graph.add((self.run, REC.activities, activity))
        self.graph.add((activity, RDF.type, PROV.Activity))
        for kind in _rows(activity_type):
            self.graph.add((activity, RDF.type, _iri(kind)))
        if associated_with:
            self.graph.add((activity, PROV.wasAssociatedWith, _iri(associated_with)))
        self._persist()

    def add_resource(self, path, used_by, used_at, label=None, sha256=None, size_bytes=None, archive_path=None):
        """Record a PROV entity used by an activity at a specific time."""
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
        """Record a PROV entity generated by an activity at a specific time."""
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
        """Record a dimensionless QUDT metric at ``step``, auto-numbered when omitted."""
        if step is None:
            step = self._next_step(metric_name)
        metric = REC[f"metric/{_safe(metric_name)}/{step}"]
        self.graph.add((self.run, REC.metrics, metric))
        self.graph.add((metric, RDF.type, REC.Metric))
        self.graph.add((metric, RDF.type, QUDT.Quantity))
        self._literal(metric, REC.label, metric_name)
        self.graph.set((metric, QUDT.hasQuantityKind, QK.Dimensionless))
        self.graph.set((metric, QUDT.value, Literal(value)))
        self.graph.set((metric, QUDT.unit, UNIT.UNITLESS))
        self.graph.set((metric, REC.step, Literal(step, datatype=XSD.integer)))
        self.graph.set((metric, PROV.generatedAtTime, _time(datetime.now(UTC))))
        self._persist()

    def _next_step(self, metric_name):
        """Return the next auto-increment step for ``metric_name``."""
        if metric_name not in self._metric_steps:
            # the JSON-LD context drops xsd:string, so match labels by value
            recorded = [
                int(step)
                for metric in self.graph.objects(self.run, REC.metrics)
                if str(self.graph.value(metric, REC.label)) == str(metric_name)
                for step in self.graph.objects(metric, REC.step)
            ]
            self._metric_steps[metric_name] = max(recorded) + 1 if recorded else 0
        step = self._metric_steps[metric_name]
        self._metric_steps[metric_name] = step + 1
        return step

    def close(self):
        """Flush the current graph to the storage backend."""
        self._persist()

    def serialize(self):
        """Return the current REC graph as compact JSON-LD."""
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

    def _start(self, trigger, starter, started_time):
        """Qualify the run's start with the entity that triggered it and the activity behind it."""
        start = REC[f"start/{_safe(self.run_id)}"]
        self.graph.set((self.run, PROV.qualifiedStart, start))
        self.graph.add((start, RDF.type, PROV.Start))
        self.graph.set((start, PROV.atTime, _time(started_time)))
        if trigger is not None:
            entity = _iri(trigger)
            self.graph.add((entity, RDF.type, PROV.Entity))
            self.graph.set((self.run, PROV.wasStartedBy, entity))
            self.graph.set((start, PROV.entity, entity))
        if starter is not None:
            activity = _iri(starter)
            self.graph.add((activity, RDF.type, PROV.Activity))
            self.graph.set((start, PROV.hadActivity, activity))

    def _finish(self, run_type, ended_at, fail_trace=None):
        self._set_run(None, run_type)
        self.graph.set((self.run, PROV.endedAtTime, _time(ended_at)))
        self._literal(self.run, REC["fail-trace"], fail_trace)
        self._persist()

    def _set_location(self, path):
        """Record where this run's archive lives, as a rec:PathLocation."""
        loc = REC[f"location/{_safe(self.run_id)}"]
        self.graph.set((self.run, PROV.atLocation, loc))
        self.graph.add((loc, RDF.type, PROV.Location))
        self.graph.add((loc, RDF.type, REC.PathLocation))
        self._literal(loc, REC.path, _location(path))

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
