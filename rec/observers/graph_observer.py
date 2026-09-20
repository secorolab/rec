# SPDX-License-Identifier: MPL-2.0
# SPDX-FileCopyrightText: 2026 SECORO AG (secoro.uni-bremen.de)
# Author: Vamsi Kalagaturu

"""Shared direct-RDF implementation for REC storage backends."""

import functools
import json
import re
import threading
from datetime import UTC, datetime
from pathlib import Path

from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCAT, PROV, RDF, RDFS, SDO, XSD, split_uri

from rec.observers.base import BaseObserver

REC = Namespace("https://secorolab.github.io/metamodels/rec#")
PROV_EXT = Namespace("https://secorolab.github.io/metamodels/prov#")
OSLC_AUTO = Namespace("http://open-services.net/ns/auto#")
SPDX = Namespace("http://spdx.org/rdf/terms#")
# Instance data never lives in the metamodel namespace, and every node is scoped by run id so
# two runs union without collapsing onto one another's entities and metrics.
REC_RUN = Namespace("https://secorolab.github.io/rec/run/")
QUDT = Namespace("http://qudt.org/schema/qudt/")
QK = Namespace("http://qudt.org/vocab/quantitykind/")
UNIT = Namespace("http://qudt.org/vocab/unit/")
REC_CONTEXT = "https://secorolab.github.io/metamodels/rec/rec.json"
UPSTREAM = {
    "prov": str(PROV),
    "prov-ext": str(PROV_EXT),
    "oslc_auto": str(OSLC_AUTO),
    "qudt": str(QUDT),
    "dcat": str(DCAT),
    "rdfs": str(RDFS),
    "schema": str(SDO),
    "spdx": str(SPDX),
}
PREFIXES = {"rec": str(REC), **UPSTREAM}
CONTEXT = [REC_CONTEXT, PREFIXES]

HOST_FIELDS = {
    "hostname": SDO.identifier,
    "os": REC.os,
    "python": REC.runtime,
    "cpu": REC.cpu,
}

# OSLC Automation: where a run is, and once complete, how it turned out.
QUEUED = (OSLC_AUTO.queued, OSLC_AUTO.unavailable)
IN_PROGRESS = (OSLC_AUTO.inProgress, OSLC_AUTO.unavailable)
COMPLETED = (OSLC_AUTO.complete, OSLC_AUTO.passed)
FAILED = (OSLC_AUTO.complete, OSLC_AUTO.failed)
INTERRUPTED = (OSLC_AUTO.complete, OSLC_AUTO.error)
CANCELING = (OSLC_AUTO.canceling, OSLC_AUTO.unavailable)
CANCELLED = (OSLC_AUTO.canceled, OSLC_AUTO.unavailable)
LIVE = (OSLC_AUTO.queued, OSLC_AUTO.inProgress)


def locked(method):
    """Serialise graph changes and writes: the heartbeat thread shares the graph with the run."""

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)

    return wrapper


class GraphObserver(BaseObserver):
    """Build the backend-independent REC graph directly from run events."""

    def __init__(self, run_id, run_iri=None):
        self.run_id = str(run_id)
        # A caller that already minted the run elsewhere passes its IRI, so its own graph and
        # this document describe one node instead of two.
        self.run_iri = URIRef(run_iri) if run_iri else None
        self.graph = Graph()
        self._lock = threading.RLock()
        self._metric_steps = {}
        self._archive = None
        for prefix, namespace in PREFIXES.items():
            self.graph.bind(prefix, Namespace(namespace))

    @property
    def run(self):
        """Return the RDF subject for the current run."""
        return self.run_iri or REC_RUN[_slug(self.run_id)]

    def _scoped(self, *segments):
        """Return an instance IRI for this run's ``segments``."""
        return REC_RUN[f"{_slug(self.run_id)}/" + "/".join(_slug(segment) for segment in segments)]

    @locked
    def log_queued_run(self, run_id: str, queued_time: datetime):
        """Record ``run_id`` as a REC run queued at ``queued_time``."""
        self._set_run(run_id, QUEUED)
        self.graph.set((self.run, REC["queued-time"], _time(queued_time)))
        self._persist()

    @locked
    def log_started_run(self, run_id: str, started_time: datetime, trigger=None, starter=None) -> str:
        """Record the start time, its optional trigger and starter, and return the run ID."""
        self._set_run(run_id, IN_PROGRESS)
        self.graph.set((self.run, PROV.startedAtTime, _time(started_time)))
        if trigger is not None or starter is not None:
            self._start(trigger, starter, started_time)
        self._persist()
        return self.run_id

    @locked
    def log_run_heartbeat(self, beat_time: datetime, result: object | None):
        """Record the latest heartbeat and the run's result so far."""
        self.graph.set((self.run, REC["heartbeat-time"], _time(beat_time)))
        if result is not None:
            self.graph.set((self.run, REC.result, Literal(result)))
        self._persist()

    @locked
    def log_cancelled_run(self, cancelled_time: datetime):
        """Record cancellation at ``cancelled_time``."""
        self._finish(CANCELLED, cancelled_time)

    @locked
    def request_cancel(self):
        """Ask the run this observer is bound to, wherever it runs, to stop."""
        state = self._state()
        if state in LIVE:
            self._set_run(None, CANCELING)
            self._persist()
        elif state != OSLC_AUTO.canceling:
            raise RuntimeError(f"run {self.run_id} is not queued or running, its state is {state}")

    @locked
    def cancel_requested(self):
        """Whether this run was asked to stop, here or through the store."""
        return OSLC_AUTO.canceling in (self._state(), self._stored_state())

    @locked
    def log_completed_run(self, completed_time: datetime):
        """Record successful completion at ``completed_time``."""
        self._finish(COMPLETED, completed_time)

    @locked
    def log_interrupted_run(self, interrupted_time: datetime, fail_trace: str | None = None):
        """Record interruption at ``interrupted_time`` with its optional stacktrace."""
        self._finish(INTERRUPTED, interrupted_time, fail_trace)

    @locked
    def log_failed_run(self, failed_time: datetime, fail_trace: str | None = None):
        """Record failure at ``failed_time`` with its optional stacktrace."""
        self._finish(FAILED, failed_time, fail_trace)

    @locked
    def add_software(self, name, version=None, commit=None, repository=None):
        """One software package the run ran with, a software agent described with schema.org terms."""
        agent = self._scoped("software", name)
        self.graph.add((self.run, PROV.wasAssociatedWith, agent))
        self.graph.add((agent, RDF.type, PROV.SoftwareAgent))
        self.graph.add((agent, RDF.type, PROV.Agent))
        self._literal(agent, SDO.name, name)
        self._literal(agent, SDO.softwareVersion, version)
        self._literal(agent, SDO.identifier, commit)
        url = _url(repository)
        if url is not None:
            self.graph.set((agent, SDO.codeRepository, url))
        self._persist()

    @locked
    def log_host_info(self, host_info):
        """Record the host the run took place on, as one of its locations."""
        host = self._scoped("host")
        self.graph.add((self.run, PROV.atLocation, host))
        self.graph.add((host, RDF.type, REC.Host))
        for key, predicate in HOST_FIELDS.items():
            self._literal(host, predicate, (host_info or {}).get(key))
        self._persist()

    @locked
    def add_agent(self, agent_id, agent_type, name=None):
        """Add a PROV agent to the run; a software agent needs its ``name``."""
        agent = _iri(agent_id)
        self.graph.add((self.run, PROV.wasAssociatedWith, agent))
        self.graph.add((agent, RDF.type, PROV.Agent))
        for kind in _rows(agent_type):
            self.graph.add((agent, RDF.type, _iri(kind)))
        self._literal(agent, SDO.name, name)
        self._persist()

    @locked
    def add_activity(self, activity_id, activity_type, associated_with=None):
        """Add a PROV activity and optionally associate it with an agent."""
        activity = _iri(activity_id)
        self.graph.add((activity, PROV.wasInformedBy, self.run))
        self.graph.add((activity, RDF.type, PROV.Activity))
        for kind in _rows(activity_type):
            self.graph.add((activity, RDF.type, _iri(kind)))
        if associated_with:
            self.graph.add((activity, PROV.wasAssociatedWith, _iri(associated_with)))
        self._persist()

    @locked
    def add_resource(self, path, used_by, used_at, label=None, sha256=None, size_bytes=None, archive_path=None):
        """Record a PROV entity used by an activity at a specific time, and return its IRI."""
        entity, slug = self._entity(path, label, sha256, size_bytes, archive_path)
        activity = _iri(used_by or self.run)
        usage = self._scoped("usage", local_name(activity), slug)
        self.graph.add((activity, PROV.used, entity))
        self.graph.add((activity, PROV.qualifiedUsage, usage))
        self.graph.add((usage, RDF.type, PROV.Usage))
        self.graph.add((usage, PROV.entity, entity))
        self.graph.set((usage, PROV.atTime, _time(used_at)))
        self._persist()
        return entity

    @locked
    def add_artefact(self, path, generated_by, generated_at, label=None, sha256=None, size_bytes=None, archive_path=None):
        """Record a PROV entity generated by an activity at a specific time, and return its IRI."""
        entity, slug = self._entity(path, label, sha256, size_bytes, archive_path)
        activity = _iri(generated_by or self.run)
        generation = self._scoped("generation", slug)
        self.graph.add((entity, PROV.wasGeneratedBy, activity))
        self.graph.add((entity, PROV.qualifiedGeneration, generation))
        self.graph.add((generation, RDF.type, PROV.Generation))
        self.graph.add((generation, PROV.activity, activity))
        self.graph.set((generation, PROV.atTime, _time(generated_at)))
        self._persist()
        return entity

    @locked
    def log_scalar(self, metric_name, value, step=None):
        """Record a dimensionless QUDT metric at ``step``, auto-numbered when omitted."""
        if step is None:
            step = self._next_step(metric_name)
        metric = self._scoped("metric", metric_name, step)
        self.graph.add((metric, RDF.type, REC.Metric))
        self.graph.add((metric, RDF.type, PROV.Entity))
        self.graph.add((metric, PROV.wasGeneratedBy, self.run))
        self._literal(metric, RDFS.label, metric_name)
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
                for metric in self.graph.subjects(PROV.wasGeneratedBy, self.run)
                if (metric, RDF.type, REC.Metric) in self.graph
                and str(self.graph.value(metric, RDFS.label)) == str(metric_name)
                for step in self.graph.objects(metric, REC.step)
            ]
            self._metric_steps[metric_name] = max(recorded) + 1 if recorded else 0
        step = self._metric_steps[metric_name]
        self._metric_steps[metric_name] = step + 1
        return step

    @locked
    def close(self):
        """Flush the current graph to the storage backend."""
        self._persist()

    def _persist(self):
        """Write the graph, first adopting a cancel request another writer left in the store."""
        if self._state() in LIVE and self._stored_state() == OSLC_AUTO.canceling:
            self._set_run(None, CANCELING)
        self._write()

    def _write(self):
        raise NotImplementedError

    def _stored_state(self):
        """The run's ``oslc_auto:state`` as the store has it now, or None."""
        raise NotImplementedError

    def _state(self):
        return self.graph.value(self.run, OSLC_AUTO.state)

    def _set_run(self, run_id, lifecycle):
        if run_id is not None:
            self.run_id = str(run_id)
        state, verdict = lifecycle
        self.graph.add((self.run, RDF.type, PROV.Activity))
        self.graph.add((self.run, RDF.type, PROV_EXT.Execution))
        self.graph.set((self.run, OSLC_AUTO.state, state))
        self.graph.set((self.run, OSLC_AUTO.verdict, verdict))

    def _start(self, trigger, starter, started_time):
        """Qualify the run's start with the entity that triggered it and the activity behind it."""
        start = self._scoped("start")
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

    def _finish(self, lifecycle, ended_at, fail_trace=None):
        self._set_run(None, lifecycle)
        self.graph.set((self.run, PROV.endedAtTime, _time(ended_at)))
        self._literal(self.run, REC["fail-trace"], fail_trace)
        self._persist()

    def _set_location(self, path):
        """Record where this run's archive lives; a relative path is relative to the document.

        The host is the run's other location, so only the previous archive location is replaced.
        """
        archive = _location_iri(path)
        if self._archive is not None and self._archive != archive:
            self.graph.remove((self.run, PROV.atLocation, self._archive))
        self._archive = archive
        self.graph.add((self.run, PROV.atLocation, archive))

    def _entity(self, path, label, sha256, size_bytes, archive_path=None):
        """Add one file entity, identified by its archive-relative path; return it and that slug."""
        location = archive_path or path
        # A file's identity is its path without traversal or anchor, flattened to one segment.
        slug = _slug("_".join(part for part in Path(location).parts if part not in ("..", ".", "/")))
        entity = self._scoped("entity", slug)
        self.graph.add((entity, RDF.type, PROV.Entity))
        if sha256 is not None:
            checksum = self._scoped("checksum", slug)
            self.graph.set((entity, SPDX.checksum, checksum))
            self.graph.add((checksum, RDF.type, SPDX.Checksum))
            self.graph.set((checksum, SPDX.algorithm, SPDX.checksumAlgorithm_sha256))
            self.graph.set((checksum, SPDX.checksumValue, Literal(sha256, datatype=XSD.hexBinary)))
        self._literal(entity, DCAT.byteSize, size_bytes, XSD.nonNegativeInteger)
        self._literal(entity, RDFS.label, label)
        if location:
            self.graph.set((entity, PROV.atLocation, _location_iri(location)))
        return entity, slug

    def _literal(self, subject, predicate, value, datatype=XSD.string):
        if value is not None:
            self.graph.set((subject, predicate, Literal(value, datatype=datatype)))


def _rows(value):
    return value if isinstance(value, list) else [value]


def _iri(value):
    """A full IRI, or a ``prov:`` term; instances never live in the metamodel namespace."""
    text = str(value)
    if text.startswith("prov:"):
        return PROV[text.removeprefix("prov:")]
    return URIRef(text)


def _slug(value):
    return re.sub(r"[^A-Za-z0-9._-]+", "_", str(value)).strip("_") or "item"


def local_name(iri):
    """The last segment of an IRI: a run's id, or the name a derived node carries."""
    try:
        return split_uri(URIRef(str(iri)))[1]
    except ValueError:
        return _slug(iri)


def _location_iri(value):
    """A file as an IRI: an IRI stays, an absolute path becomes file:, a relative one stays relative."""
    text = str(value)
    if isinstance(value, URIRef) or re.match(r"^[A-Za-z][A-Za-z0-9+.-]+:", text):
        return URIRef(text)
    path = Path(text)
    return URIRef(path.as_uri() if path.is_absolute() else path.as_posix())


def _url(value):
    """An access URL as an IRI; a git@host:path remote is the https form of the same URL."""
    if not value:
        return None
    text = re.sub(r"^[^@/:]+@([^:/]+):", r"https://\1/", str(value))
    return URIRef(text) if "://" in text else None


def _time(value):
    return Literal(value.isoformat() if hasattr(value, "isoformat") else value, datatype=XSD.dateTime)


def serialize(graph):
    """JSON-LD compacted with the prefixes alone; the published context is named, never fetched."""
    document = json.loads(graph.serialize(format="json-ld", context=PREFIXES, auto_compact=True))
    document["@context"] = CONTEXT
    return json.dumps(document, indent=2)


def run_node(graph):
    """The one run a REC document describes: the subject carrying an OSLC state."""
    return next(graph.subjects(OSLC_AUTO.state, None), None)
