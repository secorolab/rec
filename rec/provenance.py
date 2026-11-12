"""A run record as a PROV bundle on the prov-extension and rec vocabularies, and back."""

from datetime import datetime
from typing import Any

from rdf_utils import DatasetFormat
from rdf_utils.models.prov import PackageInfo, load_execution_prov, load_pkg_prov
from rdf_utils.models.time import add_wall_instant
from rdf_utils.models.vocab import URI_SPDX_SW_TYPE_PACKAGE
from rdflib import XSD, Dataset, Graph, Literal, Namespace, URIRef
from rdflib.namespace import PROV, RDF, SDO, TIME
from rdflib.term import Node

from rec import State, Verdict
from rec.record import Host, RunRecord

RUN_BASE = "https://secoro.uni-bremen.de/rec/run/"
REC = Namespace("https://secorolab.github.io/metamodels/rec#")
PROV_EXT = Namespace("https://secorolab.github.io/metamodels/prov#")
OSLC_AUTO = Namespace("http://open-services.net/ns/auto#")

STATES = {
    State.NEW: OSLC_AUTO.new,
    State.QUEUED: OSLC_AUTO.queued,
    State.IN_PROGRESS: OSLC_AUTO.inProgress,
    State.CANCELING: OSLC_AUTO.canceling,
    State.CANCELED: OSLC_AUTO.canceled,
    State.COMPLETE: OSLC_AUTO.complete,
}
VERDICTS = {
    Verdict.UNAVAILABLE: OSLC_AUTO.unavailable,
    Verdict.PASSED: OSLC_AUTO.passed,
    Verdict.WARNING: OSLC_AUTO.warning,
    Verdict.FAILED: OSLC_AUTO.failed,
    Verdict.ERROR: OSLC_AUTO.error,
}
HOST_TERMS = {"hostname": SDO.identifier, "os": REC.os, "python": REC.runtime, "cpu": REC.cpu}
PREFIXES = {
    "prov": PROV,
    "prov-ext": PROV_EXT,
    "rec": REC,
    "oslc_auto": OSLC_AUTO,
    "schema": SDO,
    "time": TIME,
    "xsd": XSD,
}


def dataset(record: RunRecord, base: str = RUN_BASE) -> Dataset:
    """The run in its bundle, attributed to its recorder: a `prov-ext:Execution` and an
    `oslc_auto:AutomationResult` with its state and verdict; once started, of the packages it used
    by its recorder."""
    ds = Dataset()
    for prefix, namespace in PREFIXES.items():
        ds.bind(prefix, namespace)
    g = ds.graph(record.bundle)
    # The bundle's id is kept in the record, so these are rdf-utils `create_bundle`'s triples
    # under an id minted once rather than on every write.
    g.add((record.bundle, RDF.type, PROV.Bundle))
    g.add((record.bundle, PROV.wasAttributedTo, record.recorder.purl))
    load_pkg_prov(g, record.recorder)

    run = URIRef(base + record.run_id)
    g.add((run, RDF.type, PROV_EXT.Execution))
    g.add((run, RDF.type, OSLC_AUTO.AutomationResult))
    g.add((run, OSLC_AUTO.state, STATES[record.state]))
    g.add((run, OSLC_AUTO.verdict, VERDICTS[record.verdict]))
    for value, predicate in ((record.queued_time, REC["queued-time"]), (record.heartbeat_time, REC["heartbeat-time"])):
        if value:
            g.add((run, predicate, Literal(value)))
    if record.fail_trace:
        g.add((run, REC["fail-trace"], Literal(record.fail_trace, datatype=XSD.string)))

    for package in record.packages:
        load_pkg_prov(g, package)
    used = [package.purl for package in record.packages]
    ended = None
    if record.end_time:
        ended = URIRef(f"{run}/end")
        add_wall_instant(g, ended, record.end_time)
    if record.start_time:
        started = URIRef(f"{run}/beginning")
        add_wall_instant(g, started, record.start_time)
        load_execution_prov(g, run, used, record.recorder.purl, started, ended)
    else:
        # A queued run cancelled before it started: it has an end, but no beginning or recorder.
        for purl in used:
            g.add((run, PROV.used, purl))
        if ended is not None:
            g.add((run, TIME.hasEnd, ended))

    if record.host:
        host = URIRef(f"{run}/host")
        g.add((host, RDF.type, REC.Host))
        g.add((host, RDF.type, PROV.Location))
        for key, predicate in HOST_TERMS.items():
            if getattr(record.host, key):
                g.add((host, predicate, Literal(getattr(record.host, key))))
        g.add((run, PROV.atLocation, host))
    return ds


def record(ds: Dataset, run_id: str, base: str = RUN_BASE) -> RunRecord:
    """The record ``dataset`` wrote; its packages come back in no particular order, a graph having none."""
    run = URIRef(base + run_id)
    g = next(graph for graph in ds.graphs() if (run, OSLC_AUTO.state, None) in graph)
    host = g.value(run, PROV.atLocation)
    return RunRecord(
        run_id=run_id,
        bundle=g.identifier,
        state=next(state for state, iri in STATES.items() if iri == g.value(run, OSLC_AUTO.state)),
        verdict=next(verdict for verdict, iri in VERDICTS.items() if iri == g.value(run, OSLC_AUTO.verdict)),
        queued_time=_python(g.value(run, REC["queued-time"])),
        start_time=_python(g.value(run, TIME.hasBeginning / TIME.inXSDDateTimeStamp)),
        end_time=_python(g.value(run, TIME.hasEnd / TIME.inXSDDateTimeStamp)),
        heartbeat_time=_python(g.value(run, REC["heartbeat-time"])),
        fail_trace=_python(g.value(run, REC["fail-trace"])),
        recorder=_package(g, g.value(g.identifier, PROV.wasAttributedTo)),
        host=Host(**{key: _python(g.value(host, predicate)) for key, predicate in HOST_TERMS.items()}) if host else None,
        packages=[
            _package(g, purl)
            for purl in g.objects(run, PROV.used)
            if (purl, RDF.type, URI_SPDX_SW_TYPE_PACKAGE) in g
        ],
    )


def serialize(ds: Dataset, fmt: DatasetFormat) -> str:
    """The dataset in FMT; JSON-LD compacted on the vocabularies' prefixes."""
    if fmt is DatasetFormat.JSON_LD:
        return ds.serialize(format=fmt, context={prefix: str(ns) for prefix, ns in PREFIXES.items()}, indent=2)
    return ds.serialize(format=fmt)


def _package(g: Graph, purl: Node) -> PackageInfo:
    """The package `load_pkg_prov` wrote at PURL; it writes no distribution name or directory."""
    revision = _python(g.value(purl, SDO.identifier))
    return PackageInfo(
        name=str(g.value(purl, SDO.name)),
        purl=URIRef(purl),
        version=_python(g.value(purl, SDO.softwareVersion)),
        revision=revision.removesuffix("-dirty") if revision else None,
        repository=_python(g.value(purl, SDO.codeRepository)),
        dirty=bool(revision and revision.endswith("-dirty")),
    )


def _python(term: Node | None) -> Any:
    """A term as the value the record holds: a literal's Python value, an IRI as its string."""
    if term is None:
        return None
    # rdflib maps no Python type to xsd:dateTimeStamp.
    if isinstance(term, Literal) and term.datatype == XSD.dateTimeStamp:
        return datetime.fromisoformat(str(term))
    return term.toPython() if isinstance(term, Literal) else str(term)
