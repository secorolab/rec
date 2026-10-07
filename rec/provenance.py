"""A run record as PROV on the prov-extension and rec vocabularies, and back."""

import json
from urllib.parse import quote, unquote

from rdf_utils.models.prov import (
    _path_from_file_url,
    add_agent,
    add_entity,
    add_file_entity,
    add_relative_location,
    load_execution_prov,
    load_pkg_prov,
)
from rdflib import XSD, Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCAT, PROV, RDF, RDFS, SDO

from rec import State, Verdict
from rec.record import (
    ORDER,
    Agent,
    Dependency,
    FileRef,
    Generation,
    Host,
    Metric,
    Repository,
    RunRecord,
    Software,
    Usage,
)

RUN_BASE = "https://secoro.uni-bremen.de/rec/run/"
REC = Namespace("https://secorolab.github.io/metamodels/rec#")
PROV_EXT = Namespace("https://secorolab.github.io/metamodels/prov#")
OSLC_AUTO = Namespace("http://open-services.net/ns/auto#")
SPDX = Namespace("http://spdx.org/rdf/terms#")
QUDT = Namespace("http://qudt.org/schema/qudt/")
DIMENSIONLESS = URIRef("http://qudt.org/vocab/quantitykind/Dimensionless")
UNITLESS = URIRef("http://qudt.org/vocab/unit/UNITLESS")

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
    "spdx": SPDX,
    "dcat": DCAT,
    "schema": SDO,
    "qudt": QUDT,
    "rdfs": RDFS,
    "xsd": XSD,
}


def graph(record: RunRecord, base: str = RUN_BASE) -> Graph:
    """The run as a `prov:Activity` with its OSLC state; once started, a `prov-ext:Execution`
    of its program by the software that recorded it."""
    g = Graph()
    for prefix, namespace in PREFIXES.items():
        g.bind(prefix, namespace)
    run = URIRef(base + quote(record.run_id, safe=""))
    g.add((run, RDF.type, PROV.Activity))
    g.add((run, OSLC_AUTO.state, STATES[record.state]))
    g.add((run, OSLC_AUTO.verdict, VERDICTS[record.verdict]))
    for value, predicate in ((record.queued_time, REC["queued-time"]), (record.heartbeat_time, REC["heartbeat-time"])):
        if value:
            g.add((run, predicate, Literal(value)))
    # A structured result as a JSON literal (JSON-LD 1.1, 4.2.2); a scalar keeps its XSD type.
    if isinstance(record.result, (dict, list, tuple)):
        g.add((run, REC.result, Literal(json.dumps(record.result), datatype=RDF.JSON)))
    elif record.result is not None:
        g.add((run, REC.result, Literal(record.result)))
    if record.fail_trace:
        g.add((run, REC["fail-trace"], Literal(record.fail_trace, datatype=XSD.string)))

    used = [_file(g, run, ref) for ref in record.sources]
    for row in record.repositories:
        repository = URIRef(f"{run}/repository/{quote(row.name, safe='')}")
        add_entity(g, repository)
        g.add((repository, RDF.type, SDO.SoftwareSourceCode))
        g.add((repository, RDFS.label, Literal(row.name)))
        if row.commit:
            g.add((repository, SDO.identifier, Literal(row.commit)))
        if row.url:
            g.add((repository, SDO.codeRepository, URIRef(row.url)))
        used.append(repository)
    for row in record.dependencies:
        dependency = URIRef(f"{run}/dependency/{quote(row.name, safe='')}")
        add_entity(g, dependency)
        g.add((dependency, RDF.type, SDO.SoftwareApplication))
        g.add((dependency, RDFS.label, Literal(row.name)))
        if row.version:
            g.add((dependency, SDO.softwareVersion, Literal(row.version)))
        used.append(dependency)
    for usage in record.resources:
        entity = _file(g, run, usage.file)
        activity = URIRef(usage.activity or run)
        node = URIRef(f"{entity}/usage/{quote(str(activity), safe='')}")
        g.add((node, RDF.type, PROV.Usage))
        g.add((node, PROV.entity, entity))
        if usage.time:
            g.add((node, PROV.atTime, Literal(usage.time)))
        g.add((activity, RDF.type, PROV.Activity))
        g.add((activity, PROV.used, entity))
        g.add((activity, PROV.qualifiedUsage, node))

    if record.start_time:
        software = URIRef(f"{run}/software")
        load_pkg_prov(g, software, record.recorder.name, record.recorder.version, record.recorder.commit, record.recorder.repository)
        association = URIRef(f"{run}/association")
        g.add((run, PROV.qualifiedAssociation, association))
        g.add((association, RDF.type, PROV.Association))
        g.add((association, PROV.agent, software))
        if record.program:
            plan = _file(g, run, record.program)
            g.add((plan, RDF.type, PROV.Plan))
            g.add((association, PROV.hadPlan, plan))
            used.append(plan)
        load_execution_prov(g, run, used, software, record.start_time, record.end_time)
    else:
        for entity in used:
            g.add((run, PROV.used, entity))
        if record.end_time:
            g.add((run, PROV.endedAtTime, Literal(record.end_time)))
    for agent in record.agents:
        add_agent(g, URIRef(agent.id), (PROV[agent.type],), agent.name)
        g.add((run, PROV.wasAssociatedWith, URIRef(agent.id)))

    if record.trigger:
        start = URIRef(f"{run}/start")
        g.add((run, PROV.wasStartedBy, URIRef(record.trigger)))
        g.add((run, PROV.qualifiedStart, start))
        g.add((start, RDF.type, PROV.Start))
        g.add((start, PROV.entity, URIRef(record.trigger)))
        if record.starter:
            g.add((start, PROV.hadActivity, URIRef(record.starter)))
    if record.host:
        host = URIRef(f"{run}/host")
        g.add((host, RDF.type, REC.Host))
        g.add((host, RDF.type, PROV.Location))
        for key, predicate in HOST_TERMS.items():
            if getattr(record.host, key):
                g.add((host, predicate, Literal(getattr(record.host, key))))
        g.add((run, PROV.atLocation, host))

    for generation in record.artefacts:
        entity = _file(g, run, generation.file)
        activity = URIRef(generation.activity or run)
        node = URIRef(f"{entity}/generation/{quote(str(activity), safe='')}")
        g.add((activity, RDF.type, PROV.Activity))
        g.add((entity, PROV.wasGeneratedBy, activity))
        g.add((entity, PROV.qualifiedGeneration, node))
        g.add((node, RDF.type, PROV.Generation))
        g.add((node, PROV.activity, activity))
        if generation.time:
            g.add((node, PROV.atTime, Literal(generation.time)))
    for metric in record.metrics:
        node = URIRef(f"{run}/metric/{quote(metric.name, safe='')}/{metric.step}")
        add_entity(g, node)
        g.add((node, RDF.type, REC.Metric))
        g.add((node, PROV.wasGeneratedBy, run))
        g.add((node, RDFS.label, Literal(metric.name)))
        g.add((node, QUDT.hasQuantityKind, DIMENSIONLESS))
        g.add((node, QUDT.value, Literal(metric.value)))
        g.add((node, QUDT.unit, UNITLESS))
        g.add((node, REC.step, Literal(metric.step)))
        if metric.time:
            g.add((node, PROV.generatedAtTime, Literal(metric.time)))
    return g


def record(g: Graph, run_id: str, base: str = RUN_BASE) -> RunRecord:
    """The record ``graph`` wrote; its lists come back sorted, a graph having no order."""
    run = URIRef(base + quote(run_id, safe=""))
    association = g.value(run, PROV.qualifiedAssociation)
    plan, software = g.value(association, PROV.hadPlan), g.value(association, PROV.agent)
    start = g.value(run, PROV.qualifiedStart)
    host = g.value(run, PROV.atLocation)
    result = g.value(run, REC.result)
    run_usages = {g.value(usage, PROV.entity) for usage in g.objects(run, PROV.qualifiedUsage)}
    kinds = {SDO.SoftwareSourceCode, SDO.SoftwareApplication}
    # Only what links to this run, or what graph() named under its IRI: the graph may hold other runs.
    own = f"{run}/"
    return RunRecord(
        run_id=run_id,
        state=next(state for state, iri in STATES.items() if iri == g.value(run, OSLC_AUTO.state)),
        verdict=next(verdict for verdict, iri in VERDICTS.items() if iri == g.value(run, OSLC_AUTO.verdict)),
        queued_time=_python(g.value(run, REC["queued-time"])),
        start_time=_python(g.value(run, PROV.startedAtTime)),
        end_time=_python(g.value(run, PROV.endedAtTime)),
        heartbeat_time=_python(g.value(run, REC["heartbeat-time"])),
        result=json.loads(result) if result is not None and result.datatype == RDF.JSON else _python(result),
        fail_trace=_python(g.value(run, REC["fail-trace"])),
        program=_file_ref(g, plan) if plan else None,
        recorder=Software(
            str(g.value(software, SDO.name)),
            _python(g.value(software, SDO.softwareVersion)),
            _python(g.value(software, SDO.identifier)),
            _python(g.value(software, SDO.codeRepository)),
        )
        if software
        else None,
        trigger=_python(g.value(start, PROV.entity)),
        starter=_python(g.value(start, PROV.hadActivity)),
        host=Host(**{key: _python(g.value(host, predicate)) for key, predicate in HOST_TERMS.items()}) if host else None,
        agents=sorted(
            (
                Agent(
                    str(agent),
                    next((str(kind).removeprefix(str(PROV)) for kind in g.objects(agent, RDF.type) if kind != PROV.Agent), "Agent"),
                    _python(g.value(agent, SDO.name)),
                )
                for agent in g.objects(run, PROV.wasAssociatedWith)
                if agent != software
            ),
            key=ORDER["agents"],
        ),
        sources=sorted(
            (
                _file_ref(g, entity)
                for entity in g.objects(run, PROV.used)
                if entity != plan and entity not in run_usages and not set(g.objects(entity, RDF.type)) & kinds
            ),
            key=ORDER["sources"],
        ),
        repositories=sorted(
            (
                Repository(str(g.value(node, RDFS.label)), _python(g.value(node, SDO.codeRepository)), _python(g.value(node, SDO.identifier)))
                for node in g.objects(run, PROV.used)
                if (node, RDF.type, SDO.SoftwareSourceCode) in g
            ),
            key=ORDER["repositories"],
        ),
        dependencies=sorted(
            (
                Dependency(str(g.value(node, RDFS.label)), _python(g.value(node, SDO.softwareVersion)))
                for node in g.objects(run, PROV.used)
                if (node, RDF.type, SDO.SoftwareApplication) in g
            ),
            key=ORDER["dependencies"],
        ),
        resources=sorted(
            (
                Usage(
                    _file_ref(g, g.value(usage, PROV.entity)),
                    None if activity == run else str(activity),
                    _python(g.value(usage, PROV.atTime)),
                )
                for activity, usage in g.subject_objects(PROV.qualifiedUsage)
                if usage.startswith(own)
            ),
            key=ORDER["resources"],
        ),
        artefacts=sorted(
            (
                Generation(
                    _file_ref(g, entity),
                    None if g.value(node, PROV.activity) == run else str(g.value(node, PROV.activity)),
                    _python(g.value(node, PROV.atTime)),
                )
                for entity, node in g.subject_objects(PROV.qualifiedGeneration)
                if node.startswith(own)
            ),
            key=ORDER["artefacts"],
        ),
        metrics=sorted(
            (
                Metric(
                    str(g.value(node, RDFS.label)),
                    g.value(node, REC.step).toPython(),
                    g.value(node, QUDT.value).toPython(),
                    _python(g.value(node, PROV.generatedAtTime)),
                )
                for node in g.subjects(PROV.wasGeneratedBy, run)
                if (node, RDF.type, REC.Metric) in g
            ),
            key=ORDER["metrics"],
        ),
    )


def document(g: Graph) -> dict:
    """The graph as JSON-LD, compacted on the vocabularies' prefixes."""
    return json.loads(g.serialize(format="json-ld", context={prefix: str(ns) for prefix, ns in PREFIXES.items()}))


def _file(g: Graph, run: URIRef, ref: FileRef) -> URIRef:
    # The root is part of the name: one relative path logged from two directories is two files.
    if ref.root:
        entity = URIRef(f"{run}/file/{quote(ref.root, safe='')}/{quote(ref.path, safe='')}")
    else:
        entity = URIRef(f"{run}/file/{quote(ref.path, safe='')}")
    if ref.root:
        location = URIRef(f"{entity}/location")
        add_relative_location(g, location, ref.path, ref.root)
    else:
        location = ref.path
    add_file_entity(g, entity, location)
    if ref.title:
        g.set((entity, RDFS.label, Literal(ref.title)))
    if ref.sha256:
        checksum = URIRef(f"{entity}/checksum")
        g.add((entity, SPDX.checksum, checksum))
        g.add((checksum, RDF.type, SPDX.Checksum))
        g.set((checksum, SPDX.algorithm, SPDX.checksumAlgorithm_sha256))
        g.set((checksum, SPDX.checksumValue, Literal(ref.sha256, datatype=XSD.hexBinary)))
    if ref.size_bytes is not None:
        g.set((entity, DCAT.byteSize, Literal(ref.size_bytes, datatype=XSD.nonNegativeInteger)))
    return entity


def _file_ref(g: Graph, entity: URIRef) -> FileRef:
    location = g.value(entity, PROV.atLocation)
    rel_path = g.value(location, PROV_EXT["rel-path"])
    if rel_path is not None:
        path, root = unquote(str(rel_path)), str(_path_from_file_url(str(g.value(location, PROV.atLocation))))
    else:
        path, root = str(_path_from_file_url(str(location))) if str(location).startswith("file:") else str(location), None
    size = g.value(entity, DCAT.byteSize)
    # An xsd:hexBinary's Python value is bytes; the record holds the hex text.
    checksum = g.value(g.value(entity, SPDX.checksum), SPDX.checksumValue)
    return FileRef(
        path,
        root,
        _python(g.value(entity, RDFS.label)),
        str(checksum) if checksum is not None else None,
        size.toPython() if size is not None else None,
    )


def _python(term):
    """A term as the value the record holds: a literal's Python value, an IRI as its string."""
    if term is None:
        return None
    return term.toPython() if isinstance(term, Literal) else str(term)
