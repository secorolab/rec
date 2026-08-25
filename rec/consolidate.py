# SPDX-License-Identifier: MPL-2.0
# SPDX-FileCopyrightText: 2026 SECORO AG (secoro.uni-bremen.de)

"""Consolidate one run's provenance documents into a single named-graph dataset.

The live observers never merge. This is the one-shot batch step that runs after the terminal
lifecycle event: it reads the run's five documents, checks that they describe one run node,
materialises the subclass entailments the axioms license, validates the spine and writes
``provenance.trig``. It rewrites no input.

The shared run IRI is the contract. When the documents carry the same ``rec:run-id`` under
different subjects that is a defect report, never a merge input -- consolidation refuses.
"""

import json
import os
from datetime import datetime
from pathlib import Path

from pyshacl import validate
from rdflib import BNode, Dataset, Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS, SOSA, XSD, split_uri

MS_PROV = Namespace("https://secorolab.github.io/metamodels/motion-spec/prov#")
REC = Namespace("https://secorolab.github.io/metamodels/rec#")
SENS = Namespace("https://secorolab.github.io/metamodels/robot/sensors#")
QUDT = Namespace("http://qudt.org/schema/qudt/")
TIME = Namespace("http://www.w3.org/2006/time#")

REC_GRAPH = URIRef("urn:rec")
RUNTIME_GRAPH = URIRef("urn:runtime")
BDD_GRAPH = URIRef("urn:bdd")
GENERATION_GRAPH = URIRef("urn:generation")
PLAN_GRAPH = URIRef("urn:plan")
DESIGN_GRAPH = URIRef("urn:design")
INFERRED_GRAPH = URIRef("urn:inferred")

CONSOLIDATED = "provenance.trig"
METAMODELS_URL = "https://secorolab.github.io/metamodels/"
GENERATION_DOCUMENTS = ("motion-spec.ld.json", "dsl.ld.json", "coord-dsl.ld.json")
AXIOMS = (("motion-spec", "prov.ttl"), ("rec", "rec.ttl"))
# The runtime and the lifecycle are what these shapes describe. The BDD graph is deliberately
# out: its observations time-stamp with the xsd:dateTime sosa:resultTime SOSA prescribes, while
# ms-prov:InstantShape targets objects of sosa:resultTime and demands a time:Instant, and
# sens:ObservationShape demands a sosa:madeBySensor no acceptance fluent has. The design and
# generation documents keep their own gates in the archive.
SHAPES = (("rec", "rec.shacl.ttl"), ("motion-spec", "prov.shacl.ttl"), ("robot", "sensors.shacl.ttl"))
VALIDATED_GRAPHS = (REC_GRAPH, RUNTIME_GRAPH, INFERRED_GRAPH)
# A sim clock counts from the simulator's own epoch, so its instants land in 1970. A wall clock
# never does, and its stamp says nothing about a tick.
SIM_EPOCH_YEAR = 2000


class ConsolidationError(RuntimeError):
    """One run's documents do not consolidate into a single dataset."""


def consolidate_run(
    run_dir: Path,
    generation_dir: Path | None = None,
    metamodels_dir: Path | None = None,
) -> Path:
    """Write ``run_dir/provenance.trig`` and return its path."""
    run_dir = Path(run_dir)
    generation_dir = Path(generation_dir) if generation_dir else run_dir.parent.parent
    metamodels_dir = Path(metamodels_dir) if metamodels_dir else _metamodels_dir(run_dir)

    dataset = Dataset()
    provenance = generation_dir / "generated" / "provenance"
    documents = (
        (REC_GRAPH, [_required(run_dir / "rec.ld.json")], "json-ld"),
        (RUNTIME_GRAPH, [_required(run_dir / "runtime" / "runtime.ttl")], "turtle"),
        (BDD_GRAPH, sorted((run_dir / "runtime").glob("bdd-*.ttl")), "turtle"),
        (GENERATION_GRAPH, [provenance / name for name in GENERATION_DOCUMENTS], "json-ld"),
        (PLAN_GRAPH, [provenance / "plan.ld.json"], "json-ld"),
        (DESIGN_GRAPH, sorted((generation_dir / "generated" / "model").glob("*.ld.json")), "json-ld"),
    )
    for name, paths, fmt in documents:
        _load(dataset, name, paths, fmt, metamodels_dir)

    run = _verified_run(dataset)
    inferred = dataset.graph(INFERRED_GRAPH)
    _entail(dataset, inferred, metamodels_dir)
    _enrich_bdd_ticks(dataset, inferred, run)
    _validate(dataset, metamodels_dir)

    destination = run_dir / CONSOLIDATED
    dataset.serialize(destination=str(destination), format="trig")
    return destination


def _required(path: Path) -> Path:
    if not path.exists():
        raise ConsolidationError(f"{path}: missing, the run has nothing to consolidate")
    return path


def _load(dataset: Dataset, name: URIRef, paths, fmt: str, metamodels_dir: Path) -> None:
    """Parse ``paths`` into the named graph ``name``, skipping the documents a run may lack."""
    paths = [path for path in paths if path.exists()]
    if not paths:
        return
    graph = dataset.graph(name)
    for path in paths:
        # A generated JSON-LD document may be a dataset; its named graphs flatten into ours.
        parsed = Dataset(default_union=True)
        if fmt == "json-ld":
            parsed.parse(
                data=_localised(path, metamodels_dir),
                format=fmt,
                base=path.resolve().as_uri(),
            )
        else:
            parsed.parse(str(path), format=fmt)
        for triple in parsed.triples((None, None, None)):
            graph.add(triple)


def _localised(path: Path, metamodels_dir: Path) -> str:
    """Return the document with its metamodel contexts pointed at the local checkout.

    A published context lags the checkout the shapes come from, and consolidating a document
    against one vocabulary while validating it against another reports defects that are not
    there. Nothing else about the document changes.
    """
    document = json.loads(path.read_text())

    def local(entry):
        if isinstance(entry, str) and entry.startswith(METAMODELS_URL):
            candidate = metamodels_dir / entry[len(METAMODELS_URL) :]
            if candidate.exists():
                return candidate.resolve().as_uri()
        return entry

    for node in document if isinstance(document, list) else [document]:
        context = node.get("@context") if isinstance(node, dict) else None
        if isinstance(context, str):
            node["@context"] = local(context)
        elif isinstance(context, list):
            node["@context"] = [local(entry) for entry in context]
    return json.dumps(document)


def _metamodels_dir(run_dir: Path) -> Path:
    """Locate the metamodel checkout the axioms and shapes live in."""
    env_path = os.environ.get("METAMODELS_PATH")
    roots = [Path(env_path)] if env_path else []
    start = run_dir.resolve()
    roots.extend(start.parents)
    for root in roots:
        for candidate in (root, root / "src" / "metamodels", root / "metamodels"):
            if (candidate / "rec" / "rec.ttl").exists():
                return candidate
    raise ConsolidationError("could not locate the metamodels checkout (set METAMODELS_PATH)")


def _verified_run(dataset: Dataset) -> URIRef:
    """Return the one run node the runtime and the lifecycle documents agree on."""
    runtime = dataset.graph(RUNTIME_GRAPH)
    runs = set(runtime.subjects(RDF.type, MS_PROV.TaskExecution))
    if not runs:
        raise ConsolidationError(
            "runtime.ttl declares no ms-prov:TaskExecution: this archive predates the run "
            "vocabulary -- regenerate with --recover-runtime-ttl"
        )
    if len(runs) > 1:
        raise ConsolidationError(f"runtime.ttl declares {len(runs)} runs: {sorted(map(str, runs))}")
    run = runs.pop()
    lifecycle = dict(dataset.graph(REC_GRAPH).subject_objects(REC["run-id"]))
    if run in lifecycle:
        return run
    matching = [subject for subject, run_id in lifecycle.items() if str(run_id) in str(run)]
    if matching:
        raise ConsolidationError(
            f"the run id matches but the IRIs do not: runtime says <{run}>, rec.ld.json says "
            f"<{matching[0]}> -- the shared IRI is the contract, consolidation does not patch it"
        )
    raise ConsolidationError(f"rec.ld.json carries no rec:run-id for <{run}>")


def _entail(dataset: Dataset, inferred: Graph, metamodels_dir: Path) -> None:
    """Materialise the rdf:type triples the subclass axioms license, and nothing else."""
    from owlrl import DeductiveClosure, RDFS_Semantics

    axioms = Graph()
    for parts in AXIOMS:
        axioms.parse(str(_required(metamodels_dir.joinpath(*parts))), format="turtle")
    superclasses = set(axioms.objects(None, RDFS.subClassOf))
    declared = set(axioms.subjects(RDFS.subClassOf)) | superclasses

    closure = Graph()
    for triple in axioms:
        closure.add(triple)
    for graph in dataset.graphs():
        for triple in graph.triples((None, RDF.type, None)):
            if triple[2] in declared:
                closure.add(triple)
    DeductiveClosure(RDFS_Semantics).expand(closure)
    # Everything the axioms hand out, whether or not a document also states it: the graph says
    # what follows from the axioms, not what happened to be left over.
    for triple in closure.triples((None, RDF.type, None)):
        if triple[2] in superclasses:
            inferred.add(triple)


def _enrich_bdd_ticks(dataset: Dataset, inferred: Graph, run: URIRef) -> None:
    """Position each acceptance observation on the run's tick scale.

    An observation's sosa:resultTime is the xsd:dateTime SOSA prescribes, so the tick goes on a
    sosa:phenomenonTime instant -- the SOSA property whose range is a temporal entity -- rather
    than displacing a literal that is already correct.
    """
    bdd = dataset.graph(BDD_GRAPH)
    if not len(bdd):
        return
    runtime = dataset.graph(RUNTIME_GRAPH)
    trs, rate = _tick_scale(runtime, run)
    if trs is None or rate is None:
        return
    instants = _instant_namespace(runtime)
    for observation, stamp in bdd.subject_objects(SOSA.resultTime):
        if not isinstance(stamp, Literal) or stamp.datatype != XSD.dateTime:
            continue
        moment = stamp.toPython()
        if not isinstance(moment, datetime) or moment.year >= SIM_EPOCH_YEAR:
            continue  # a wall clock says nothing about a tick
        tick = round(moment.timestamp() * rate)
        instant = instants[str(tick)]
        inferred.add((observation, SOSA.phenomenonTime, instant))
        # The tick the run already recorded is that instant; only an unrecorded one is placed.
        if (instant, TIME.inTimePosition, None) in runtime:
            continue
        position = BNode()
        inferred.add((instant, RDF.type, TIME.Instant))
        inferred.add((instant, TIME.inTimePosition, position))
        inferred.add((position, TIME.numericPosition, Literal(tick)))
        inferred.add((position, TIME.hasTRS, trs))


def _tick_scale(runtime: Graph, run: URIRef):
    """Return the run's time reference system and its tick rate in Hz."""
    beginning = runtime.value(run, TIME.hasBeginning)
    position = runtime.value(beginning, TIME.inTimePosition) if beginning else None
    trs = runtime.value(position, TIME.hasTRS) if position else None
    quantity = runtime.value(trs, SENS["update-rate"]) if trs else None
    rate = runtime.value(quantity, QUDT.value) if quantity else None
    return trs, float(rate) if rate is not None else None


def _instant_namespace(runtime: Graph) -> Namespace:
    """The run's instant IRIs, so a tick position names the instant the runtime already names."""
    for instant in runtime.subjects(RDF.type, TIME.Instant):
        return Namespace(split_uri(instant)[0])
    raise ConsolidationError("runtime.ttl names no time:Instant to position the acceptance record on")


def _validate(dataset: Dataset, metamodels_dir: Path) -> None:
    shapes = Graph()
    for parts in SHAPES:
        shapes.parse(str(_required(metamodels_dir.joinpath(*parts))), format="turtle")
    data = Graph()
    for name in VALIDATED_GRAPHS:
        for triple in dataset.graph(name):
            data.add(triple)
    conforms, _graph, text = validate(data_graph=data, shacl_graph=shapes, inference="none")
    if not conforms:
        raise ConsolidationError(f"consolidated run failed SHACL validation: {text}")
