# SPDX-License-Identifier: MPL-2.0
# SPDX-FileCopyrightText: 2026 SECORO AG (secoro.uni-bremen.de)

"""Consolidate one run's provenance documents into a single named-graph dataset.

The live observers never merge. This is the one-shot batch step that runs after the terminal
lifecycle event: it reads the run's documents, checks that they describe one run node,
materialises the subclass entailments the axioms license, validates the spine and writes
``provenance.trig``. It rewrites no input.
"""

import json
import os
from pathlib import Path

from pyshacl import validate
from rdflib import Dataset, Graph, URIRef
from rdflib.namespace import RDF, RDFS

from rec.observers.graph_observer import run_node

REC_GRAPH = URIRef("urn:rec")
GENERATION_GRAPH = URIRef("urn:generation")
DESIGN_GRAPH = URIRef("urn:design")
INFERRED_GRAPH = URIRef("urn:inferred")

CONSOLIDATED = "provenance.trig"
METAMODELS_URL = "https://secorolab.github.io/metamodels/"
GENERATION_DOCUMENT = "provenance.ld.json"
# The subclass axioms live in the shapes files -- one .json and one .shacl.ttl per
# vocabulary is the metamodels layout; _entail reads only the rdfs:subClassOf triples.
AXIOMS = (("prov-extension.shacl.ttl",), ("rec", "rec.shacl.ttl"))
# The lifecycle is what these shapes describe. The design and generation documents keep their
# own gates in the archive.
SHAPES = (
    ("prov.shacl.ttl",),
    ("prov-extension.shacl.ttl",),
    ("rec", "rec.shacl.ttl"),
    ("robot", "sensors.shacl.ttl"),
)
VALIDATED_GRAPHS = (REC_GRAPH, INFERRED_GRAPH)


class ConsolidationError(RuntimeError):
    """One run's documents do not consolidate into a single dataset."""


def consolidate_dataset(
    run_dir: Path,
    generation_dir: Path | None = None,
    metamodels_dir: Path | None = None,
) -> Dataset:
    """Return one run's consolidated, entailed and validated named-graph dataset."""
    run_dir = Path(run_dir)
    generation_dir = Path(generation_dir) if generation_dir else run_dir.parent.parent
    metamodels_dir = Path(metamodels_dir) if metamodels_dir else _metamodels_dir(run_dir)

    dataset = Dataset()
    generated = generation_dir / "generated"
    documents = (
        (REC_GRAPH, [_required(run_dir / "rec.ld.json")]),
        (GENERATION_GRAPH, [generated / GENERATION_DOCUMENT]),
        (DESIGN_GRAPH, sorted((generated / "model").glob("*.ld.json"))),
    )
    for name, paths in documents:
        _load(dataset, name, paths, metamodels_dir)

    _verified_run(dataset)
    _entail(dataset, dataset.graph(INFERRED_GRAPH), metamodels_dir)
    _validate(dataset, metamodels_dir)
    return dataset


def consolidate_run(
    run_dir: Path,
    generation_dir: Path | None = None,
    metamodels_dir: Path | None = None,
) -> Path:
    """Write ``run_dir/provenance.trig`` and return its path."""
    dataset = consolidate_dataset(run_dir, generation_dir, metamodels_dir)
    destination = Path(run_dir) / CONSOLIDATED
    dataset.serialize(destination=str(destination), format="trig")
    return destination


def _required(path: Path) -> Path:
    if not path.exists():
        raise ConsolidationError(f"{path}: missing, the run has nothing to consolidate")
    return path


def _load(dataset: Dataset, name: URIRef, paths, metamodels_dir: Path) -> None:
    """Parse ``paths`` into the named graph ``name``, skipping the documents a run may lack."""
    paths = [path for path in paths if path.exists()]
    if not paths:
        return
    graph = dataset.graph(name)
    for path in paths:
        # A generated JSON-LD document may be a dataset; its named graphs flatten into ours.
        parsed = Dataset(default_union=True)
        parsed.parse(
            data=_localised(path, metamodels_dir),
            format="json-ld",
            base=path.resolve().as_uri(),
        )
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
            if (candidate / "rec" / "rec.shacl.ttl").exists():
                return candidate
    raise ConsolidationError("could not locate the metamodels checkout (set METAMODELS_PATH)")


def _verified_run(dataset: Dataset) -> URIRef:
    """Return the one run node the lifecycle document describes."""
    run = run_node(dataset.graph(REC_GRAPH))
    if run is None:
        raise ConsolidationError("rec.ld.json describes no run: no node carries an oslc_auto:state")
    return run


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
