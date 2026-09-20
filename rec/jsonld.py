"""One run's record as a JSON-LD document on the rec and prov-extension vocabularies, and back."""

import re
from pathlib import Path

METAMODELS = "https://secorolab.github.io/metamodels/"
CONTEXT = [
    METAMODELS + "prov.json",
    METAMODELS + "prov-extension.json",
    METAMODELS + "rec/rec.json",
    {"qudt": "http://qudt.org/schema/qudt/"},
]
DIMENSIONLESS = "http://qudt.org/vocab/quantitykind/Dimensionless"
UNITLESS = "http://qudt.org/vocab/unit/UNITLESS"

# OSLC Automation: where a run is, and once complete, how it turned out. DEAD is derived from
# a stale heartbeat by whoever reads, never recorded.
LIFECYCLE = {
    "QUEUED": ("queued", "unavailable"),
    "RUNNING": ("in-progress", "unavailable"),
    "COMPLETED": ("complete", "passed"),
    "FAILED": ("complete", "failed"),
    "INTERRUPTED": ("complete", "error"),
    "TIMED_OUT": ("complete", "error"),
    "CANCELLED": ("canceled", "unavailable"),
}
TIMED_OUT_TRACE = "timed out"


def document(run_iri, record):
    """The JSON-LD document of one run, from the columns an observer stored for it."""
    status = record.get("status")
    if status not in LIFECYCLE:
        raise ValueError(f"{status} is not a recordable status")
    info = record.get("run_info") or {}
    state, verdict = LIFECYCLE[status]
    run = {"@id": run_iri, "@type": ["Activity", "Execution"], "state": state, "verdict": verdict}
    _set(run, "queued-time", info.get("queued_time"))
    _set(run, "startedAtTime", info.get("start_time"))
    _set(run, "endedAtTime", info.get("end_time"))
    _set(run, "heartbeat-time", info.get("heartbeat_time"))
    _set(run, "result", info.get("result"))
    _set(run, "fail-trace", info.get("fail_trace"))
    if status == "TIMED_OUT":
        run["fail-trace"] = info.get("fail_trace") or TIMED_OUT_TRACE
    nodes = [run]
    if info.get("trigger"):
        run["prov:wasStartedBy"] = {"@id": info["trigger"]}
        start = {"@id": f"{run_iri}/start", "@type": "prov:Start", "entity": info["trigger"]}
        _set(start, "hadActivity", info.get("starter"))
        _set(start, "atTime", info.get("start_time"))
        run["prov:qualifiedStart"] = {"@id": start["@id"]}
        nodes.append(start)

    host = record.get("host_info")
    if host:
        node = {"@id": f"{run_iri}/host", "@type": "Host"}
        _set(node, "identifier", host.get("hostname"))
        _set(node, "os", host.get("os"))
        _set(node, "runtime", host.get("python"))
        _set(node, "cpu", host.get("cpu"))
        run["atLocation"] = [node["@id"]]
        nodes.append(node)

    associated = []
    for agent in record.get("agents") or []:
        node = {"@id": agent["id"], "@type": ["Agent", agent["type"]]}
        _set(node, "name", agent.get("name"))
        associated.append(node["@id"])
        nodes.append(node)
    for row in record.get("repositories") or []:
        node = {"@id": f"{run_iri}/software/{_slug(row['name'])}", "@type": ["Agent", "SoftwareAgent"]}
        node["name"] = row["name"]
        _set(node, "identifier", row.get("commit"))
        _set(node, "codeRepository", _url(row.get("url")))
        associated.append(node["@id"])
        nodes.append(node)
    for row in record.get("dependencies") or []:
        node = {"@id": f"{run_iri}/software/{_slug(row['name'])}", "@type": ["Agent", "SoftwareAgent"]}
        node["name"] = row["name"]
        _set(node, "softwareVersion", row.get("version"))
        associated.append(node["@id"])
        nodes.append(node)
    if associated:
        run["wasAssociatedWith"] = associated

    used, usages = [], []
    for row in record.get("sources") or []:
        entity = _entity(run_iri, row)
        used.append(entity["@id"])
        nodes.append(entity)
    for row in record.get("resources") or []:
        entity = _entity(run_iri, row)
        activity = row.get("activity") or run_iri
        usage = {
            "@id": f"{run_iri}/usage/{_slug(activity.rsplit('/', 1)[-1])}/{_slug(row['path'])}",
            "@type": "Usage",
            "entity": entity["@id"],
        }
        _set(usage, "atTime", row.get("time"))
        nodes.extend([entity, usage])
        if activity == run_iri:
            used.append(entity["@id"])
            usages.append(usage["@id"])
        else:
            nodes.append({"@id": activity, "@type": "Activity", "used": [entity["@id"]], "qualifiedUsage": [usage["@id"]]})
    if used:
        run["used"] = used
    if usages:
        run["qualifiedUsage"] = usages
    for row in record.get("artefacts") or []:
        entity = _entity(run_iri, row)
        activity = row.get("activity") or run_iri
        generation = {"@id": f"{run_iri}/generation/{_slug(row['path'])}", "@type": "Generation", "activity": activity}
        _set(generation, "atTime", row.get("time"))
        entity["wasGeneratedBy"] = activity
        entity["qualifiedGeneration"] = generation["@id"]
        nodes.extend([entity, generation])
        if activity != run_iri:
            nodes.append({"@id": activity, "@type": "Activity"})

    for row in record.get("metrics") or []:
        node = {
            "@id": f"{run_iri}/metric/{_slug(row['name'])}/{row['step']}",
            "@type": ["Entity", "Metric"],
            "wasGeneratedBy": run_iri,
            "label": row["name"],
            "qudt:hasQuantityKind": {"@id": DIMENSIONLESS},
            "qudt:value": row["value"],
            "qudt:unit": {"@id": UNITLESS},
            "step": row["step"],
        }
        _set(node, "generatedAtTime", row.get("time"))
        nodes.append(node)
    return {"@context": CONTEXT, "@graph": nodes}


def record(doc):
    """The columns an observer stores, read back from a document this module wrote."""
    nodes = {node["@id"]: node for node in doc["@graph"]}
    run = next(node for node in doc["@graph"] if "state" in node)
    run_iri = run["@id"]
    info = {
        "queued_time": run.get("queued-time"),
        "start_time": run.get("startedAtTime"),
        "end_time": run.get("endedAtTime"),
        "heartbeat_time": run.get("heartbeat-time"),
        "result": run.get("result"),
        "fail_trace": run.get("fail-trace"),
    }
    if "prov:wasStartedBy" in run:
        info["trigger"] = run["prov:wasStartedBy"]["@id"]
        info["starter"] = nodes[run["prov:qualifiedStart"]["@id"]].get("hadActivity")
    pair = (run["state"], run["verdict"])
    status = next(name for name, value in LIFECYCLE.items() if value == pair)
    if pair == LIFECYCLE["TIMED_OUT"] and info["fail_trace"] == TIMED_OUT_TRACE:
        status = "TIMED_OUT"
    rec = {"status": status, "run_info": {key: value for key, value in info.items() if value is not None}}

    host = nodes.get(f"{run_iri}/host")
    if host:
        rec["host_info"] = _drop_none(
            {"hostname": host.get("identifier"), "os": host.get("os"), "python": host.get("runtime"), "cpu": host.get("cpu")}
        )
    for agent_id in run.get("wasAssociatedWith") or []:
        node = nodes[agent_id]
        if agent_id.startswith(f"{run_iri}/software/"):
            if "softwareVersion" in node:
                rec.setdefault("dependencies", []).append(_drop_none({"name": node["name"], "version": node["softwareVersion"]}))
            else:
                rec.setdefault("repositories", []).append(
                    _drop_none({"name": node["name"], "commit": node.get("identifier"), "url": node.get("codeRepository")})
                )
        else:
            kind = next(kind for kind in node["@type"] if kind != "Agent")
            rec.setdefault("agents", []).append(_drop_none({"id": agent_id, "type": kind, "name": node.get("name")}))
    usages = {nodes[usage]["entity"]: nodes[usage] for node in doc["@graph"] for usage in node.get("qualifiedUsage") or []}
    for node in doc["@graph"]:
        for entity_id in node.get("used") or []:
            entity = nodes[entity_id]
            usage = usages.get(entity_id)
            if usage is None:
                rec.setdefault("sources", []).append(_file(entity))
            else:
                row = _file(entity)
                _set(row, "time", usage.get("atTime"))
                if node["@id"] != run_iri:
                    row["activity"] = node["@id"]
                rec.setdefault("resources", []).append(row)
    for node in doc["@graph"]:
        if "qualifiedGeneration" in node:
            row = _file(node)
            generation = nodes[node["qualifiedGeneration"]]
            _set(row, "time", generation.get("atTime"))
            if node["wasGeneratedBy"] != run_iri:
                row["activity"] = node["wasGeneratedBy"]
            rec.setdefault("artefacts", []).append(row)
    for node in doc["@graph"]:
        if "Metric" in node.get("@type", []):
            rec.setdefault("metrics", []).append(
                _drop_none({"name": node["label"], "value": node["qudt:value"], "step": node["step"], "time": node.get("generatedAtTime")})
            )
    return rec


def _entity(run_iri, row):
    path = row["path"]
    node = {"@id": f"{run_iri}/entity/{_slug(path)}", "@type": "Entity", "atLocation": _location(path)}
    _set(node, "label", row.get("title"))
    if row.get("sha256"):
        node["checksum"] = {
            "@id": f"{run_iri}/checksum/{_slug(path)}",
            "@type": "Checksum",
            "algorithm": "sha256",
            "checksum-value": row["sha256"],
        }
    _set(node, "byte-size", row.get("size_bytes"))
    return node


def _file(entity):
    row = {"path": _path(entity["atLocation"])}
    _set(row, "title", entity.get("label"))
    if "checksum" in entity:
        row["sha256"] = entity["checksum"]["checksum-value"]
    _set(row, "size_bytes", entity.get("byte-size"))
    return row


def _location(path):
    text = str(path)
    if re.match(r"^[A-Za-z][A-Za-z0-9+.-]+:", text):
        return text
    return Path(text).as_uri() if Path(text).is_absolute() else Path(text).as_posix()


def _path(location):
    return str(Path(location.removeprefix("file://"))) if location.startswith("file://") else location


def _url(value):
    """A git@host:path remote is the https form of the same URL."""
    if not value:
        return None
    text = re.sub(r"^[^@/:]+@([^:/]+):", r"https://\1/", str(value))
    return text if "://" in text else None


def _slug(value):
    return re.sub(r"[^A-Za-z0-9._-]+", "_", str(value)).strip("_") or "item"


def _set(node, key, value):
    if value is not None:
        node[key] = value


def _drop_none(row):
    return {key: value for key, value in row.items() if value is not None}
