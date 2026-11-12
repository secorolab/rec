"""The vocabulary of a run's provenance bundle: prov-extension, rec, and OSLC Automation state and verdict."""

import re

from rdflib import XSD, Namespace
from rdflib.namespace import PROV, SDO, TIME

from rec import State, Verdict

RUN_BASE = "https://secoro.uni-bremen.de/rec/run/"
# RFC 3986 unreserved characters (2.3), so an id is the same in an IRI, a file name and a database row.
RUN_ID = re.compile(r"(?!\.\.?$)[A-Za-z0-9._~-]+")
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
# The JSON-LD context the stores serialize with, compacting on the prefixes.
CONTEXT = {prefix: str(namespace) for prefix, namespace in PREFIXES.items()}
