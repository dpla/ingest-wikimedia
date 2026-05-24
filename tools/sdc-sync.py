# TODO caption, date, page, iiif manifest, url

import pywikibot
import requests
import re
import json
import datetime
import argparse
import random
import os
import sys
import time
import tomllib
import urllib.parse
from bs4 import BeautifulSoup
from pywikibot.comms import http
from pywikibot import pagegenerators
from ingest_wikimedia.wikimedia import extract_dpla_id_from_commons_title

# When running manually, sometimes it is helpful to specify the category to work on in the command line, using --cat "<category>".

parser = argparse.ArgumentParser()
parser.add_argument(
    "--cat",
    dest="cat",
    metavar="CAT",
    action="store",
    help="Commons category name (without 'Category:' prefix) to enumerate File: pages from",
)
parser.add_argument(
    "--recurse",
    dest="recurse",
    action="store_true",
    help="When using --cat, also walk subcategories",
)
parser.add_argument("--method", dest="method", metavar="METHOD", action="store")
parser.add_argument("--lists", dest="lists", metavar="LISTS", action="store")
parser.add_argument(
    "--file",
    dest="files",
    metavar="FILE",
    action="append",
    help="Commons file title to process directly (repeatable)",
)
parser.add_argument(
    "--limit",
    dest="limit",
    type=int,
    default=0,
    help="When using --cat, stop after this many files (0 = no limit)",
)
args = parser.parse_args()

method = "livecat"
if args.method:
    method = args.method


def _normalize_rights_uri(uri):
    """Canonicalize a DPLA rights URI for lookup against rights.json.

    DPLA emits rights URIs in a few minor variants — http vs https and with or
    without a trailing slash. Keying on a single canonical form means we don't
    silently miss licenses just because of scheme/slash drift on either side.
    """
    if not uri:
        return uri
    canonical = uri.replace("https://", "http://").rstrip("/")
    return canonical


# Resolve every load (config + vendored JSONs) relative to the repo root so the
# script behaves the same regardless of the caller's working directory.
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_HERE)

# Config + Commons login happen after argparse so that `--help` doesn't trigger
# file I/O or network/auth on this module's import.
with open(os.path.join(_REPO_ROOT, "config.toml"), "rb") as _f:
    dpla_api = tomllib.load(_f)["dpla_api_key"]

site = pywikibot.Site()
site.login()

# Hubs and subject mappings are fetched live from ingestion3 on every run, by
# design: this sync exists precisely to propagate upstream changes to that
# data, and a vendored snapshot would defeat the point. rights.json is the
# exception — it lives here because it's a small, slow-moving SDC-specific
# mapping not maintained in ingestion3.
hubs = requests.get(
    "https://raw.githubusercontent.com/dpla/ingestion3/develop/src/main/resources/wiki/institutions_v2.json",
    timeout=30,
).json()
with open(os.path.join(_REPO_ROOT, "rights.json")) as f:
    rights = {_normalize_rights_uri(k): v for k, v in json.load(f).items()}
subject_ids = requests.get(
    "https://raw.githubusercontent.com/DominicBM/ingestion3/develop/src/main/resources/subjects.json",
    timeout=30,
).json()

# This is the JSON used for formatting a claim. The P459 -> Q61848113 (determination method) qualifier is hardcoded in for everything DPLA adds. Not all data types have the same format for value, so this is formatted in the function for each property added.


def formattedclaim(prop, value, value_type, dpla_id):
    claim = {
        "mainsnak": {
            "snaktype": "value",
            "property": prop,
            "datavalue": {"value": value, "type": value_type},
        },
        "type": "statement",
        "rank": "normal",
        "qualifiers": {
            "P459": [
                {
                    "snaktype": "value",
                    "property": "P459",
                    "datavalue": {
                        "value": {"entity-type": "item", "numeric-id": 61848113},
                        "type": "wikibase-entityid",
                    },
                    "datatype": "wikibase-item",
                }
            ]
        },
        "references": [
            {
                "snaks": {
                    "P854": [
                        {
                            "snaktype": "value",
                            "property": "P854",
                            "datavalue": {
                                "value": f"https://dp.la/item/{dpla_id}",
                                "type": "string",
                            },
                        }
                    ],
                    "P123": [
                        {
                            "snaktype": "value",
                            "property": "P123",
                            "datavalue": {
                                "value": {
                                    "entity-type": "item",
                                    "numeric-id": 2944483,
                                },
                                "type": "wikibase-entityid",
                            },
                        }
                    ],
                    "P813": [
                        {
                            "snaktype": "value",
                            "property": "P813",
                            "datavalue": {
                                "value": {
                                    "time": "+"
                                    + str(datetime.date.today())
                                    + "T00:00:00Z",
                                    "timezone": 0,
                                    "before": 0,
                                    "after": 0,
                                    "precision": 11,
                                    "calendarmodel": "http://www.wikidata.org/entity/Q1985727",
                                },
                                "type": "time",
                            },
                        }
                    ],
                }
            }
        ],
    }

    if value == "somevalue":
        claim["mainsnak"].pop("datavalue")
        claim["mainsnak"]["snaktype"] = "somevalue"

    return claim


# This is the function that will perform the POST to the Wikimedia Commons API, when passed all necessary parameters, to add a qualifier if it is missing for an existing claim. Currently, this is only used for P459. It creates a JSON object to send in the body of the request with the data to post and the login token.


def postqual(claimid, prop, value):
    global token
    summary = f"Adding [[:d:Property:{prop}]] to {claimid}."

    postdata = {
        "action": "wbsetqualifier",
        "format": "json",
        "claim": claimid,
        "property": prop,
        "snaktype": "value",
        "value": value,
        "token": token,
        "bot": True,
    }

    try:
        json.loads(
            http.fetch(
                "https://commons.wikimedia.org/w/api.php", method="POST", data=postdata
            ).text
        )
        pywikibot.output(summary)

    except Exception as e:
        # Refresh the global token and retry once. The previous version only
        # refreshed pywikibot's internal cache and didn't update the module
        # `token` (used in postdata above) nor retry the POST, so qualifiers
        # were silently dropped on any transient CSRF failure. Mirror the
        # token-refresh pattern used by process_one().
        print(repr(e))
        print(" -- Refreshing CSRF token after API error and retrying...")
        token = login()
        postdata["token"] = token
        try:
            json.loads(
                http.fetch(
                    "https://commons.wikimedia.org/w/api.php",
                    method="POST",
                    data=postdata,
                ).text
            )
            pywikibot.output(summary)
        except Exception as retry_e:
            print(f" -- Retry after token refresh failed: {repr(retry_e)}")


# This function performs an initial GET request on the given Wikimedia file to check if the statement we will be adding is already in the page. It returns a boolean, with True if the statement is not found and can be added. "qid" is passed as a tuple with both the value and the data type, so this check can handle the formatting for different data types. If statements are found in the entity with the prop and value, but no qualifiers, we return the statement id instead, so that the qualifier can be added to that statement instead of creating a new one using postqual().


# Per-file cache for wbgetentities, populated at the start of process_one()
# and consulted by check() for all subsequent add_* calls. Avoids ~25 redundant
# round-trips per file. Invalidate when claims change to keep the read-after-write
# semantics correct (process_one batches writes at the end, so a single fetch is
# safe for the duration of one file).
_entity_cache = {}


def get_entity(mediaid):
    """Return the wbgetentities response for mediaid, caching per process_one run."""
    cached = _entity_cache.get(mediaid)
    if cached is not None:
        return cached
    request = site.simple_request(action="wbgetentities", ids=mediaid)
    raw = request.submit()
    entity = raw.get("entities", {}).get(mediaid, {})
    _entity_cache[mediaid] = entity
    return entity


def invalidate_entity(mediaid):
    _entity_cache.pop(mediaid, None)


def check(mediaid, qid, prop):
    ref = ""
    existing_data = get_entity(mediaid)
    if not existing_data.get("pageid"):
        return True, ""
    try:
        if existing_data.get("statements").get(prop):
            statements = existing_data.get("statements").get(prop)
        else:
            return True, ""
    except Exception:
        return True, ""

    # The following code is used to check the existing statements that match the prop. If any statement matches the prop and qid but has no qualifiers, the statement id is returned. If there is a matching statement with qualifiers, return False. Otherwise (statements with matching prop have no matching qid) return True. This logic is not complete: it will return a statement id for a statement with no qualifier, even if another statement already has the desired qualifier. Also, it would return False even in cases where the qualifier value is different from the desired qualifier, in cases where there there is a matching qid and prop with qualifiers.
    if qid[0] == "item":
        if any(
            statement["mainsnak"]["datavalue"]["value"]["id"] == qid[1]
            and not statement.get("references")
            for statement in statements
        ):
            for statement in statements:
                if statement["mainsnak"]["datavalue"]["value"]["id"] == qid[
                    1
                ] and not statement.get("references"):
                    ref = statement["id"]
        if any(
            statement["mainsnak"]["datavalue"]["value"]["id"] == qid[1]
            and not statement.get("qualifiers")
            for statement in statements
        ):
            for statement in statements:
                if statement["mainsnak"]["datavalue"]["value"]["id"] == qid[
                    1
                ] and not statement.get("qualifiers"):
                    return add_det(mediaid, statement["id"]), ref

        elif any(
            statement["mainsnak"]["datavalue"]["value"]["id"] == qid[1]
            for statement in statements
        ):
            print(
                f" -- There already exists a statement with a {prop} > {qid[1]} claim for {mediaid}."
            )
            return False, ref

        else:
            return True, ref
    if qid[0] == "string":
        if any(
            statement["mainsnak"]["datavalue"]["value"] == qid[1]
            and not statement.get("references")
            for statement in statements
        ):
            for statement in statements:
                if statement["mainsnak"]["datavalue"]["value"] == qid[
                    1
                ] and not statement.get("references"):
                    ref = statement["id"]
        if any(
            statement["mainsnak"]["datavalue"]["value"] == qid[1]
            and not statement.get("qualifiers")
            for statement in statements
        ):
            for statement in statements:
                if statement["mainsnak"]["datavalue"]["value"] == qid[
                    1
                ] and not statement.get("qualifiers"):
                    return add_det(mediaid, statement["id"]), ref

        elif any(
            statement["mainsnak"]["datavalue"]["value"] == qid[1]
            for statement in statements
        ):
            print(
                f" -- There already exists a statement with a {prop} > {qid[1]} claim for {mediaid}."
            )
            return False, ref

        else:
            return True, ref
    if qid[0] == "monolingualtext":
        if any(
            statement["mainsnak"]["datavalue"]["value"]["text"] == qid[1]
            and not statement.get("references")
            for statement in statements
        ):
            for statement in statements:
                if statement["mainsnak"]["datavalue"]["value"]["text"] == qid[
                    1
                ] and not statement.get("references"):
                    ref = statement["id"]
        if any(
            statement["mainsnak"]["datavalue"]["value"]["text"] == qid[1]
            and not statement.get("qualifiers")
            for statement in statements
        ):
            for statement in statements:
                if statement["mainsnak"]["datavalue"]["value"]["text"] == qid[
                    1
                ] and not statement.get("qualifiers"):
                    return add_det(mediaid, statement["id"]), ref

        elif any(
            statement["mainsnak"]["datavalue"]["value"]["text"] == qid[1]
            for statement in statements
        ):
            print(
                f" -- There already exists a statement with a {prop} > {qid[1]} claim for {mediaid}."
            )
            return False, ref

        else:
            return True, ref
    if qid[0] == "somevalue":
        p = "P1932" if prop == "P571" else "P2093"
        try:
            if any(statement.get("qualifiers", {}).get(p) for statement in statements):
                for statement in statements:
                    qualifiers = statement.get("qualifiers", {}).get(p) or []
                    # Scan every qualifier entry, not just the first. Wikibase
                    # qualifier props may carry multiple values.
                    if any(
                        q.get("datavalue", {}).get("value") == qid[1]
                        for q in qualifiers
                    ) and not statement.get("references"):
                        ref = statement["id"]
                # Walk every statement looking for a qualifier match. Only
                # return False on an actual match; otherwise fall through and
                # return True once we've exhausted all statements. Returning
                # early on a non-match would skip later statements that DO
                # match and cause duplicate writes on multi-value properties
                # (creators, dates, etc.).
                for statement in statements:
                    qualifiers = statement.get("qualifiers", {}).get(p) or []
                    if any(
                        q.get("datavalue", {}).get("value") == qid[1]
                        for q in qualifiers
                    ):
                        print(
                            f" -- There already exists a statement with a {prop} > {qid[1]} claim for {mediaid}."
                        )
                        return False, ref
                return True, ref
            else:
                return True, ref
        except KeyError:
            return True, ref
    if qid[0] == "source":
        try:
            if any(
                statement.get("qualifiers", {}).get("P973") for statement in statements
            ):
                for statement in statements:
                    qualifiers = statement.get("qualifiers", {}).get("P973") or []
                    if any(
                        q.get("datavalue", {}).get("value") == qid[1]
                        for q in qualifiers
                    ) and not statement.get("references"):
                        ref = statement["id"]
                # See the somevalue branch above: scan every statement before
                # concluding the claim is missing.
                for statement in statements:
                    qualifiers = statement.get("qualifiers", {}).get("P973") or []
                    if any(
                        q.get("datavalue", {}).get("value") == qid[1]
                        for q in qualifiers
                    ):
                        print(
                            f" -- There already exists a statement with a {prop} > {qid[1]} claim for {mediaid}."
                        )
                        return False, ref
                return True, ref
            else:
                return True, ref
        except KeyError:
            return True, ref
    # Unrecognized qid type — treat claim as absent; no existing ref to update.
    print(
        f" -- check() fallback: unrecognized qid type '{qid[0]}' for {mediaid}, {prop}"
    )
    return True, ""


# The following functions define specific statements to add, and uses formattedclaim() to append them to the "claims" array. It first uses the check() to check if the statement is not yet in the item, and appends it the list of statements to add in the edit if not. check() returns True, False, or the string value of a statement id.


def add_rs(mediaid, rs, dpla_id):
    prop = None
    qid = None
    rs_key = _normalize_rights_uri(rs)
    rights_entry = rights.get(rs_key)
    if rights_entry:
        prop = list(rights_entry)[0]
        qid = rights_entry[prop]
        summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
        claim = formattedclaim(
            prop,
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
            "wikibase-entityid",
            dpla_id,
        )
        checkclaim = check(mediaid, ("item", qid), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(summary)
            claims["claims"].append(claim)
        if prop == "P275" and qid != "Q6938433":
            prop = "P6216"
            qid = "Q50423863"

        if prop == "P6426":
            prop = "P6216"
            qid = "Q19652"

        if qid == "Q6938433":
            prop = "P6216"
            qid = "Q88088423"

    if rs_key == "http://creativecommons.org/publicdomain/mark/1.0":
        prop = "P6216"
        qid = "Q19652"

    if prop is not None:
        summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
        claim = formattedclaim(
            prop,
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
            "wikibase-entityid",
            dpla_id,
        )
        checkclaim = check(mediaid, ("item", qid), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(summary)
            claims["claims"].append(claim)


def add_collection(mediaid, hub, institution, dpla_id):
    if hub == "Q518155":
        institution = hub
    if institution:
        qid = institution
        prop = "P195"
        summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
        claim = formattedclaim(
            prop,
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
            "wikibase-entityid",
            dpla_id,
        )
        checkclaim = check(mediaid, ("item", qid), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(summary)
            claims["claims"].append(claim)


def add_access(mediaid, access, dpla_id):
    if access:
        qid = access
        prop = "P7228"
        summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
        claim = formattedclaim(
            prop,
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
            "wikibase-entityid",
            dpla_id,
        )
        checkclaim = check(mediaid, ("item", qid), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(summary)
            claims["claims"].append(claim)


def add_level(mediaid, level, dpla_id):
    if level:
        qid = level
        prop = "P6224"
        summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
        claim = formattedclaim(
            prop,
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
            "wikibase-entityid",
            dpla_id,
        )
        checkclaim = check(mediaid, ("item", qid), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(summary)
            claims["claims"].append(claim)


def add_id(mediaid, id):
    prop = "P760"
    summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
    claim = formattedclaim(prop, id, "string", id)
    checkclaim = check(mediaid, ("string", id), prop)
    if checkclaim[1]:
        add_ref(checkclaim[1], claim)
    if checkclaim[0] is True:
        pywikibot.output(summary)
        claims["claims"].append(claim)


def add_naid(mediaid, naid, dpla_id):
    prop = "P1225"
    summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
    claim = formattedclaim(prop, naid, "string", dpla_id)
    checkclaim = check(mediaid, ("string", naid), prop)
    if checkclaim[1]:
        add_ref(checkclaim[1], claim)
    if checkclaim[0] is True:
        pywikibot.output(summary)
        claims["claims"].append(claim)


def add_subject(mediaid, subject, dpla_id):
    prop = "P4272"
    summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
    claim = formattedclaim(prop, subject, "string", dpla_id)
    checkclaim = check(mediaid, ("string", subject), prop)
    if checkclaim[1]:
        add_ref(checkclaim[1], claim)
    if checkclaim[0] is True:
        pywikibot.output(summary)
        claims["claims"].append(claim)


def add_subject_entity(mediaid, qid, dpla_id):
    prop = "P921"
    summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
    claim = formattedclaim(
        prop,
        {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
        "wikibase-entityid",
        dpla_id,
    )
    checkclaim = check(mediaid, ("item", qid), prop)
    if checkclaim[1]:
        add_ref(checkclaim[1], claim)
    if checkclaim[0] is True:
        pywikibot.output(summary)
        claims["claims"].append(claim)


def add_title(mediaid, title, dpla_id):
    if title:
        prop = "P1476"
        # Normalize once: dedupe must compare what we'd actually post, not the
        # raw input. Otherwise reruns where the raw string differs from the
        # truncated/rstripped form (long titles, trailing whitespace) treat the
        # claim as missing and post a duplicate. Also skip whitespace-only
        # values so we don't post empty claims.
        normalized = title[:1499].rstrip()
        if not normalized:
            return
        summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
        claim = formattedclaim(
            prop,
            {"text": normalized, "language": "en"},
            "monolingualtext",
            dpla_id,
        )
        checkclaim = check(mediaid, ("monolingualtext", normalized), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(summary)
            claims["claims"].append(claim)


def add_desc(mediaid, desc, dpla_id):
    if desc:
        prop = "P10358"
        normalized = desc[:1499].rstrip()
        if not normalized:
            return
        summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
        claim = formattedclaim(
            prop,
            {"text": normalized, "language": "en"},
            "monolingualtext",
            dpla_id,
        )
        checkclaim = check(mediaid, ("monolingualtext", normalized), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(summary)
            claims["claims"].append(claim)


def add_creator(mediaid, creator, dpla_id):
    if creator:
        prop = "P170"
        normalized = creator[:1499].rstrip()
        if not normalized:
            return
        summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
        claim = formattedclaim(prop, "somevalue", "wikibase-entityid", dpla_id)
        claim["qualifiers"]["P2093"] = [
            {
                "snaktype": "value",
                "property": "P2093",
                "datavalue": {"value": normalized, "type": "string"},
            }
        ]
        checkclaim = check(mediaid, ("somevalue", normalized), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(summary)
            claims["claims"].append(claim)


def add_date(mediaid, date, dpla_id):
    prop = "P571"
    normalized = date[:1499].rstrip()
    if not normalized:
        return
    summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
    claim = formattedclaim(prop, "somevalue", "time", dpla_id)
    claim["qualifiers"]["P1932"] = [
        {
            "snaktype": "value",
            "property": "P1932",
            "datavalue": {"value": normalized, "type": "string"},
        }
    ]
    checkclaim = check(mediaid, ("somevalue", normalized), prop)
    if checkclaim[1]:
        add_ref(checkclaim[1], claim)
    if checkclaim[0] is True:
        pywikibot.output(summary)
        claims["claims"].append(claim)


def add_contributed(mediaid, hub, institution, dpla_id):
    prop = "P9126"
    summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
    qid = "Q2944483"
    claim = formattedclaim(
        prop,
        {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
        "wikibase-entityid",
        dpla_id,
    )
    claim["qualifiers"]["P3831"] = [
        {
            "snaktype": "value",
            "property": "P3831",
            "datavalue": {
                "value": {"entity-type": "item", "numeric-id": 393351},
                "type": "wikibase-entityid",
            },
        }
    ]
    checkclaim = check(mediaid, ("item", qid), prop)
    if checkclaim[1]:
        add_ref(checkclaim[1], claim)
    if checkclaim[0] is True:
        pywikibot.output(summary)
        claims["claims"].append(claim)
    if hub == "Q518155":
        qid = "Q518155"
        claim = formattedclaim(
            prop,
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
            "wikibase-entityid",
            dpla_id,
        )
        claim["qualifiers"]["P3831"] = [
            {
                "snaktype": "value",
                "property": "P3831",
                "datavalue": {
                    "value": {"entity-type": "item", "numeric-id": 108296843},
                    "type": "wikibase-entityid",
                },
            }
        ]
        checkclaim = check(mediaid, ("item", qid), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(summary)
            claims["claims"].append(claim)
        qid = institution
        claim = formattedclaim(
            prop,
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
            "wikibase-entityid",
            dpla_id,
        )
        claim["qualifiers"]["P3831"] = [
            {
                "snaktype": "value",
                "property": "P3831",
                "datavalue": {
                    "value": {"entity-type": "item", "numeric-id": 108296919},
                    "type": "wikibase-entityid",
                },
            }
        ]
        checkclaim = check(mediaid, ("item", qid), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(summary)
            claims["claims"].append(claim)
    else:
        qid = hub
        claim = formattedclaim(
            prop,
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
            "wikibase-entityid",
            dpla_id,
        )
        claim["qualifiers"]["P3831"] = [
            {
                "snaktype": "value",
                "property": "P3831",
                "datavalue": {
                    "value": {"entity-type": "item", "numeric-id": 393351},
                    "type": "wikibase-entityid",
                },
            }
        ]
        checkclaim = check(mediaid, ("item", qid), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(summary)
            claims["claims"].append(claim)
        qid = institution
        claim = formattedclaim(
            prop,
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
            "wikibase-entityid",
            dpla_id,
        )
        claim["qualifiers"]["P3831"] = [
            {
                "snaktype": "value",
                "property": "P3831",
                "datavalue": {
                    "value": {"entity-type": "item", "numeric-id": 108296843},
                    "type": "wikibase-entityid",
                },
            }
        ]
        checkclaim = check(mediaid, ("item", qid), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(summary)
            claims["claims"].append(claim)


def add_local_id(mediaid, id, institution, dpla_id):
    if id:
        prop = "P217"
        summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
        claim = formattedclaim(prop, id, "string", dpla_id)
        checkclaim = check(mediaid, ("string", id), prop)
        claim["qualifiers"]["P195"] = [
            {
                "snaktype": "value",
                "property": "P195",
                "datavalue": {
                    "value": {
                        "entity-type": "item",
                        "numeric-id": int(institution.replace("Q", "")),
                    },
                    "type": "wikibase-entityid",
                },
            }
        ]
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(summary)
            claims["claims"].append(claim)


def add_source(mediaid, hub, url, dpla_id):
    qid = "Q74228490"
    prop = "P7482"
    summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
    claim = formattedclaim(
        prop,
        {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
        "wikibase-entityid",
        dpla_id,
    )
    claim["qualifiers"]["P973"] = [
        {
            "snaktype": "value",
            "property": "P973",
            "datavalue": {"value": url, "type": "string"},
            "datatype": "url",
        }
    ]
    claim["qualifiers"]["P137"] = [
        {
            "snaktype": "value",
            "property": "P137",
            "datavalue": {
                "value": {
                    "entity-type": "item",
                    "numeric-id": int(hub.replace("Q", "")),
                },
                "type": "wikibase-entityid",
            },
        }
    ]
    checkclaim = check(mediaid, ("source", url), prop)
    if checkclaim[1]:
        add_ref(checkclaim[1], claim)
    if checkclaim[0] is True:
        pywikibot.output(summary)
        claims["claims"].append(claim)


def add_det(mediaid, claimid):
    if claimid:
        qid = "Q61848113"
        prop = "P459"
        value = json.dumps(
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))}
        )
        postqual(claimid, prop, value)
        # postqual just mutated Commons state for this mediaid (added P459
        # to an existing claim). Drop the cached snapshot so any subsequent
        # check() call for the same mediaid in this run reads the fresh
        # post-write state instead of repeating the qualifier write.
        invalidate_entity(mediaid)


def add_ref(claimid, claim):
    if claimid:
        claim["id"] = claimid
        refclaims["claims"].append(claim)
        print(f" -- Adding reference for {claimid}.")


def dpla_claims(
    mediaid,
    dpla_id,
    url,
    descs,
    dates,
    titles,
    hub,
    local_ids,
    institution,
    rs,
    creators,
    subjects,
    naids,
    access,
    level,
):
    print(f" -- Accessing Commons ID {mediaid}")
    try:
        file_claims = requests.get(
            f"https://commons.wikimedia.org/wiki/Special:EntityData/{mediaid}.json"
        ).json()
    except Exception:
        file_claims = {"entities": {mediaid: {"statements": {}}}}
    print(f" -- Accessed Commons ID {mediaid}")
    dpla_claim_list = []
    removals = []
    rightsprop = "P6216"
    rightsvalue = ""
    statusvalue = ""
    rs_key = _normalize_rights_uri(rs)
    rights_entry = rights.get(rs_key)
    if rights_entry:
        rightsprop = list(rights_entry)[0]
        rightsvalue = rights_entry[rightsprop]

        if rightsprop == "P275":
            statusvalue = "Q50423863"

        if rightsprop == "P6426":
            statusvalue = "Q19652"

        if rightsvalue == "Q6938433":
            statusvalue = "Q88088423"

    if rs_key == "http://creativecommons.org/publicdomain/mark/1.0":
        statusvalue = "Q19652"

    parsesubjects = []
    parsetitles = []
    parsecreators = []
    parsedescs = []
    parsesubjectentities = []
    for subject in subjects:
        parsesubjects.append(subject[0][:1499].rstrip())
        if subject[1]:
            parsesubjectentities.append(subject[1][:1499].rstrip())
    for title in titles:
        parsetitles.append(title[:1499].rstrip())
    for creator in creators:
        parsecreators.append(creator[:1499].rstrip())
    for desc in descs:
        parsedescs.append(desc[:1499].rstrip())
    titles = parsetitles
    creators = parsecreators
    descs = parsedescs
    subjects = parsesubjects
    # All values are lists so the `value not in expected[prop]` reconciliation
    # below behaves consistently (a bare string would degrade `not in` into a
    # substring check and let real DPLA claims look "unexpected").
    expected = {
        "P217": local_ids,
        "P760": [dpla_id],
        "P1476": titles,
        "P195": ["Q518155" if hub == "Q518155" else institution],
        "P170": creators,
        "P9126": ["Q2944483", hub, institution],
        "P7482": [url],
        "P4272": subjects,
        "P571": dates,
        "P10358": descs,
        "P1225": naids,
        "P6224": [level],
        "P7228": [access],
        "P921": parsesubjectentities,
    }
    # P6216 (copyright status) and the rights-property (P275/P6426) need to
    # coexist: when rightsprop *is* P6216 (public-domain-mark path), a single
    # P6216 entry overwrote the other and caused dpla_claims() to wrongly
    # queue the correct status claim for removal. Build them separately.
    p6216_values = []
    if statusvalue:
        p6216_values.append(statusvalue)
    if rightsprop == "P6216":
        if rightsvalue:
            p6216_values.append(rightsvalue)
    else:
        if rightsvalue:
            expected[rightsprop] = [rightsvalue]
    expected["P6216"] = p6216_values
    for prop in file_claims["entities"][mediaid]["statements"]:
        for stmt in file_claims["entities"][mediaid]["statements"][prop]:
            if stmt.get("references"):
                if any(pubprop["snaks"].get("P123") for pubprop in stmt["references"]):
                    if any(
                        pubcheck["snaks"]["P123"][0]["datavalue"]["value"]["id"]
                        == "Q2944483"
                        for pubcheck in stmt["references"]
                        if pubcheck["snaks"].get("P123")
                    ):
                        if stmt["mainsnak"]["snaktype"] == "value":
                            dtype = stmt["mainsnak"]["datavalue"]["type"]
                            if stmt["mainsnak"]["property"] == "P7482":
                                try:
                                    dpla_claim_list.append(
                                        {
                                            stmt["mainsnak"]["property"]: {
                                                "id": stmt["id"],
                                                "value": stmt["qualifiers"]["P973"][0][
                                                    "datavalue"
                                                ]["value"],
                                            }
                                        }
                                    )
                                except (KeyError, IndexError, TypeError):
                                    # P7482 statement without a P973 qualifier — skip it
                                    pass
                            elif dtype == "wikibase-entityid":
                                dpla_claim_list.append(
                                    {
                                        stmt["mainsnak"]["property"]: {
                                            "id": stmt["id"],
                                            "value": stmt["mainsnak"]["datavalue"][
                                                "value"
                                            ]["id"],
                                        }
                                    }
                                )
                            elif dtype == "string":
                                dpla_claim_list.append(
                                    {
                                        stmt["mainsnak"]["property"]: {
                                            "id": stmt["id"],
                                            "value": stmt["mainsnak"]["datavalue"][
                                                "value"
                                            ],
                                        }
                                    }
                                )
                            elif dtype == "monolingualtext":
                                dpla_claim_list.append(
                                    {
                                        stmt["mainsnak"]["property"]: {
                                            "id": stmt["id"],
                                            "value": stmt["mainsnak"]["datavalue"][
                                                "value"
                                            ]["text"],
                                        }
                                    }
                                )
                        if stmt["mainsnak"]["snaktype"] == "somevalue":
                            p = "P1932" if prop == "P571" else "P2093"
                            try:
                                dpla_claim_list.append(
                                    {
                                        stmt["mainsnak"]["property"]: {
                                            "id": stmt["id"],
                                            "value": stmt["qualifiers"][p][0][
                                                "datavalue"
                                            ]["value"],
                                        }
                                    }
                                )
                            except (KeyError, IndexError, TypeError):
                                # somevalue claim missing its expected
                                # stated-as qualifier — queue for removal.
                                removals.append(stmt["id"])
    for claim in dpla_claim_list:
        for prop in claim:
            if prop not in expected:
                removals.append(claim[prop]["id"])
            elif claim[prop]["value"] not in expected[prop]:
                removals.append(claim[prop]["id"])
    if removals:
        rmdata = {
            "action": "wbremoveclaims",
            "format": "json",
            "id": mediaid,
            "claim": "|".join(removals),
            "token": token,
            "bot": True,
            "summary": f"Changing structured data claims from [[COM:DPLA|DPLA]] item '[[dpla:{dpla_id}|{dpla_id}]]'. [[COM:DPLA/MOD|Leave feedback]]!",
        }
        http.fetch(
            "https://commons.wikimedia.org/w/api.php", method="POST", data=rmdata
        )
        print(" --- Saved removals!")


def parsed(dpla_id, dpla_api):
    print(f" -- Accessing DPLA ID {dpla_id}")
    try:
        dpla = requests.get(
            f"https://api.dp.la/v2/items/{dpla_id}?api_key={dpla_api}",
            timeout=15,
        ).json()
    except Exception:
        print(" -- Sleeping 30 seconds and retrying...")
        time.sleep(30)
        dpla = requests.get(
            f"https://api.dp.la/v2/items/{dpla_id}?api_key={dpla_api}",
            timeout=15,
        ).json()
    print(f" -- Accessed DPLA ID {dpla_id}")

    try:
        dpla = dpla["docs"][0]
    except Exception:
        print(dpla)
        print("DPLA API returned error.")
        return None
    hub = hubs[dpla["provider"]["name"]]["Wikidata"]
    institution = hubs[dpla["provider"]["name"]]["institutions"][
        dpla["dataProvider"]["name"]
    ]["Wikidata"]
    titles = dpla["sourceResource"]["title"]
    if isinstance(titles, str):
        titles = [titles]
    rs = dpla["rights"]
    url = dpla["isShownAt"]

    try:
        dates = []
        for displaydate in dpla["sourceResource"]["date"]:
            dates.append(displaydate["displayDate"])
    except Exception:
        dates = []
    try:
        local_ids = dpla["sourceResource"]["identifier"]
        if isinstance(local_ids, str):
            local_ids = [local_ids]
    except Exception:
        local_ids = []
    try:
        descs = dpla["sourceResource"]["description"]
        if isinstance(descs, str):
            descs = [descs]
    except Exception:
        descs = []
    try:
        subjects = []
        # First pass: build the subject list and queue up any NARA exactMatch
        # entries for reconciliation. We reserve a slot in `subjects` for each
        # one and fill in the resolved Wikidata ID in a single batched HTTP
        # call below.
        reconci_slots = []  # list of (slot_index, name, naid)
        for subject in dpla["sourceResource"]["subject"]:
            added = False
            if subject.get("name") in subject_ids:
                for subjqid in subject_ids[subject.get("name")]["id"]:
                    if not (any(subjqid in i for i in subjects)):
                        subjects.append((str(subject.get("name")), subjqid))
                        added = True
                    if not (any(subject.get("name") in i for i in subjects)):
                        subjects.append((str(subject.get("name") or ""), ""))
                        added = True
            elif subject.get("exactMatch"):
                naid = subject.get("exactMatch")[0].replace(
                    "https://catalog.archives.gov/id/", ""
                )
                name = str(subject.get("name") or "")
                subjects.append([name, ""])  # mutable placeholder
                reconci_slots.append((len(subjects) - 1, name, naid))
                added = True
            if not added:
                subjects.append((str(subject.get("name") or ""), ""))

        if reconci_slots:
            queries = {
                f"q{i}": {
                    "query": name,
                    "limit": 5,
                    "properties": [{"pid": "P1225", "v": naid}],
                    "type_strict": "should",
                }
                for i, (_, name, naid) in enumerate(reconci_slots)
            }
            h = requests.get(
                "https://wikidata.reconci.link/en/api?queries="
                + urllib.parse.quote(json.dumps(queries)),
                timeout=15,
            )
            results = h.json()
            for i, (slot, name, _) in enumerate(reconci_slots):
                result = results.get(f"q{i}", {}).get("result") or []
                subjqid = result[0]["id"] if result else ""
                subjects[slot] = (name, subjqid)

        subjects = [tuple(s) for s in subjects]
    except Exception:
        subjects = []
    try:
        creators = dpla["sourceResource"]["creator"]
        if isinstance(creators, str):
            creators = [creators]
    except Exception:
        creators = []
    if dpla["provider"]["name"] == "National Archives and Records Administration":
        naids = dpla["sourceResource"]["identifier"]
        if isinstance(naids, str):
            naids = [naids]
        codes = {
            "10031403": "Q66739888",
            "10031402": "Q24238356",
            "10031399": "Q66739729",
            "10031400": "Q66739849",
            "10031401": "Q66739875",
        }
        levels = {"item": "Q11723795", "itemAv": "Q11723795", "fileUnit": "Q59221146"}
        access = ""
        level = ""
        try:
            xml = BeautifulSoup(dpla["originalRecord"]["stringValue"], "xml")
        except Exception as e:
            # No XML parser available (e.g. lxml missing the xml feature) — skip
            # NARA-specific access/level extraction rather than aborting the file.
            print(f" -- Skipping NARA XML parse for {dpla_id}: {e}")
            xml = None
        if xml is not None:
            try:
                acccess_naid = str(
                    xml.find("accessRestriction").find("status").find("naId").text
                )
                access = codes[acccess_naid]
            except Exception:
                access = ""
            for lvl_key in levels:
                if xml.find(lvl_key):
                    level = levels[lvl_key]
        local_ids = []
    else:
        naids = []
        access = ""
        level = ""

    return (
        url,
        descs,
        dates,
        titles,
        hub,
        local_ids,
        institution,
        rs,
        creators,
        subjects,
        naids,
        access,
        level,
    )


def login():
    tokenrequest = http.fetch(
        "https://commons.wikimedia.org/w/api.php?action=query&meta=tokens&type=csrf&format=json"
    )
    tokendata = json.loads(tokenrequest.text)
    token = tokendata.get("query").get("tokens").get("csrftoken")
    return token


token = login()


def _resolve_dpla_id(title, dpla_api):
    """Return the DPLA item ID for a Commons file title.

    Tries the standard DPLA filename pattern first; falls back to a NARA
    identifier lookup for National Archives files. Returns the title unchanged
    if neither resolves (parsed() will record it as a missing ID).

    The NARA fallback hits the DPLA search API; any network, timeout, or
    unexpected-payload failure here would otherwise abort the entire --cat
    batch on a single bad file, so we catch broadly, log, and fall through
    to the title-passthrough.
    """
    dpla_id = extract_dpla_id_from_commons_title(title)
    if dpla_id:
        return dpla_id
    print("Detecting NARA identifier...")
    nara_id = re.sub(r"^.*NARA - (.*?)[\.| ].*$", r"\1", title)
    try:
        nara_item = requests.get(
            f'https://api.dp.la/v2/items?api_key={dpla_api}&provider.@id="http://dp.la/api/contributor/nara"&sourceResource.identifier="{nara_id}"',
            timeout=15,
        ).json()
    except (requests.RequestException, json.JSONDecodeError) as e:
        print(f" -- NARA lookup failed for {nara_id!r}: {e!r}")
        return title
    docs = nara_item.get("docs") if isinstance(nara_item, dict) else None
    if isinstance(docs, list) and len(docs) == 1:
        return docs[0].get("id") or title
    return title


def process_one(mediaid, dpla_id):
    """Fetch DPLA metadata and sync SDC claims for a single Commons file."""
    global claims, refclaims, token

    # Drop any stale cache from a prior file so check() always reads fresh state
    # for this mediaid.
    invalidate_entity(mediaid)
    # Pre-warm the per-file entity cache so the ~25 add_*/check() calls below
    # share a single wbgetentities round-trip.
    get_entity(mediaid)

    # parsed() returns None on lookup failure (was: returned False and the
    # tuple-unpack TypeError was caught here, which also swallowed real
    # parser bugs as "missing ID"). Check the sentinel explicitly.
    parsed_result = parsed(dpla_id, dpla_api)
    if not parsed_result:
        with open("Missing ids.txt", "a") as missing:
            missing.write(dpla_id + "\n")
            print(" -- Missing ID recorded.")
        return
    (
        url,
        descs,
        dates,
        titles,
        hub,
        local_ids,
        institution,
        rs,
        creators,
        subjects,
        naids,
        access,
        level,
    ) = parsed_result

    claims = {"claims": []}
    refclaims = {"claims": []}

    try:
        add_rs(mediaid, rs, dpla_id)
    except pywikibot.exceptions.APIError:
        print(" -- No such file on Commons.")
        return
    add_id(mediaid, dpla_id)
    for title in titles:
        add_title(mediaid, title.rstrip(), dpla_id)
    add_collection(mediaid, hub, institution, dpla_id)
    for creator in creators:
        add_creator(mediaid, creator.rstrip(), dpla_id)
    for date in dates:
        add_date(mediaid, date.rstrip(), dpla_id)
    for subject in subjects:
        add_subject(mediaid, subject[0], dpla_id)
        if subject[1]:
            add_subject_entity(mediaid, subject[1], dpla_id)
    for desc in descs:
        add_desc(mediaid, desc.rstrip(), dpla_id)
    add_contributed(mediaid, hub, institution, dpla_id)
    add_source(mediaid, hub, url, dpla_id)
    for local_id in local_ids:
        if len(local_id) < 1501:
            add_local_id(mediaid, local_id, institution, dpla_id)
    for naid in naids:
        add_naid(mediaid, naid, dpla_id)
    add_access(mediaid, access, dpla_id)
    add_level(mediaid, level, dpla_id)

    if refclaims["claims"]:
        postrefs = {
            "action": "wbeditentity",
            "format": "json",
            "id": mediaid,
            "data": json.dumps(refclaims),
            "token": token,
            "bot": True,
            "summary": f"Added structured data references from [[COM:DPLA|DPLA]] item '[[dpla:{dpla_id}|{dpla_id}]]'. [[COM:DPLA/MOD|Leave feedback]]!",
        }
        save = http.fetch(
            "https://commons.wikimedia.org/w/api.php",
            method="POST",
            data=postrefs,
        )
        try:
            post = json.loads(save.text)
            if post["success"] == 1:
                print(" --- Saved new refs!")
            else:
                print(post)
                print(" --- Error encountered on save.")
                sys.exit()
        except Exception:
            try:
                token = login()
                postrefs["token"] = token
                save = http.fetch(
                    "https://commons.wikimedia.org/w/api.php",
                    method="POST",
                    data=postrefs,
                )
                post = json.loads(save.text)
                if post["success"] == 1:
                    print(" --- Saved new refs!")
                else:
                    print(post)
                    print(" --- Error encountered on save.")
                    sys.exit()
            except Exception:
                print(" --- Error encountered. 2")
                sys.exit()

    postdata = {
        "action": "wbeditentity",
        "format": "json",
        "id": mediaid,
        "data": json.dumps(claims),
        "token": token,
        "bot": True,
        "summary": f"Added structured data claims from [[COM:DPLA|DPLA]] item '[[dpla:{dpla_id}|{dpla_id}]]'. [[COM:DPLA/MOD|Leave feedback]]!",
    }

    if claims["claims"]:
        try:
            save = http.fetch(
                "https://commons.wikimedia.org/w/api.php",
                method="POST",
                data=postdata,
            )
        except (requests.exceptions.ConnectionError, ConnectionError):
            try:
                save = http.fetch(
                    "https://commons.wikimedia.org/w/api.php",
                    method="POST",
                    data=postdata,
                )
            except (requests.exceptions.ConnectionError, ConnectionError):
                save = http.fetch(
                    "https://commons.wikimedia.org/w/api.php",
                    method="POST",
                    data=postdata,
                )
        try:
            post = json.loads(save.text)
            if post["success"] == 1:
                print(" --- Saved new claims!")
            else:
                print(post)
                print(" --- Error encountered on save.")
                sys.exit()
        except Exception:
            try:
                token = login()
                postdata["token"] = token
                save = http.fetch(
                    "https://commons.wikimedia.org/w/api.php",
                    method="POST",
                    data=postdata,
                )
                post = json.loads(save.text)
                if post["success"] == 1:
                    print(" --- Saved new claims!")
                else:
                    print(post)
                    print(" --- Error encountered on save.")
                    sys.exit()
            except Exception as retry_e:
                # `post` may not be assigned yet if http.fetch / json.loads
                # raised — referencing it directly would mask the real
                # error with an UnboundLocalError. Log what we actually have.
                print(f" --- Retry after token refresh failed: {retry_e!r}")
                if "save" in locals():
                    print(save.text)
                print(" --- Error encountered. 3")
                sys.exit()

    dpla_claims(
        mediaid,
        dpla_id,
        url,
        descs,
        dates,
        titles,
        hub,
        local_ids,
        institution,
        rs,
        creators,
        subjects,
        naids,
        access,
        level,
    )


# We can use a PWB generator to programatically make the list of files we are working on based on a set of criteria. Here, we are generating the page titles from a Wikimedia Commons search and categories. For other types of available page generators, see <https://doc.wikimedia.org/pywikibot/master/api_ref/pywikibot.html#module-pywikibot.pagegenerators>. As an additional step, we take the pageid provided by the generator and prepend "M" for the mediaid needed for posting SDC statements.

count = 0

if method == "list":
    ltotal = [i for i in os.listdir(args.lists) if ".txt" in i]
    lists = [i for i in ltotal if "COMPLETE" not in i and "WORKING" not in i]
    percent = 100 * (len(ltotal) - len(lists)) / len(ltotal) if ltotal else 0
    while lists:
        # range(0, len(lists)-1) is exclusive on the upper bound, so the
        # previous expression never selected the last index — fixed.
        x = random.randrange(len(lists))
        working_file = os.path.join(args.lists, "WORKING-" + lists[x])
        print(working_file)
        os.rename(os.path.join(args.lists, lists[x]), working_file)

        files = pagegenerators.TextIOPageGenerator(working_file)

        for file in files:
            count += 1
            print(f"{count}:\n - {args.lists}/{lists[x]} ({percent:.2f}% done)")
            print("\n" + str(file).replace('""', '"'))
            mediaid = "M" + str(file.pageid)
            dpla_id = _resolve_dpla_id(str(file), dpla_api)
            process_one(mediaid, dpla_id)

        os.rename(working_file, os.path.join(args.lists, "COMPLETE-" + lists[x]))

        ltotal = [i for i in os.listdir(args.lists) if ".txt" in i]
        lists = [i for i in ltotal if "COMPLETE" not in i and "WORKING" not in i]
        percent = 100 * (len(ltotal) - len(lists)) / len(ltotal) if ltotal else 0

        duduped = set()
        try:
            with open("Missing ids.txt", "r") as f:
                for line in f:
                    duduped.add(line.strip())
            with open("Missing ids.txt", "w") as f:
                f.write("\n".join(duduped) + "\n")
        except FileNotFoundError:
            # No missing IDs recorded this batch; nothing to dedupe.
            pass

elif args.files:
    for title in args.files:
        print("\n" + title)
        page = pywikibot.Page(site, title)
        if not page.exists():
            print(f" -- Page not found on Commons: {title}")
            continue
        mediaid = "M" + str(page.pageid)
        dpla_id = _resolve_dpla_id(title, dpla_api)
        count += 1
        print(f"{count}: {mediaid}")
        process_one(mediaid, dpla_id)

elif args.cat:
    category = pywikibot.Category(site, args.cat)
    generator = pagegenerators.CategorizedPageGenerator(
        category, namespaces=[6], recurse=args.recurse
    )
    for page in generator:
        title = page.title()
        print("\n" + title)
        mediaid = "M" + str(page.pageid)
        dpla_id = _resolve_dpla_id(title, dpla_api)
        count += 1
        print(f"{count}: {mediaid}")
        process_one(mediaid, dpla_id)
        if args.limit and count >= args.limit:
            print(f" -- Reached --limit {args.limit}, stopping.")
            break
