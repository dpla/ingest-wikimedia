# TODO caption, date, page, iiif manifest, url

import pywikibot, requests, re, json, datetime, argparse, random, os, sys, time, unicodedata
from bs4 import BeautifulSoup
from pywikibot.comms import http
from pywikibot import pagegenerators


# DPLA API key
key = "XXX"

# When running manually, sometimes it is helpful to specify the category to work on in the command line, using --cat "<category>".

parser = argparse.ArgumentParser()
parser.add_argument("--cat", dest="cat", metavar="CAT", action="store")
parser.add_argument("--method", dest="method", metavar="METHOD", action="store")
parser.add_argument("--lists", dest="lists", metavar="LISTS", action="store")
args = parser.parse_args()

method = "livecat"
if args.method:
    method = args.method

hubs = json.loads(
    requests.get(
        "https://raw.githubusercontent.com/dpla/ingestion3/develop/src/main/resources/wiki/institutions_v2.json"
    ).text
)

rights = json.load(open("rights.json"))
subject_ids = json.loads(
    requests.get(
        "https://raw.githubusercontent.com/DominicBM/ingestion3/develop/src/main/resources/subjects.json"
    ).text
)

# This is the JSON used for formatting a claim. The P459 -> Q61848113 (determination method) qualifier is hardcoded in for everything DPLA adds. Not all data types have the same format for value, so this is formatted in the function for each property added.


def formattedclaim(prop, value, type, dpla_id):
    claim = {
        "mainsnak": {
            "snaktype": "value",
            "property": prop,
            "datavalue": {"value": value, "type": type},
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
                                "value": "https://dp.la/item/" + dpla_id,
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


# This is the function that will perform the POST to the Wikimedia Commons API, when passed all necessary parameters, to add a qualifier if it is missing for anexisting claim. Currently, this is only used for P459. It creates a JSON object to send in the body of the request with the data to post and the login token.


def postqual(claimid, prop, value):

    summary = "Adding [[:d:Property:" + prop + "]] to " + claimid + "."

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
        qual = json.loads(
            http.fetch(
                "https://commons.wikimedia.org/w/api.php", method="POST", data=postdata
            ).text
        )
        pywikibot.output(summary)

    except Exception as e:
        print(repr(e))
        #         print(qual)
        print(site.tokens["csrf"])
        site.get_tokens(["csrf"])
        site.tokens.load_tokens(["csrf"])
        print(site.tokens["csrf"])


# This function performs an initial GET request on the given Wikimedia file to check if the statement we will be adding is already in the page. It returns a boolean, with True if the statement is not found and can be added. "qid" is passed as a tuple with both the value and the data type, so this check can handle the formatting for different data types. If statements are found in the entity with the prop and value, but no qualifiers, we return the statement id instead, so that the qualifier can be added to that statement instead of creating a new one using postqual().


def check(mediaid, qid, prop):
    request = site.simple_request(action="wbgetentities", ids=mediaid)

    bool = False
    ref = ""
    ret = bool, ref
    raw = request.submit()

    if raw.get("entities").get(mediaid).get("pageid"):
        existing_data = raw.get("entities").get(mediaid)
    else:
        return True, ""
        exit()
    try:
        if existing_data.get("statements").get(prop):
            statements = existing_data.get("statements").get(prop)
        else:
            return True, ""
            exit()
    except:
        return True, ""
        exit()

    # The following code is used to check the existing statements that match the prop. If any statement matches the prop and qid but has no qualifiers, the statement id is returned. If there is a matching statement with qualifiers, return False. Otherwise (statements with matching prop have no matching qid) return True. This logic is not complete: it will return a statement id for a statement with no qualifier, even if another statement already has the desired qualifier. Also, it would return False even in cases where the qualifier value is different from the desired qualifier, in cases where there there is a matching qid and prop with qualifiers.
    #     statement['mainsnak']['datavalue']['value'] == qid
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
                    return add_det(statement["id"]), ref
                    exit()

        elif any(
            statement["mainsnak"]["datavalue"]["value"]["id"] == qid[1]
            for statement in statements
        ):
            print(
                " -- There already exists a statement with a "
                + prop
                + " > "
                + qid[1]
                + " claim for "
                + mediaid
                + "."
            )
            #         print(existing_data.get('statements'))
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
                    return add_det(statement["id"]), ref
                    exit()

        elif any(
            statement["mainsnak"]["datavalue"]["value"] == qid[1]
            for statement in statements
        ):
            print(
                " -- There already exists a statement with a "
                + prop
                + " > "
                + qid[1]
                + " claim for "
                + mediaid
                + "."
            )
            #         print(existing_data.get('statements'))
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
                    return add_det(statement["id"]), ref
                    exit()

        elif any(
            statement["mainsnak"]["datavalue"]["value"]["text"] == qid[1]
            for statement in statements
        ):
            print(
                " -- There already exists a statement with a "
                + prop
                + " > "
                + qid[1]
                + " claim for "
                + mediaid
                + "."
            )
            #         print(existing_data.get('statements'))
            return False, ref

        else:
            return True, ref
    if qid[0] == "somevalue":
        try:
            if any(statement["qualifiers"].get("P2093") for statement in statements):
                for statement in statements:
                    if statement["qualifiers"]["P2093"][0]["datavalue"]["value"] == qid[
                        1
                    ] and not statement.get("references"):
                        ref = statement["id"]
                for statement in statements:
                    try:
                        if (
                            statement["qualifiers"]["P2093"][0]["datavalue"]["value"]
                            == qid[1]
                        ):
                            print(
                                " -- There already exists a statement with a "
                                + prop
                                + " > "
                                + qid[1]
                                + " claim for "
                                + mediaid
                                + "."
                            )
                            #         print(existing_data.get('statements'))
                            return False, ref

                        else:
                            return True, ref
                    except:
                        pass
            else:
                return True, ref
        except KeyError:
            return True, ref
    if qid[0] == "source":
        try:
            if any(statement["qualifiers"].get("P973") for statement in statements):
                for statement in statements:
                    if statement["qualifiers"]["P973"][0]["datavalue"]["value"] == qid[
                        1
                    ] and not statement.get("references"):
                        ref = statement["id"]
                for statement in statements:
                    try:
                        if (
                            statement["qualifiers"]["P973"][0]["datavalue"]["value"]
                            == qid[1]
                        ):
                            print(
                                " -- There already exists a statement with a "
                                + prop
                                + " > "
                                + qid[1]
                                + " claim for "
                                + mediaid
                                + "."
                            )
                            #         print(existing_data.get('statements'))
                            return False, ref

                        else:
                            return True, ref
                    except:
                        pass
            else:
                return True, ref
        except KeyError:
            return True, ref


# The following functions define specific statements to add, and uses formattedclaim() to append them to the "claims" array. It first uses the check() to check if the statement is not yet in the item, and appends it the list of statements to add in the edit if not. For now, we are just hardcoding actual values which are the same for all edits. check() returns True, False, or the string value of a statement id.


def add_rs(mediaid, rs, dpla_id):
    if rights.get(rs):
        prop = list(rights[rs].keys())[0]
        qid = rights[rs][prop]
        summary = " -- Adding [[:d:Property:" + prop + "]] to " + mediaid + "."
        claim = formattedclaim(
            prop,
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
            "wikibase-entityid",
            dpla_id,
        )
        checkclaim = check(mediaid, ("item", qid), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] == True:
            pywikibot.output(summary)
            claims["claims"].append(claim)
        #             return claim
        if prop == "P275" and not qid == "Q6938433":
            prop = "P6216"
            qid = "Q50423863"

        if prop == "P6426":
            prop = "P6216"
            qid = "Q19652"

        if qid == "Q6938433":
            prop = "P6216"
            qid = "Q88088423"

    if rs == "http://creativecommons.org/publicdomain/mark/1.0/":
        prop = "P6216"
        qid = "Q19652"

    if rs:
        summary = " -- Adding [[:d:Property:" + prop + "]] to " + mediaid + "."
        claim = formattedclaim(
            prop,
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
            "wikibase-entityid",
            dpla_id,
        )
        checkclaim = check(mediaid, ("item", qid), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] == True:
            pywikibot.output(summary)
            claims["claims"].append(claim)
            return claim


def add_collection(mediaid, hub, institution, dpla_id):
    if hub == "Q518155":
        institution = hub
    if institution:
        qid = institution
        prop = "P195"
        summary = " -- Adding [[:d:Property:" + prop + "]] to " + mediaid + "."
        claim = formattedclaim(
            prop,
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
            "wikibase-entityid",
            dpla_id,
        )
        checkclaim = check(mediaid, ("item", qid), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] == True:
            pywikibot.output(summary)
            claims["claims"].append(claim)
            return claim


def add_access(mediaid, access, dpla_id):
    if access:
        qid = access
        prop = "P7228"
        summary = " -- Adding [[:d:Property:" + prop + "]] to " + mediaid + "."
        claim = formattedclaim(
            prop,
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
            "wikibase-entityid",
            dpla_id,
        )
        checkclaim = check(mediaid, ("item", qid), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] == True:
            pywikibot.output(summary)
            claims["claims"].append(claim)
            return claim


def add_level(mediaid, level, dpla_id):
    if level:
        qid = level
        prop = "P6224"
        summary = " -- Adding [[:d:Property:" + prop + "]] to " + mediaid + "."
        claim = formattedclaim(
            prop,
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
            "wikibase-entityid",
            dpla_id,
        )
        checkclaim = check(mediaid, ("item", qid), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] == True:
            pywikibot.output(summary)
            claims["claims"].append(claim)
            return claim


def add_parent(mediaid, parent, dpla_id):
    if institution:
        qid = institution
        prop = "P195"
        summary = " -- Adding [[:d:Property:" + prop + "]] to " + mediaid + "."
        claim = formattedclaim(
            prop,
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
            "wikibase-entityid",
            dpla_id,
        )
        checkclaim = check(mediaid, ("item", qid), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] == True:
            pywikibot.output(summary)
            claims["claims"].append(claim)
            return claim


def add_id(mediaid, id):
    prop = "P760"
    summary = " -- Adding [[:d:Property:" + prop + "]] to " + mediaid + "."
    claim = formattedclaim(prop, id, "string", id)
    checkclaim = check(mediaid, ("string", id), prop)
    if checkclaim[1]:
        add_ref(checkclaim[1], claim)
    if checkclaim[0] == True:
        pywikibot.output(summary)
        claims["claims"].append(claim)
        return claim


def add_naid(mediaid, naid, dpla_id):
    prop = "P1225"
    summary = " -- Adding [[:d:Property:" + prop + "]] to " + mediaid + "."
    claim = formattedclaim(prop, naid, "string", dpla_id)
    checkclaim = check(mediaid, ("string", naid), prop)
    if checkclaim[1]:
        add_ref(checkclaim[1], claim)
    if checkclaim[0] == True:
        pywikibot.output(summary)
        claims["claims"].append(claim)
        return claim


def add_subject(mediaid, subject, dpla_id):
    prop = "P4272"
    summary = " -- Adding [[:d:Property:" + prop + "]] to " + mediaid + "."
    claim = formattedclaim(prop, subject, "string", dpla_id)
    checkclaim = check(mediaid, ("string", subject), prop)
    if checkclaim[1]:
        add_ref(checkclaim[1], claim)
    if checkclaim[0] == True:
        pywikibot.output(summary)
        claims["claims"].append(claim)
        return claim


def add_subject_entity(mediaid, qid, dpla_id):
    prop = "P921"
    summary = " -- Adding [[:d:Property:" + prop + "]] to " + mediaid + "."

    claim = formattedclaim(
        prop,
        {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
        "wikibase-entityid",
        dpla_id,
    )
    checkclaim = check(mediaid, ("item", qid), prop)
    if checkclaim[1]:
        add_ref(checkclaim[1], claim)
    if checkclaim[0] == True:
        pywikibot.output(summary)
        claims["claims"].append(claim)
        return claim


def add_title(mediaid, title, dpla_id):
    if title:
        title = title
        prop = "P1476"
        summary = " -- Adding [[:d:Property:" + prop + "]] to " + mediaid + "."
        claim = formattedclaim(
            prop,
            {"text": title[:1499].rstrip(), "language": "en"},
            "monolingualtext",
            dpla_id,
        )
        checkclaim = check(mediaid, ("monolingualtext", title), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] == True:
            pywikibot.output(summary)
            claims["claims"].append(claim)
            return claim


def add_desc(mediaid, desc, dpla_id):
    if desc:
        desc = desc
        prop = "P10358"
        summary = " -- Adding [[:d:Property:" + prop + "]] to " + mediaid + "."
        claim = formattedclaim(
            prop,
            {"text": desc[:1499].rstrip(), "language": "en"},
            "monolingualtext",
            dpla_id,
        )
        checkclaim = check(mediaid, ("monolingualtext", desc), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] == True:
            pywikibot.output(summary)
            claims["claims"].append(claim)
            return claim


def add_creator(mediaid, creator, dpla_id):
    if creator:
        prop = "P170"
        summary = " -- Adding [[:d:Property:" + prop + "]] to " + mediaid + "."
        claim = formattedclaim(prop, "somevalue", "wikibase-entityid", dpla_id)
        claim["qualifiers"]["P2093"] = [
            {
                "snaktype": "value",
                "property": "P2093",
                "datavalue": {"value": creator[:1499].rstrip(), "type": "string"},
            }
        ]
        checkclaim = check(mediaid, ("somevalue", creator), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] == True:
            pywikibot.output(summary)
            claims["claims"].append(claim)
            return claim


# This will catch when displayDate is a single year or a date.
def add_date(mediaid, date, dpla_id):

    prop = "P170"
    summary = " -- Adding [[:d:Property:" + prop + "]] to " + mediaid + "."
    if re.match("^[0-9]{1,4}$", str(date)):
        if int(date) < 2025:
            datetime.datetime.strptime("%Y")
            claim = formattedclaim(prop, date, "time", dpla_id)
            checkclaim = check(mediaid, ("time", date), prop)

    if checkclaim[1]:
        add_ref(checkclaim[1], claim)
    if checkclaim[0] == True:
        pywikibot.output(summary)
        claims["claims"].append(claim)
        return claim


def add_contributed(mediaid, hub, institution, dpla_id):
    prop = "P9126"
    summary = " -- Adding [[:d:Property:" + prop + "]] to " + mediaid + "."
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
    if checkclaim[0] == True:
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
        if checkclaim[0] == True:
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
        if checkclaim[0] == True:
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
        if checkclaim[0] == True:
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
        if checkclaim[0] == True:
            pywikibot.output(summary)
            claims["claims"].append(claim)


def add_local_id(mediaid, id, institution, dpla_id):
    if id:
        prop = "P217"
        summary = " -- Adding [[:d:Property:" + prop + "]] to " + mediaid + "."
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
        if checkclaim[0] == True:
            pywikibot.output(summary)
            claims["claims"].append(claim)
            return claim


def add_source(mediaid, hub, url, dpla_id):
    qid = "Q74228490"
    prop = "P7482"
    summary = " -- Adding [[:d:Property:" + prop + "]] to " + mediaid + "."
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
    if checkclaim[0] == True:
        pywikibot.output(summary)
        claims["claims"].append(claim)
        return claim


def add_det(claimid):
    if claimid:
        qid = "Q61848113"
        prop = "P459"
        value = json.dumps(
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))}
        )
        postqual(claimid, prop, value)


def add_ref(claimid, claim):
    if claimid:
        claim["id"] = claimid
        refclaims["claims"].append(claim)
        print(" -- Adding reference for " + claimid + ".")


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
    print(" -- Accessing Commons ID " + mediaid)
    try:
        file_claims = json.loads(
            requests.get(
                "https://commons.wikimedia.org/wiki/Special:EntityData/"
                + mediaid
                + ".json"
            ).text
        )
    except:
        file_claims = {}
        file_claims["entities"] = {mediaid: {"statements": {}}}
    print(" -- Accessed Commons ID " + mediaid)
    dpla_claims = []
    removals = []
    if rights.get(rs):
        rightsprop = list(rights[rs].keys())[0]
        rightsvalue = rights[rs][rightsprop]

        if rightsprop == "P275":
            statusvalue = "Q50423863"

        if rightsprop == "P6426":
            statusvalue = "Q19652"

        if rightsvalue == "Q6938433":
            statusvalue = "Q88088423"

    if rs == "http://creativecommons.org/publicdomain/mark/1.0/":
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
    props = [
        "P6216",
        "P275",
        "P217",
        "P6426",
        "P760",
        "P1476",
        "P195",
        "P170",
        "P9126",
        "P7482",
        "P4272",
        "P571",
        "P10358",
        "P1225",
        "P7228",
        "P6224",
        "P921",
    ]
    claims = {
        "P6216": statusvalue,
        rightsprop: rightsvalue,
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
    for prop in file_claims["entities"][mediaid]["statements"].keys():
        for stmt in file_claims["entities"][mediaid]["statements"][prop]:
            if stmt.get("references"):
                if any(
                    pub["snaks"]["P123"][0]["datavalue"]["value"]["id"] == "Q2944483"
                    for pub in stmt["references"]
                ):
                    if stmt["mainsnak"]["snaktype"] == "value":
                        type = stmt["mainsnak"]["datavalue"]["type"]
                        if stmt["mainsnak"]["property"] == "P7482":
                            try:
                                dpla_claims.append(
                                    {
                                        stmt["mainsnak"]["property"]: {
                                            "id": stmt["id"],
                                            "value": stmt["qualifiers"]["P973"][0][
                                                "datavalue"
                                            ]["value"],
                                        }
                                    }
                                )
                            except:
                                pass
                        elif type == "wikibase-entityid":
                            dpla_claims.append(
                                {
                                    stmt["mainsnak"]["property"]: {
                                        "id": stmt["id"],
                                        "value": stmt["mainsnak"]["datavalue"]["value"][
                                            "id"
                                        ],
                                    }
                                }
                            )
                        elif type == "string":
                            dpla_claims.append(
                                {
                                    stmt["mainsnak"]["property"]: {
                                        "id": stmt["id"],
                                        "value": stmt["mainsnak"]["datavalue"]["value"],
                                    }
                                }
                            )
                        elif type == "monolingualtext":
                            dpla_claims.append(
                                {
                                    stmt["mainsnak"]["property"]: {
                                        "id": stmt["id"],
                                        "value": stmt["mainsnak"]["datavalue"]["value"][
                                            "text"
                                        ],
                                    }
                                }
                            )
                    if stmt["mainsnak"]["snaktype"] == "somevalue":
                        try:
                            dpla_claims.append(
                                {
                                    stmt["mainsnak"]["property"]: {
                                        "id": stmt["id"],
                                        "value": stmt["qualifiers"]["P2093"][0][
                                            "datavalue"
                                        ]["value"],
                                    }
                                }
                            )
                        except:
                            removals.append(stmt["id"])
    for claim in dpla_claims:
        for prop in claim.keys():
            if prop not in claims.keys():
                removals.append(claim[prop]["id"])
            elif claim[prop]["value"] not in claims[prop]:
                #                 if any(clm[:1499].replace('\xa0',' ').rstrip() == claim[prop]['value'] for clm in claims[prop]):
                removals.append(claim[prop]["id"])
    if len(removals) > 0:
        rmdata = {
            "action": "wbremoveclaims",
            "format": "json",
            "id": mediaid,
            "claim": "|".join(removals),
            "token": token,
            "bot": True,
            "summary": "Changing structured data claims from [[COM:DPLA|DPLA]] item '[[dpla:"
            + dpla_id
            + "|"
            + dpla_id
            + "]]'. [[COM:DPLA/MOD|Leave feedback]]!",
        }

        save = http.fetch(
            "https://commons.wikimedia.org/w/api.php", method="POST", data=rmdata
        )
        print(" --- Saved removals!")


def parsed(dpla_id, key):

    print(" -- Accessing DPLA ID " + dpla_id)
    try:
        dpla = json.loads(
            requests.get(
                "https://api.dp.la/v2/items/"
                + dpla_id
                + "?api_key=" + key,
                timeout=15,
            ).text
        )
    except:
        print(" -- Sleeping 30 seconds and retrying...")
        time.sleep(30)
        dpla = json.loads(
            requests.get(
                "https://api.dp.la/v2/items/"
                + dpla_id
                + "?api_key=" + key
            ).text
        )
    print(" -- Accessed DPLA ID " + dpla_id)

    try:
        dpla = dpla["docs"][0]
    except:
        print(dpla)
        print("DPLA API returned error.")
        return False
    hub = hubs[dpla["provider"]["name"]]["Wikidata"]
    institution = hubs[dpla["provider"]["name"]]["institutions"][
        dpla["dataProvider"]["name"]
    ]["Wikidata"]
    titles = dpla["sourceResource"]["title"]
    rs = dpla["rights"]
    url = dpla["isShownAt"]

    try:
        dates = dpla["sourceResource"]["date"]
    except:
        dates = ""
    try:
        local_ids = dpla["sourceResource"]["identifier"]
    except:
        local_ids = ""
    try:
        descs = dpla["sourceResource"]["description"]
    except:
        descs = ""
    try:
        subjects = []
        for subject in dpla["sourceResource"]["subject"]:
            added = False
            # print(subject.get('name'))
            if subject.get("name") in subject_ids:
                for subjqid in subject_ids[subject.get("name")]["id"]:
                    # print((str(subject.get('name')), subjqid))
                    if not (any(subjqid in i for i in subjects)):
                        # print((str(subject.get('name')), subjqid))
                        subjects.append((str(subject.get("name")), subjqid))
                        added = True
                    if not (any(subject.get("name") in i for i in subjects)):
                        subjects.append((str(subject.get("name") or ""), ""))
                        # print((str(subject.get('name') or ''), ''))
                        added = True
            elif subject.get("exactMatch"):
                subjqid = ""
                naid = subject.get("exactMatch")[0].replace(
                    "https://catalog.archives.gov/id/", ""
                )
                h = requests.get(
                    "https://wikidata.reconci.link/en/api?queries=%7B%0A%20%20%22q1%22%3A%20%7B%0A%20%20%20%20%22query%22%3A%20%22"
                    + str(subject.get("name") or "")
                    + "%22%2C%0A%20%20%20%20%20%20%22limit%22%3A%205%2C%0A%20%20%20%20%20%20%22properties%22%3A%20%5B%0A%20%20%20%20%20%20%20%20%7B%0A%20%20%20%20%20%20%20%20%20%20%22pid%22%3A%20%22P1225%22%2C%0A%20%20%20%20%20%20%20%20%20%20%22v%22%3A%20%22"
                    + naid
                    + "%22%0A%20%20%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20%5D%2C%0A%20%20%20%20%20%20%22type_strict%22%3A%20%22should%22%0A%20%20%20%20%7D%0A%7D"
                )
                subjectresults = json.loads(h.text)
                if subjectresults["q1"]["result"]:
                    subjqid = subjectresults["q1"]["result"][0]["id"]
                subjects.append((str(subject.get("name") or ""), subjqid))
                added = True
            if added == False:
                # print((str(subject.get('name') or ''), ''))
                subjects.append((str(subject.get("name") or ""), ""))
        # print(subjects)
    except:
        subjects = ""
    try:
        creators = dpla["sourceResource"]["creator"]
    except:
        creators = ""
    if dpla["provider"]["name"] == "National Archives and Records Administration":
        naids = dpla["sourceResource"]["identifier"]
        codes = {
            "10031403": "Q66739888",
            "10031402": "Q24238356",
            "10031399": "Q66739729",
            "10031400": "Q66739849",
            "10031401": "Q66739875",
        }
        levels = {"item": "Q11723795", "itemAv": "Q11723795", "fileUnit": "Q59221146"}
        xml = BeautifulSoup(dpla["originalRecord"]["stringValue"], "xml")
        try:
            acccess_naid = str(
                xml.find("accessRestriction").find("status").find("naId").text
            )
            access = codes[acccess_naid]
        except:
            access = ""
        for key in levels.keys():
            if xml.find(key):
                level = levels[key]
        local_ids = ""
    else:
        naids = ""
        access = ""
        level = ""

    #     language
    #     type
    #     extent
    #     format
    #     contributor
    #     publisher

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

    # Only make a post request if the claims array has accumulated at least one claim to add to the file. If any results set finds at least one edit to make, then 'posted' remains True, and the search will be tried again to pick up any more edits to make. If a whole results set is checked and no edits remain, we assume the edits to files with those search parameters are all complete.

    if len(claims["claims"]) > 0:
        posted = True
        try:
            post = json.loads(
                http.fetch(
                    "https://commons.wikimedia.org/w/api.php",
                    method="POST",
                    data=postdata,
                ).text
            )
            print(" --- Saved new claims!")
        except:
            print(" --- Error encountered. 1")
            sys.exit()
    return count


# Since we are posting directly to the API, we must explicitly request a login token that will be sent with the POSTs.

site = pywikibot.Site()


def login():

    tokenrequest = http.fetch(
        "https://commons.wikimedia.org/w/api.php?action=query&meta=tokens&type=csrf&format=json"
    )

    tokendata = json.loads(tokenrequest.text)
    token = tokendata.get("query").get("tokens").get("csrftoken")
    return token


token = login()

# We can use a PWB generator to programatically make the list of files we are working on based on a set of criteria. Here, we are generating the page titles from a Wikimedia Commons search and categories. For other types of available page generators, see <https://doc.wikimedia.org/pywikibot/master/api_ref/pywikibot.html#module-pywikibot.pagegenerators>. As an additional step, we take the pageid provided by the generator and prepend "M" for the mediaid needed for posting SDC statements. If the list of claims generated is greater than zero, then we send the post using wbeditentity to the Wikimedia Commons API.

count = 0

if method == "list":

    ltotal = [i for i in os.listdir(args.lists) if ".txt" in i]
    lists = [i for i in ltotal if not ("COMPLETE" in i) and not ("WORKING" in i)]
    percent = 100 * (len(ltotal) - len(lists)) / len(ltotal)
    while len(lists) > 0:
        if len(lists) > 1:
            x = random.choice(range(0, len(lists) - 1))
        elif len(lists) == 1:
            x = 0
        working_file = args.lists + "/WORKING-" + lists[x]
        print(working_file)
        os.rename(args.lists + "/" + lists[x], working_file)

        files = pywikibot.pagegenerators.TextIOPageGenerator(working_file)

        for file in files:
            print("\n" + str(file).replace('""', '"'))
            mediaid = "M" + str(file.pageid)
            dpla_id = re.sub(r"^.*DPLA - (.*?)[\.| ].*$", r"\1", str(file))
            try:
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
                ) = parsed(dpla_id, key)
            except TypeError:
                with open("Missing ids.txt", "a") as missing:
                    missing.write(dpla_id + "\n")
                    print(" -- Missing ID recorded.")
                continue

            claims = {"claims": []}
            refclaims = {"claims": []}

            count = count + 1
            print(
                str(count)
                + ":\n - "
                + args.lists
                + "/"
                + lists[x]
                + " ("
                + str("{:.2f}".format(percent))
                + "% done)"
            )
            add_rs(mediaid, rs, dpla_id)
            add_id(mediaid, dpla_id)
            for title in titles:
                add_title(mediaid, title.rstrip(), dpla_id)
            add_collection(mediaid, hub, institution, dpla_id)
            for creator in creators:
                add_creator(mediaid, creator.rstrip(), dpla_id)
            for subject in subjects:
                add_subject(mediaid, subject[0], dpla_id)
                if subject[1]:
                    add_subject_entity(mediaid, subject[1], dpla_id)
            for desc in descs:
                add_desc(mediaid, desc.rstrip(), dpla_id)
            #             for date in dates:
            #                 add_date(mediaid, date['displayDate'], dpla_id)
            add_contributed(mediaid, hub, institution, dpla_id)
            add_source(mediaid, hub, url, dpla_id)
            for local_id in local_ids:
                add_local_id(mediaid, local_id, institution, dpla_id)
            for naid in naids:
                add_naid(mediaid, naid, dpla_id)
            add_access(mediaid, access, dpla_id)
            add_level(mediaid, level, dpla_id)
            if len(refclaims["claims"]) > 0:
                postrefs = {
                    "action": "wbeditentity",
                    "format": "json",
                    "id": mediaid,
                    "data": json.dumps(refclaims),
                    "token": token,
                    "bot": True,
                    "summary": "Added structured data references from [[COM:DPLA|DPLA]] item '[[dpla:"
                    + dpla_id
                    + "|"
                    + dpla_id
                    + "]]'. [[COM:DPLA/MOD|Leave feedback]]!",
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
                except:
                    try:
                        token = login()
                        postrefs = {
                            "action": "wbeditentity",
                            "format": "json",
                            "id": mediaid,
                            "data": json.dumps(refclaims),
                            "token": token,
                            "bot": True,
                            "summary": "Added structured data references from [[COM:DPLA|DPLA]] item '[[dpla:"
                            + dpla_id
                            + "|"
                            + dpla_id
                            + "]]'. [[COM:DPLA/MOD|Leave feedback]]!",
                        }
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
                    except:
                        print(" --- Error encountered. 2")
                        sys.exit()

            postdata = {
                "action": "wbeditentity",
                "format": "json",
                "id": mediaid,
                "data": json.dumps(claims),
                "token": token,
                "bot": True,
                "summary": "Added structured data claims from [[COM:DPLA|DPLA]] item '[[dpla:"
                + dpla_id
                + "|"
                + dpla_id
                + "]]'. [[COM:DPLA/MOD|Leave feedback]]!",
            }

            if len(claims["claims"]) > 0:
                try:
                    save = http.fetch(
                        "https://commons.wikimedia.org/w/api.php",
                        method="POST",
                        data=postdata,
                    )
                except requests.exceptions.ConnectionError:
                    try:
                        save = http.fetch(
                            "https://commons.wikimedia.org/w/api.php",
                            method="POST",
                            data=postdata,
                        )
                    except requests.exceptions.ConnectionError:
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
                except:
                    try:
                        token = login()
                        postdata = {
                            "action": "wbeditentity",
                            "format": "json",
                            "id": mediaid,
                            "data": json.dumps(claims),
                            "token": token,
                            "bot": True,
                            "summary": "Added structured data claims from [[COM:DPLA|DPLA]] item '[[dpla:"
                            + dpla_id
                            + "|"
                            + dpla_id
                            + "]]'. [[COM:DPLA/MOD|Leave feedback]]!",
                        }
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
                    except:
                        print(str(post) + "\n" + str(postdata))
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

        os.rename(working_file, args.lists + "/COMPLETE-" + lists[x])

        ltotal = [i for i in os.listdir(args.lists) if ".txt" in i]
        lists = [i for i in ltotal if not ("COMPLETE" in i) and not ("WORKING" in i)]

        # De-dupe missing file:
        duduped = set()
        for line in open("Missing ids.txt", "r"):
            duduped.add(line)
        with open("Missing ids.txt", "w") as f:
            f.write("".join(duduped))
