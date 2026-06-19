"""CLI-level tests for tools/get_ids_es.py.

Focused on the new multi-``--institution`` + ``--collection`` argument
validation contract added when launch.py grew QID-combine support. The
ES query construction and S3 staging paths are exercised end-to-end by
the actual partner runs — only the validation surface that can silently
ship a wrong CLI is unit-tested here.
"""

from __future__ import annotations

from unittest.mock import patch

from click.testing import CliRunner


def _invoke(*args: str):
    """Invoke get-ids-es's Click app with monkey-patched DPLA.check_partner
    so we don't need a config.toml. Stops further execution after
    validation by patching everything that follows."""
    from tools import get_ids_es

    with (
        patch.object(get_ids_es.DPLA, "check_partner", return_value=None),
        patch.object(get_ids_es, "notify_phase_start"),
        patch.object(get_ids_es, "PARTNER_HUBS", {"bpl": "Digital Commonwealth"}),
        patch.object(
            get_ids_es,
            "fetch_institutions_v2",
            # ``load_eligible_dp_names`` only counts an institution as
            # eligible when BOTH the hub and the institution carry a
            # ``Wikidata`` field — that's how the production data
            # excludes records that haven't been fully onboarded.
            # Stubbed values are arbitrary QIDs; they exist solely to
            # pass the eligibility gate so the tests can exercise the
            # CLI-validation contract we actually care about.
            return_value={
                "Digital Commonwealth": {
                    "Wikidata": "Q12345",
                    "institutions": {
                        "Boston Public Library": {
                            "upload": True,
                            "Wikidata": "Q1001",
                        },
                        "Boston City Archives": {
                            "upload": True,
                            "Wikidata": "Q1002",
                        },
                    },
                }
            },
        ),
        # Short-circuit AFTER validation by raising at the next external call.
        patch.object(
            get_ids_es,
            "fetch_subjects_json",
            side_effect=RuntimeError("stop-after-validation"),
        ),
        patch.object(get_ids_es, "load_rights_json", return_value={}),
    ):
        runner = CliRunner()
        # ``catch_exceptions=True`` (the default) puts unhandled
        # exceptions into ``result.exception`` instead of re-raising —
        # that's how ``test_multiple_institutions_pass_validation_and_combine``
        # reads the sentinel ``RuntimeError`` from the stubbed
        # ``fetch_subjects_json``. With ``catch_exceptions=False`` the
        # runner re-raises into the test body, killing it before the
        # assertion runs.
        return runner.invoke(get_ids_es.main, list(args))


def test_collection_without_institution_is_hub_wide():
    """``--collection`` with no ``--institution`` is valid: the collection
    is matched across every upload-eligible institution in the hub (some
    collections span multiple institutions). It must pass validation and
    reach the ID-generation path (the stubbed ``fetch_subjects_json``
    sentinel proves it got past validation)."""
    result = _invoke(
        "bpl",
        "--collection",
        "Some Collection",
    )
    assert isinstance(result.exception, RuntimeError)
    assert "stop-after-validation" in str(result.exception)


def test_collection_with_multiple_institutions_passes():
    """``--collection`` combined with multiple ``--institution`` values is
    accepted at the tool level — the ES query simply ANDs the collection
    with the institution set. (The launch script restricts pipe-target
    SYNTAX to zero or one institution; the tool itself is permissive.)"""
    result = _invoke(
        "bpl",
        "--institution",
        "Boston Public Library",
        "--institution",
        "Boston City Archives",
        "--collection",
        "Some Collection",
    )
    assert isinstance(result.exception, RuntimeError)
    assert "stop-after-validation" in str(result.exception)


def test_collection_empty_string_rejected():
    """An empty ``--collection`` value is still rejected."""
    result = _invoke("bpl", "--collection", "   ")
    assert result.exit_code == 1, result.output
    assert "--collection cannot be empty" in result.output


def test_multiple_institutions_pass_validation_and_combine():
    """Multiple ``--institution`` values without ``--collection`` must
    pass validation and reach the ID-generation path (where our stubbed
    ``fetch_subjects_json`` raises a known sentinel — proving the run
    got past validation rather than rejecting the inputs upfront)."""
    result = _invoke(
        "bpl",
        "--institution",
        "Boston Public Library",
        "--institution",
        "Boston City Archives",
    )
    assert isinstance(result.exception, RuntimeError)
    assert "stop-after-validation" in str(result.exception)


def test_unknown_institution_rejected_with_clear_message():
    """An institution name that isn't in the hub's eligible set must be
    rejected explicitly, naming the offender — this is the safety net
    that prevents the launch script from silently asking ES for an
    institution that doesn't exist."""
    result = _invoke(
        "bpl",
        "--institution",
        "Boston Public Library",
        "--institution",
        "Nonexistent Institution",
    )
    assert result.exit_code == 1, result.output
    assert "not upload-eligible" in result.output
    assert "Nonexistent Institution" in result.output


# ---------------------------------------------------------------------------
# --single-id mode — re-stage one DPLA item's sidecars so the per-item
# launch path (``/wikimedia-upload <dpla-id>``) actually exercises the
# latest mapping code, instead of diffing sdc-sync against whatever
# sdc.json the partner's last full run produced.
# ---------------------------------------------------------------------------


def test_single_id_rejects_combined_with_institution():
    """--single-id targets exactly one doc by ID; combining it with
    --institution makes no sense (the institution filter would be
    ignored or would unexpectedly hide the requested ID)."""
    result = _invoke(
        "bpl",
        "--single-id",
        "deadbeefcafe00000000000000000000",
        "--institution",
        "Boston Public Library",
    )
    assert result.exit_code == 1, result.output
    assert "cannot be combined with --institution" in result.output


def test_single_id_rejects_combined_with_collection():
    """Same defense for --collection."""
    result = _invoke(
        "bpl",
        "--single-id",
        "deadbeefcafe00000000000000000000",
        "--collection",
        "Some Collection",
    )
    assert result.exit_code == 1, result.output
    assert "cannot be combined with --institution or --collection" in result.output


def _invoke_single_id(single_id, hits):
    """Invoke get-ids-es with --single-id, mocking out ES + S3 + reconci.link
    so the test exercises validation, query construction, and the
    not-found vs found code paths without external calls."""
    from unittest.mock import MagicMock

    from tools import get_ids_es

    fake_response = MagicMock()
    fake_response.json.return_value = {"hits": {"hits": hits}}
    fake_response.raise_for_status.return_value = None

    with (
        patch.object(get_ids_es.DPLA, "check_partner", return_value=None),
        patch.object(get_ids_es, "notify_phase_start"),
        patch.object(get_ids_es, "PARTNER_HUBS", {"bpl": "Digital Commonwealth"}),
        patch.object(
            get_ids_es,
            "fetch_institutions_v2",
            return_value={
                "Digital Commonwealth": {
                    "Wikidata": "Q12345",
                    "institutions": {
                        "Boston Public Library": {"upload": True, "Wikidata": "Q1001"},
                    },
                }
            },
        ),
        patch.object(get_ids_es, "load_rights_json", return_value={}),
        patch.object(get_ids_es, "fetch_subjects_json", return_value={}),
        patch.object(get_ids_es, "post_es", return_value=fake_response) as post_es_mock,
        patch.object(get_ids_es, "check_es_response"),
        patch.object(get_ids_es, "stage_item_to_s3"),
        # Phase 3 stages sdc.json via this helper — mock it explicitly,
        # otherwise the real S3Client tries an actual write and the CI
        # job exits 1 with "1 sdc.json writes failed" before any
        # assertion runs (the failure mode CodeRabbit caught on the
        # earlier push of this PR).
        patch.object(get_ids_es, "stage_sdc_to_s3"),
        patch.object(get_ids_es.Banlist, "is_banned", return_value=False),
        patch.object(get_ids_es, "reconcile_subjects", return_value={}),
        patch.object(get_ids_es, "build_claims_for_doc", return_value={"claims": []}),
        patch("ingest_wikimedia.s3.S3Client.get_item_metadata", return_value="{}"),
    ):
        runner = CliRunner()
        result = runner.invoke(get_ids_es.main, ["bpl", "--single-id", single_id])
        return result, post_es_mock


def test_single_id_builds_term_query_not_paginated_filter():
    """--single-id must query ES by exact DPLA-ID term, NOT via
    ``build_query``'s hub-wide filter. Otherwise the rightsCategory /
    institution-upload-flag gates could silently hide the requested ID,
    defeating the operator's explicit single-item invocation."""
    fake_id = "deadbeefcafe00000000000000000000"
    fake_hit = {
        "_source": {
            "id": fake_id,
            "provider": {"name": "Digital Commonwealth"},
            "dataProvider": {"name": "Boston Public Library"},
            "mediaMaster": ["http://example.org/img.jpg"],
            "sourceResource": {"title": ["x"]},
        }
    }
    result, post_es_mock = _invoke_single_id(fake_id, [fake_hit])

    assert result.exit_code == 0, result.output
    # The single document was printed.
    assert fake_id in result.output
    # The ES query was a one-shot term lookup, not a paginated filter
    # built by build_query (which would include rightsCategory /
    # eligible-dp-names filters that single-id is meant to bypass).
    sent_query = post_es_mock.call_args[0][0]
    # ``size: 2`` (not 1) so the in-process ``len(hits) != 1`` defense
    # is actually reachable — see the matching comment in get_ids_es.py
    # where the query is built.
    assert sent_query == {"query": {"term": {"id": fake_id}}, "size": 2}


def test_single_id_exits_nonzero_when_es_has_no_match():
    """ID not in ES → exit code 1 with a clear error message. The launch
    script's tmux chain relies on the non-zero exit to short-circuit
    the downstream downloader/uploader/sdc-sync (which would otherwise
    run against an empty CSV and produce confusing 0-item summaries)."""
    fake_id = "deadbeefcafe00000000000000000000"
    result, _ = _invoke_single_id(fake_id, [])
    assert result.exit_code == 1, result.output
    assert fake_id in result.output
    assert "no document" in result.output


def test_single_id_banlist_hit_gets_distinct_error():
    """A banlisted ID must produce a ``banlist``-specific error message,
    not the generic ``no document from ES`` one. Operationally distinct:
    fixing a banlist hit is a one-line edit to dpla-id-banlist.txt,
    whereas a missing-from-ES hit needs an indexing investigation. The
    Slack failure handler surfaces this message verbatim, so the wording
    has to point on-call at the right remediation."""
    from unittest.mock import MagicMock

    from tools import get_ids_es

    fake_id = "deadbeefcafe00000000000000000000"
    # post_es should NEVER be called when the banlist check short-circuits.
    post_es_mock = MagicMock(
        side_effect=AssertionError("ES round-trip skipped on banlist hit")
    )
    with (
        patch.object(get_ids_es.DPLA, "check_partner", return_value=None),
        patch.object(get_ids_es, "notify_phase_start"),
        patch.object(get_ids_es, "PARTNER_HUBS", {"bpl": "Digital Commonwealth"}),
        patch.object(
            get_ids_es,
            "fetch_institutions_v2",
            return_value={
                "Digital Commonwealth": {
                    "Wikidata": "Q12345",
                    "institutions": {
                        "Boston Public Library": {"upload": True, "Wikidata": "Q1001"},
                    },
                }
            },
        ),
        patch.object(get_ids_es, "load_rights_json", return_value={}),
        patch.object(get_ids_es, "fetch_subjects_json", return_value={}),
        patch.object(get_ids_es.Banlist, "is_banned", return_value=True),
        patch.object(get_ids_es, "post_es", post_es_mock),
    ):
        runner = CliRunner()
        result = runner.invoke(get_ids_es.main, ["bpl", "--single-id", fake_id])

    assert result.exit_code == 1, result.output
    assert "banlist" in result.output
    assert fake_id in result.output


def test_single_id_rejects_mismatched_id_from_es():
    """Defense-in-depth: if ES returns a document whose ``_source.id``
    differs from the queried ID (would indicate either an index
    normalization regression or stale-replica weirdness), refuse to
    stage under the wrong key. The dpla-map.json S3 path is keyed by
    the source ID; staging under a mismatched key would orphan the
    object from the downstream downloader/uploader."""
    queried = "deadbeefcafe00000000000000000000"
    returned_id = "aaaabbbbccccddddeeeeffff00000000"
    hit = {
        "_source": {
            "id": returned_id,
            "provider": {"name": "Digital Commonwealth"},
            "dataProvider": {"name": "Boston Public Library"},
        }
    }
    result, _ = _invoke_single_id(queried, [hit])
    assert result.exit_code == 1, result.output
    assert "mismatched" in result.output
    assert returned_id in result.output


# ---------------------------------------------------------------------------
# _title_sort_key — CSV output ordering by Commons file-name


def _key(source: dict, dpla_id: str = "abc") -> str:
    from tools.get_ids_es import _title_sort_key

    return _title_sort_key(source, dpla_id)


def _sr(title):
    # DPLA's sourceResource.title is canonically a list; mirror that
    # shape here so the helper sees what it would see in production.
    # (String-form is treated as missing by ``get_list``, matching the
    # uploader's title selection — keep test fixtures faithful to that.)
    if isinstance(title, str):
        title = [title]
    return {"sourceResource": {"title": title}}


def test_sort_key_alphabetical_basic():
    """Plain alphabetic titles sort A < B < C — the human-obvious case."""
    keys = [_key(_sr(t)) for t in ["Apple", "Banana", "Cherry"]]
    assert keys == sorted(keys)


def test_sort_key_strips_leading_quotes_and_parens():
    """A leading quote or paren must not bury the title beneath every
    letter — that would defeat the whole point of alphabetic sort. The
    NARA hub in particular has thousands of titles like '"YOU BET …"'
    and '(Title Index …)' that, without normalisation, would cluster
    at the top of the CSV ahead of every alphabetic title."""
    quoted = _key(_sr('"YOU BET I\'M GOING BACK TO SEA"'))
    parend = _key(_sr("(Title Index to World War II Posters)"))
    plain_y = _key(_sr("Yellowstone"))
    plain_t = _key(_sr("Treaty document"))
    # Quoted "Y..." sorts near plain "Y..." titles, not before "A".
    assert sorted([quoted, plain_t, plain_y]) == [plain_t, plain_y, quoted]
    # Paren'd "Title..." sorts near plain "T...".
    assert sorted([parend, plain_t, plain_y]) == [parend, plain_t, plain_y]


def test_sort_key_case_insensitive():
    """``apple`` and ``Apple`` sort adjacently regardless of case so a
    hub with mixed-case titles doesn't show an upper-case block then a
    lower-case block."""
    assert _key(_sr("apple"))[:5] == _key(_sr("Apple"))[:5]


def test_sort_key_multi_ordinal_items_stay_grouped():
    """All ordinals of a multi-page item share the same item title, so
    they end up adjacent in the CSV. Within an item, the uploader
    iterates ordinals 1..N in numeric order, giving '(page 1), (page 2)'
    sequencing for free."""
    same_title = _sr("Minutes of the House Committee")
    k1 = _key(same_title, "0000aaa")
    k2 = _key(same_title, "0000bbb")
    other = _key(_sr("Notes on a meeting"), "0000ccc")
    sorted_keys = sorted([k1, other, k2])
    assert sorted_keys == [k1, k2, other], (
        "Identical-title items must stay adjacent before the next title"
    )


def test_sort_key_handles_missing_title():
    """Items without ``sourceResource.title`` (or with a non-string,
    non-list shape) must still produce a stable key, not raise."""
    assert isinstance(_key({}), str)
    assert isinstance(_key({"sourceResource": {}}), str)
    assert isinstance(_key({"sourceResource": {"title": None}}), str)
    assert isinstance(_key({"sourceResource": {"title": 42}}), str)


def test_sort_key_list_title_uses_first():
    """When ``sourceResource.title`` is a list (the common DPLA shape),
    the first element drives the sort — same convention the uploader
    uses when building the Commons title."""
    list_form = _key({"sourceResource": {"title": ["Zebra", "Aardvark"]}})
    str_form = _key(_sr("Zebra"))
    # First-element form should sort like the equivalent string form.
    assert list_form[:5] == str_form[:5]


def test_sort_key_dpla_id_tiebreaks_identical_titles():
    """Two items with identical titles fall back to DPLA-ID order, so
    the sort is fully deterministic across re-runs."""
    a = _key(_sr("Same Title"), "00000000000000000000000000000001")
    b = _key(_sr("Same Title"), "00000000000000000000000000000002")
    assert a < b
