"""CLI-level test for tools/get_ids_nara.py — specifically that the SDC
pre-compute pass added in this PR runs after Phase 1 enumeration. Without
it, NARA hub-level runs leave sdc.json stale (last written by the
previous upload run) and sdc-sync silently misses any
ingest_wikimedia.sdc mapping changes shipped in the interim. The
matching production observability is "diff the sdc.json LastModified
timestamp on S3 against the get-ids-nara run time" — this test pins the
in-process call graph so a future refactor that drops Phase 3 surfaces
as a test failure rather than a Slack-message-shaped surprise on the
next operator run.
"""

from __future__ import annotations

from unittest.mock import patch

from click.testing import CliRunner


def test_main_runs_sdc_phase_after_enumeration():
    """The Phase 1 enumeration (stage_item_to_s3 for each hit) must be
    followed by a Phase 3 SDC staging pass (stage_sdc_to_s3 for each
    enumerated DPLA ID). Without Phase 3, NARA hub-level runs leave
    sdc.json stale on every item they enumerate — the exact gap this
    PR closes."""
    from tools import get_ids_nara

    fake_hits = [
        {
            "_source": {
                "id": "nara0000000000000000000000000001",
                "provider": {"name": "National Archives and Records Administration"},
                "dataProvider": {"name": "NARA — Some Library"},
                "sourceResource": {"title": ["x"]},
                "mediaMaster": ["http://example.org/x.jpg"],
            }
        },
        {
            "_source": {
                "id": "nara0000000000000000000000000002",
                "provider": {"name": "National Archives and Records Administration"},
                "dataProvider": {"name": "NARA — Another Library"},
                "sourceResource": {"title": ["y"]},
                "mediaMaster": ["http://example.org/y.jpg"],
            }
        },
    ]

    with (
        # No remote ES calls during the build_*_queries phases.
        patch.object(get_ids_nara, "build_format_queries", return_value=[]),
        patch.object(get_ids_nara, "build_language_queries", return_value=[]),
        patch.object(get_ids_nara, "notify_phase_start"),
        # Phase 1: yield the two fake hits across whatever filter is passed.
        patch.object(get_ids_nara, "_paginate", return_value=iter(fake_hits)),
        # Phase 1 staging: don't actually write to S3.
        patch.object(get_ids_nara, "stage_item_to_s3") as stage_item_mock,
        # SDC-input loaders: cheap stubs.
        patch.object(
            get_ids_nara,
            "fetch_institutions_v2",
            return_value={
                "National Archives and Records Administration": {
                    "Wikidata": "Q518155",
                    "institutions": {},
                }
            },
        ),
        patch.object(get_ids_nara, "load_rights_json", return_value={}),
        patch.object(get_ids_nara, "fetch_subjects_json", return_value={}),
        patch.object(get_ids_nara, "reconcile_subjects", return_value={}),
        # S3Client used to read dpla-map.json back in Phase 3. Return the
        # source doc verbatim — build_claims_for_doc handles it from there.
        patch.object(get_ids_nara, "S3Client") as s3_class_mock,
        # build_claims_for_doc: stub return so Phase 3 actually calls stage_sdc_to_s3.
        patch.object(
            get_ids_nara, "build_claims_for_doc", return_value={"claims": []}
        ) as build_mock,
        # Phase 3 staging: capture the calls.
        patch.object(get_ids_nara, "stage_sdc_to_s3") as stage_sdc_mock,
        patch.object(get_ids_nara.Banlist, "is_banned", return_value=False),
    ):
        # Wire S3Client().get_item_metadata to return each fake hit's source
        # doc as JSON so Phase 3's re-read succeeds.
        s3_instance = s3_class_mock.return_value
        s3_instance.get_item_metadata.side_effect = [
            '{"id":"nara0000000000000000000000000001",'
            '"provider":{"name":"National Archives and Records Administration"},'
            '"dataProvider":{"name":"NARA — Some Library"},'
            '"sourceResource":{"title":["x"]},"mediaMaster":["http://example.org/x.jpg"]}',
            '{"id":"nara0000000000000000000000000002",'
            '"provider":{"name":"National Archives and Records Administration"},'
            '"dataProvider":{"name":"NARA — Another Library"},'
            '"sourceResource":{"title":["y"]},"mediaMaster":["http://example.org/y.jpg"]}',
        ]

        # Run main() and wait for the ThreadPoolExecutors to finish.
        runner = CliRunner()
        # Click's main() exits via SystemExit; standalone_mode=False
        # surfaces any non-zero exit code through the result.
        result = runner.invoke(get_ids_nara.main, [], standalone_mode=False)

    # If get_ids_nara raises during main(), Click captures the exception.
    assert result.exception is None, f"Unexpected exception: {result.exception!r}"

    # Phase 1 staged both items.
    assert stage_item_mock.call_count == 2
    # Phase 3 ran build_claims_for_doc for each item.
    assert build_mock.call_count == 2
    # Phase 3 staged sdc.json for each enumerated DPLA ID — the
    # contract this PR pins. Pre-PR, this list would be EMPTY.
    staged_ids = [call.args[2] for call in stage_sdc_mock.call_args_list]
    assert sorted(staged_ids) == [
        "nara0000000000000000000000000001",
        "nara0000000000000000000000000002",
    ], staged_ids
