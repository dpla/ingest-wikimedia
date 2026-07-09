"""Tests for scripts/wikimedia_kill.py — the input-format parsing that makes
``/wikimedia-upload kill <target>`` accept the same target strings as
``/wikimedia-upload <target>``, AND the group-subset matching contract that
keeps a collection-scoped kill from also killing the parent institution
session (or vice-versa, or across hubs that share an institution slug).
"""

from unittest.mock import patch

import pytest

from scripts.wikimedia_kill import resolve_kill_components, _UnknownHub


def test_bare_hub_slug_resolves_to_single_element_group():
    """``bpl`` → group ``{bpl}``; matches any wikimedia-bpl[+…] session."""
    assert resolve_kill_components(["bpl"]) == [frozenset({"bpl"})]


def test_bare_component_slug_passes_through_as_single_element_group():
    """A pre-slugified institution component without a hub prefix is kept as-is
    in a single-element group, preserving the back-compat 'kill by component
    slug' behavior."""
    assert resolve_kill_components(["indiana-state-library"]) == [
        frozenset({"indiana-state-library"})
    ]


def test_hub_pipe_institution_builds_canonical_plus_institution_group():
    """``nara|Lyndon Baines Johnson Library`` → group {nara,
    lyndon-baines-johnson-library}. The group requires BOTH components
    together: matches wikimedia-nara+lyndon-baines-johnson-library[+…]
    but NOT bare wikimedia-nara, and NOT a different hub that happens
    to share the institution slug."""
    components = resolve_kill_components(["nara|Lyndon Baines Johnson Library"])
    assert components == [frozenset({"nara", "lyndon-baines-johnson-library"})]


def test_hub_pipe_institution_strips_non_alphanumeric():
    """Institutions with punctuation (e.g. ``AT&T``) must slug identically on
    launch and kill — the shared slugify strips ``&``, commas, etc.  The
    canonical hub is still part of the group."""
    components = resolve_kill_components(["nara|AT&T Archives, Inc."])
    assert components == [frozenset({"nara", "att-archives-inc"})]


def test_hub_pipe_institution_pipe_collection_includes_all_three():
    """Three-part target → group {canonical, institution_slug, collection_slug}.
    THE BUG FIX: a flat component list let `nara|Center for Legislative
    Archives|RG46` also kill the parent institution session (because the
    institution slug alone intersected) and any unrelated `*+rg-46`
    session. Now the kill only fires when ALL THREE components are
    present together in the session label."""
    components = resolve_kill_components(["nara|Center for Legislative Archives|RG 46"])
    assert components == [
        frozenset({"nara", "center-for-legislative-archives", "rg-46"})
    ]


def test_dpla_id_resolves_to_first_8_hex_group():
    """Single-item launch labels use the first 8 hex chars of the DPLA ID
    as their tmux-label suffix; kill must mirror that. The 8-hex string
    is unique enough that a single-component group is safe."""
    components = resolve_kill_components(["405714d95f606141d383ccc3ef22908b"])
    assert components == [frozenset({"405714d9"})]


def test_dpla_id_case_insensitive():
    """DPLA IDs from URLs are sometimes uppercase; normalize before slicing."""
    components = resolve_kill_components(["405714D95F606141D383CCC3EF22908B"])
    assert components == [frozenset({"405714d9"})]


def test_unknown_hub_in_pipe_token_raises_unknown_hub():
    """Hub prefix must resolve; otherwise raise _UnknownHub so the caller
    can produce a user-facing Slack error referencing the bad token."""
    with pytest.raises(_UnknownHub) as exc:
        resolve_kill_components(["not-a-hub|Some Institution"])
    assert "not-a-hub" in str(exc.value)


def test_wikidata_qid_resolves_to_canonical_plus_institution_group():
    """QID that matches an institution → group {canonical, institution_slug}
    — same shape as the hub|institution form, for the same subset-match
    safety reasons."""
    with patch(
        "scripts.wikimedia_kill.resolve_wikidata_id",
        return_value=[("nara", "Herbert Hoover Library")],
    ):
        assert resolve_kill_components(["Q12345"]) == [
            frozenset({"nara", "herbert-hoover-library"})
        ]


def test_wikidata_qid_resolves_to_canonical_only_when_no_institution():
    """QID that matches a hub itself (no institution) → group {canonical}."""
    with patch(
        "scripts.wikimedia_kill.resolve_wikidata_id",
        return_value=[("indiana", None)],
    ):
        assert resolve_kill_components(["Q67890"]) == [frozenset({"indiana"})]


def test_wikidata_qid_resolved_without_upload_eligibility_filter():
    """Kill resolves QIDs with maintain=True (QID-presence only) so a present-
    but-opted-out QID still resolves for killing. Regression: the default
    upload-gated filter dropped opted-out matches, failing the kill (Q955764)."""
    with patch(
        "scripts.wikimedia_kill.resolve_wikidata_id",
        return_value=[("nara", "Herbert Hoover Library")],
    ) as resolve:
        groups = resolve_kill_components(["Q955764"])
    resolve.assert_called_once_with("Q955764", maintain=True)
    assert groups == [frozenset({"nara", "herbert-hoover-library"})]


def test_wikidata_qid_unknown_raises_value_error():
    with patch("scripts.wikimedia_kill.resolve_wikidata_id", return_value=[]):
        with pytest.raises(ValueError) as exc:
            resolve_kill_components(["Q99999"])
    assert "Q99999" in str(exc.value)


def test_deduplicates_identical_groups_first_seen_order():
    """If two tokens produce the IDENTICAL group, only one is kept.
    Different token shapes that produce different groups (e.g.
    bare 'indiana-state-library' vs 'indiana|Indiana State Library')
    are NOT duplicates — they have different match semantics (the
    former matches any session containing the slug; the latter
    requires the canonical hub to also be present)."""
    components = resolve_kill_components(
        [
            "bpl",
            "bpl",  # identical → deduped
            "nara|Lyndon Baines Johnson Library",
            "nara|Lyndon Baines Johnson Library",  # identical → deduped
        ]
    )
    assert components == [
        frozenset({"bpl"}),
        frozenset({"nara", "lyndon-baines-johnson-library"}),
    ]


def test_bare_slug_and_hub_pipe_institution_produce_different_groups():
    """The bare slug ``indiana-state-library`` produces a single-element
    group (matches any session with that component, any hub).  The
    ``indiana|Indiana State Library`` token produces a two-element group
    requiring the indiana canonical too.  They are NOT duplicates — the
    second form is strictly narrower."""
    components = resolve_kill_components(
        [
            "indiana-state-library",
            "indiana|Indiana State Library",
        ]
    )
    assert components == [
        frozenset({"indiana-state-library"}),
        frozenset({"indiana", "indiana-state-library"}),
    ]


def test_handles_mixed_token_types():
    """A realistic /wikimedia-upload kill call might mix several forms."""
    components = resolve_kill_components(["bpl", "nara|Lyndon Baines Johnson Library"])
    assert components == [
        frozenset({"bpl"}),
        frozenset({"nara", "lyndon-baines-johnson-library"}),
    ]


def test_empty_input_returns_empty():
    assert resolve_kill_components([]) == []


def test_whitespace_around_tokens_is_stripped_before_classification():
    """Tokens arriving via shlex.split sometimes still carry whitespace from
    upstream Slack payload parsing. Without an explicit strip, an input like
    `' 405714D9... '` would fail is_dpla_id() and fall through to the bare-
    slug branch, producing a group containing the un-normalized literal that
    never matches any tmux session."""
    dpla_id_padded = "  405714D95F606141D383CCC3EF22908B  "
    bare_padded = "\tindiana-state-library\n"
    qid_padded = " Q12345 "
    hub_pipe_padded = " nara|Lyndon Baines Johnson Library "
    with patch(
        "scripts.wikimedia_kill.resolve_wikidata_id",
        return_value=[("nara", "Herbert Hoover Library")],
    ):
        groups = resolve_kill_components(
            [dpla_id_padded, bare_padded, qid_padded, hub_pipe_padded]
        )
    assert groups == [
        frozenset({"405714d9"}),
        frozenset({"indiana-state-library"}),
        frozenset({"nara", "herbert-hoover-library"}),
        frozenset({"nara", "lyndon-baines-johnson-library"}),
    ]


def test_whitespace_only_tokens_are_skipped():
    """A token that's purely whitespace produces no group — no empty-string
    'slug' silently appears in the kill list."""
    assert resolve_kill_components(["   ", "\t", ""]) == []


def test_hub_pipe_institution_with_punctuation_only_institution_raises():
    """If the institution part normalizes to an empty slug (e.g. all punctuation
    like '&&&' or '...'), raise a user-facing ValueError instead of silently
    producing an unmatchable group. The same protection covers the collection
    position of the three-part form."""
    with pytest.raises(ValueError) as exc:
        resolve_kill_components(["nara|&&&"])
    assert "&&&" in str(exc.value)
    assert "empty slug" in str(exc.value).lower()


def test_hub_pipe_institution_pipe_punctuation_only_collection_raises():
    """Collection slug that normalizes to empty should fail loudly, same as
    the institution case."""
    with pytest.raises(ValueError) as exc:
        resolve_kill_components(["nara|Center for Legislative Archives|..."])
    assert "..." in str(exc.value)
    assert "empty slug" in str(exc.value).lower()


def test_wikidata_qid_institution_normalizing_to_empty_raises():
    """If a QID resolves to an institution whose name normalizes to empty
    (rare but possible if institutions_v2.json ever stores punctuation-only
    names), surface that as a ValueError rather than a silently-broken group."""
    with patch(
        "scripts.wikimedia_kill.resolve_wikidata_id",
        return_value=[("nara", "&&&")],
    ):
        with pytest.raises(ValueError) as exc:
            resolve_kill_components(["Q12345"])
    assert "empty slug" in str(exc.value).lower()


def test_empty_institution_part_falls_back_to_hub_only_group():
    """``nara|`` — hub with empty institution — produces a single-element
    {canonical} group, identical to the bare ``nara`` form. This is a
    behavior tightening from the previous 'silently skip' result: with
    the group model the two forms are now functionally equivalent,
    which is what most users would intuitively expect."""
    components = resolve_kill_components(["nara|"])
    assert components == [frozenset({"nara"})]


# --------------------------------------------------------------------------
# Subset-matching semantics — the contract main() relies on
#
# main() iterates tmux sessions and kills any whose '+'-split component set
# is a SUPERSET of at least one group. These scenario tests directly
# exercise the bug CodeRabbit caught on the original PR: a collection-
# scoped token must NOT also kill the parent institution session, and an
# institution slug must NOT bleed across to a different hub.
# --------------------------------------------------------------------------


def _session_components(session_label_suffix: str) -> set[str]:
    """Mirror what main() does: split the part after 'wikimedia-' on '+'."""
    return set(session_label_suffix.split("+"))


def test_collection_scoped_kill_spares_parent_institution_session():
    """The original CodeRabbit-flagged bug, pinned: a token like
    ``nara|Center for Legislative Archives|RG46`` must kill only the
    specific collection sub-session, not the parent institution session."""
    [group] = resolve_kill_components(["nara|Center for Legislative Archives|RG 46"])
    parent = _session_components("nara+center-for-legislative-archives")
    child = _session_components("nara+center-for-legislative-archives+rg-46")
    sibling_collection = _session_components(
        "nara+center-for-legislative-archives+rg-99"
    )
    assert not group <= parent  # parent must NOT be killed
    assert group <= child  # exact target IS killed
    assert not group <= sibling_collection  # different collection NOT killed


def test_institution_scoped_kill_does_not_cross_hubs():
    """A token like ``nara|Lyndon Baines Johnson Library`` must not kill
    a session that happens to have the same institution slug under a
    different hub (cross-hub bleed)."""
    [group] = resolve_kill_components(["nara|Lyndon Baines Johnson Library"])
    intended = _session_components("nara+lyndon-baines-johnson-library")
    cross_hub_collision = _session_components(
        "some-other-hub+lyndon-baines-johnson-library"
    )
    bare_hub = _session_components("nara")
    assert group <= intended
    assert not group <= cross_hub_collision
    assert not group <= bare_hub


def test_institution_scoped_kill_includes_collection_sub_sessions():
    """An institution-scoped token DOES kill the institution's collection
    sub-sessions — narrowing further requires the collection form.
    Killing the parent + all its children when targeting the parent is
    the expected behavior."""
    [group] = resolve_kill_components(["nara|Lyndon Baines Johnson Library"])
    institution_only = _session_components("nara+lyndon-baines-johnson-library")
    with_collection = _session_components(
        "nara+lyndon-baines-johnson-library+some-collection"
    )
    assert group <= institution_only
    assert group <= with_collection


def test_bare_hub_kill_kills_all_sessions_for_that_hub():
    """``bpl`` kills any wikimedia-bpl[+…] session — full-hub kill."""
    [group] = resolve_kill_components(["bpl"])
    bare = _session_components("bpl")
    with_inst = _session_components("bpl+some-institution")
    with_inst_and_coll = _session_components("bpl+some-institution+some-collection")
    other_hub = _session_components("nara+something")
    assert group <= bare
    assert group <= with_inst
    assert group <= with_inst_and_coll
    assert not group <= other_hub
