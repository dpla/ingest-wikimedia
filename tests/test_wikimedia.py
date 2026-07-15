from unittest.mock import patch, MagicMock
from ingest_wikimedia.wikimedia import (
    COMMONSDELINKER_PAGE,
    MAX_COMMENT_BYTES,
    build_title_drift_move_reason,
    get_site,
    get_page_title,
    get_wiki_text,
    license_to_markup_code,
    get_permissions_template,
    get_permissions,
    escape_wiki_strings,
    escape_template_param,
    join,
    collect_duplicate_source_sha1s,
    compute_ordinal_exts_and_page_labels,
    extract_page_ordinal_from_commons_title,
    extract_strings,
    extract_strings_dict,
    file_has_inbound_usage,
    find_file_by_hash,
    first_uploader,
    is_same_item_redirect_relic,
    merge_preserved_wikitext,
    post_commonsdelinker_request,
    wiki_file_exists,
    check_content_type,
)
from types import SimpleNamespace


@patch("ingest_wikimedia.wikimedia.pywikibot.Site")
def test_get_site(mock_site):
    mock_site_instance = MagicMock()
    mock_site.return_value = mock_site_instance

    site = get_site()
    assert site == mock_site_instance
    mock_site.assert_called_once_with("commons")
    mock_site_instance.login.assert_called_once()


def test_get_site_pins_pywikibot_socket_timeout(monkeypatch):
    """Every ``get_site`` handout must pin ``pywikibot.config.socket_timeout``
    so a hung socket surfaces as a ``requests.exceptions.ReadTimeout``
    rather than blocking indefinitely at kernel recv (in-the-wild
    NPRC sdc-sync stall of 80 min against a CLOSE-WAIT socket)."""
    import pywikibot

    from ingest_wikimedia.wikimedia import PYWIKIBOT_SOCKET_TIMEOUT, get_site

    monkeypatch.setattr(pywikibot.config, "socket_timeout", None)
    monkeypatch.setattr("ingest_wikimedia.wikimedia.pywikibot.Site", MagicMock())
    get_site()
    assert pywikibot.config.socket_timeout == PYWIKIBOT_SOCKET_TIMEOUT


def test_get_wikidata_site_pins_pywikibot_socket_timeout(monkeypatch):
    """Same invariant on the Wikidata handout — the same failure mode
    exists whenever pywikibot's HTTP layer is used against any wiki."""
    import pywikibot

    from ingest_wikimedia.wikimedia import (
        PYWIKIBOT_SOCKET_TIMEOUT,
        get_wikidata_site,
    )

    monkeypatch.setattr(pywikibot.config, "socket_timeout", None)
    monkeypatch.setattr("ingest_wikimedia.wikimedia.pywikibot.Site", MagicMock())
    get_wikidata_site()
    assert pywikibot.config.socket_timeout == PYWIKIBOT_SOCKET_TIMEOUT


def test_check_content_type_valid():
    content_type = "image/jpeg"
    assert check_content_type(content_type)


def test_check_content_type_invalid():
    content_type = "text/html"
    assert not check_content_type(content_type)


def test_check_content_type_octet_stream_invalid():
    content_type = "application/octet-stream"
    assert not check_content_type(content_type)


def test_get_page_title():
    title = get_page_title("Sample Title", "abcd1234", ".jpg", 1)
    expected_title = "Sample Title - DPLA - abcd1234 (page 1).jpg"
    assert title == expected_title


def test_get_page_title_replaces_underscore_with_space():
    """Regression: MediaWiki's ``Title`` normalisation converts ``_`` in
    titles to spaces at store time, so a DPLA source title like
    ``doris_ulmann_0001`` must be rewritten to ``Doris ulmann 0001``
    at construction time — otherwise every downstream title-equality
    check (skip-if-already-there, hash-drift, expected_item_titles
    sibling protection, orphan probes, ``{{duplicate}}``-tag targets)
    treats the constructed vs. stored pair as drift and triggers a
    phantom Case-2 tag-upload that Commons itself rejects with
    ``fileexists-no-change``.

    Concrete pre-fix repro (2026-07-03 MWDL run): DPLA IDs
    ``fbfa741802e31f0b3b9ba69a79ed675b``,
    ``df20cb360e0f5fb5d8e1e9ddf7ac557c``,
    ``e34fac17587acd584cd038ced095fd01`` — three Doris Ulmann
    photographs whose ``sourceResource.title`` is a literal
    underscore-separated slug that Commons stored as
    ``File:Doris ulmann 000N - DPLA - <id>.jpg``.
    """
    title = get_page_title(
        "doris_ulmann_0001", "fbfa741802e31f0b3b9ba69a79ed675b", ".jpg"
    )
    assert title == "Doris ulmann 0001 - DPLA - fbfa741802e31f0b3b9ba69a79ed675b.jpg", (
        title
    )
    # ``_`` must NOT appear anywhere in the item-title portion.
    assert "_" not in title.split(" - DPLA -")[0]


def test_get_page_title_collapses_mixed_underscore_and_space_runs():
    """Regression (CR flagged on PR #365): ``.replace("_", " ")`` alone
    leaves ``foo__bar`` as ``foo  bar`` (two spaces) and ``foo_ bar``
    similarly. MediaWiki collapses whitespace runs to a single space at
    store time, so the constructed title MUST match. Without this
    collapse, :func:`find_file_by_hash` (at
    ``ingest_wikimedia/wikimedia.py`` — compares
    ``img.title(with_ns=False) == preferred_title``) misses matches
    against files stored with the single-space canonical form, sending
    the item down the wrong drift path.
    """
    # Two adjacent underscores → single space.
    t1 = get_page_title("foo__bar", "abcd1234", ".jpg")
    assert t1 == "Foo bar - DPLA - abcd1234.jpg", t1

    # Underscore + space (either order) → single space.
    t2 = get_page_title("foo_ bar", "abcd1234", ".jpg")
    assert t2 == "Foo bar - DPLA - abcd1234.jpg", t2
    t3 = get_page_title("foo _bar", "abcd1234", ".jpg")
    assert t3 == "Foo bar - DPLA - abcd1234.jpg", t3

    # Multi-space run (no underscores) also collapses.
    t4 = get_page_title("foo   bar", "abcd1234", ".jpg")
    assert t4 == "Foo bar - DPLA - abcd1234.jpg", t4

    # Leading/trailing whitespace stripped (matches MediaWiki's
    # ``Title`` trim on stored titles).
    t5 = get_page_title("  foo bar  ", "abcd1234", ".jpg")
    assert t5 == "Foo bar - DPLA - abcd1234.jpg", t5


def test_get_page_title_capitalizes_first_character_only():
    """MediaWiki's ``Title::capitalize`` uppercases the FIRST character
    of any title in a capitalized namespace (which ``File:`` is), but
    does NOT lowercase the rest — internal case is preserved
    verbatim. So a source title like ``eBookLibrary_1998`` becomes
    ``EBookLibrary 1998`` on Commons (E capitalized, B/L/... preserved),
    not ``Ebooklibrary 1998`` (which ``str.capitalize()`` would produce).
    """
    title = get_page_title("eBookLibrary_1998", "cafef00d" * 4, ".jpg")
    assert title == f"EBookLibrary 1998 - DPLA - {'cafef00d' * 4}.jpg", title
    # Rest of the casing is preserved — no forced lowercase.
    assert "EBook" in title
    assert "Ebook" not in title


def test_get_page_title_leaves_already_capitalized_titles_alone():
    """The uppercase-first-char step must be a no-op when the source
    title already starts with an uppercase letter — regression guard
    against a refactor that swaps in ``.capitalize()`` and silently
    lowercases everything after the first character."""
    title = get_page_title("Sample Title", "abcd1234", ".jpg")
    # Byte-identical to pre-fix output for the common case.
    assert title == "Sample Title - DPLA - abcd1234.jpg"


def test_get_page_title_trims_trailing_whitespace_after_truncation():
    """Regression: a DPLA-supplied title longer than 181 chars whose 181st
    character lands on whitespace must NOT leave that whitespace in the
    constructed Commons title. Commons normalises whitespace runs at
    store time, so the raw Python-side title (with trailing space) and
    the actual Commons-stored title (without) will not compare equal
    under the process_file identity check — that check drives every
    downstream skip-if-already-there / hash-drift / duplicate-tag
    branch, so a mismatch produces phantom Case-2 drift.

    Concrete repro: DPLA ID 95bd6bee5aed3c5311a67d5f6cee490b
    (NARA / FDR Library) has a 264-char source title. Truncated to 181
    chars, the last character is a space right after "...value of farm ".
    Pre-fix that space survived into the constructed page title,
    breaking equality with the Commons file at "...value of farm - DPLA
    - <id>.gif" and hanging the entire upload session in the phantom
    duplicate-tag drain.
    """
    # NB: single-string literal — Python's implicit-concat would elide
    # the spaces at the line breaks and change ``[181]``'s character.
    source_title = "Planning for an adequate home grown food supply brought to this New York woman, as to hundred thousands like her throughout the country, a realization of the economic value of farm produced food and fuel, and a keener appreciation of the advantages of farm living."
    # Sanity check the repro: ``[:181]``'s last character must be
    # whitespace so the fix's trailing-strip is what causes this test's
    # assertion to hold. Python slicing is end-exclusive — ``s[:181]``
    # covers indices 0..180, so we probe ``s[180]``.
    assert len(source_title) > 181
    assert source_title[:181].endswith(" "), (
        f"Test premise broken: source_title[:181] ends with "
        f"{source_title[:181][-3:]!r} — needs to end in whitespace for "
        f"this to be a truncation-lands-on-space repro."
    )
    title = get_page_title(source_title, "95bd6bee5aed3c5311a67d5f6cee490b", ".gif")
    assert " - DPLA -" in title
    assert "  - DPLA -" not in title, (
        f"expected no double-space before ` - DPLA -`; got {title!r}. "
        f"If this asserts, the `item_title[:181]` truncation left a "
        f"trailing space that leaks into the assembled title."
    )
    # Also pin the exact expected shape so a future refactor can't
    # silently drop the fix.
    assert title == (
        "Planning for an adequate home grown food supply brought to this "
        "New York woman, as to hundred thousands like her throughout the "
        "country, a realization of the economic value of farm - DPLA - "
        "95bd6bee5aed3c5311a67d5f6cee490b.gif"
    )


def test_get_page_title_preserves_short_title_trailing_whitespace_absent():
    """Trailing-whitespace strip only applies at the truncation boundary
    — a short title with no trailing whitespace must be identical to
    pre-fix output. Sanity check that the rstrip doesn't inadvertently
    alter titles that don't hit the 181-char cap."""
    title = get_page_title("Sample Title", "abcd1234", ".jpg")
    assert title == "Sample Title - DPLA - abcd1234.jpg"


def test_get_page_title_trims_multiple_trailing_whitespace_chars():
    """If the 181-char truncation lands inside a run of multiple
    whitespace characters, ``rstrip`` removes all of them, not just
    one — matching MediaWiki's ``.trim()`` on stored titles.

    Sizing: ``[:181]`` covers indices 0..180. Prefix is 178 A's, then
    ``\\t  `` at indices 178, 179, 180 — so the truncated chunk ends
    with three whitespace chars (tab + two spaces) that all need to
    strip cleanly.
    """
    prefix = "A" * 178
    source_title = prefix + "\t  more text after"
    assert source_title[:181] == prefix + "\t  ", (
        "test premise: truncation covers tab + 2 spaces"
    )
    title = get_page_title(source_title, "deadbeef" * 4, ".jpg")
    # No whitespace immediately before the ` - DPLA -` separator.
    assert title.startswith(prefix + " - DPLA -"), (
        f"expected trailing whitespace runs to be stripped; got {title!r}"
    )


def test_get_page_title_preserves_ampersand_when_no_equals():
    """Regression: titles containing `&` alone (no `=`) must NOT be rewritten.

    Replacing `&` unconditionally caused drift-correction renames of good
    Commons titles like `... suffragiis & orationibus. - DPLA - ...` into
    the uglier `... suffragiis + orationibus. - DPLA - ...` form.
    """
    title = get_page_title(
        "Hore beate Marie - cum multis suffragiis & orationibus.",
        "e01f0774b6ae80e4745504ab554f93b5",
        ".jpg",
        205,
    )
    assert "&" in title
    assert "+" not in title


def test_get_page_title_preserves_equals_when_no_ampersand():
    """`=` alone (no `&`) is fine — only the joint pattern is blacklisted."""
    title = get_page_title(
        "E=mc² and other formulas",
        "abcd" * 8,
        ".jpg",
    )
    assert "=" in title
    # `-` may appear in the DPLA-ID/extension fragments but not from `=` rewriting.
    # Verify the original character is preserved.
    assert "E=mc" in title


def test_get_page_title_breaks_query_string_when_both_present():
    """When both `&` and `=` appear, substitute to break the blacklist pattern."""
    title = get_page_title(
        "filter=value&other=thing",
        "abcd" * 8,
        ".jpg",
    )
    # Both characters get rewritten so the title no longer matches `&...=`.
    assert "&" not in title
    assert "=" not in title
    # Leading letter capitalized (MediaWiki ``Title::capitalize`` — see
    # get_page_title docstring); rest of casing preserved.
    assert "Filter-value+other-thing" in title


def test_get_page_title_replaces_slash_with_dash():
    """`/` in source titles must be substituted with `-`. The earlier
    reasoning that "slashes are valid in Commons File: titles" came from
    `action=query` returning "missing" for a slash-containing title,
    which only tests the title PARSER. The actual UPLOAD/MOVE path runs
    `UploadBase::isValidName` → `Title::makeTitleSafe(NS_FILE, …)` which
    rejects titles whose `/` triggers subpage parsing — observed as
    `imageinvalidfilename` errors on 4,114 NARA items (Nixon + LBJ
    libraries) in May 2026, where Case 3 drift moves tried to move
    files from the older code's `1-5-1966` (dash) form to a constructed
    `1/5/1966` (slash) target. Commons rejected every such move."""
    title = get_page_title("Page 1/2 of the survey", "abcd" * 8, ".jpg")
    assert "/" not in title.split(" - DPLA -")[0]
    assert title.startswith("Page 1-2 of the survey")


def test_get_page_title_lbj_diary_date_case():
    """Regression pin for the LBJ-library item that surfaced the slash
    side of the bug: `Lady Bird Johnson's Daily Diary Entry, 1/1/1968`.
    The 2025-04-29 upload (with older code) stored as `1-1-1968` on
    Commons. After PR #223 removed `/` → `-` from get_page_title, the
    current code constructs the title with `/` again, so hash-drift
    detection fires and tries to MOVE the existing dash-form file to
    the slash-form target — which Commons rejects with
    `imageinvalidfilename`. Restoring the substitution makes the
    constructed title match the stored title, so no drift fires."""
    title = get_page_title(
        "Lady Bird Johnson's Daily Diary Entry, 1/1/1968",
        "0b4d8351fe4b20831966e33905a2aa5a",
        ".pdf",
        page="1",
    )
    assert title.startswith("Lady Bird Johnson's Daily Diary Entry, 1-1-1968")
    assert "/" not in title


def test_get_page_title_replaces_all_colons_with_dashes():
    """MediaWiki's stripIllegalFilenameChars strips `:` from File-namespace
    titles unconditionally and replaces with `-` (filesystem path-separator
    concern). Our get_page_title must apply the same rule so the title we
    construct matches the title Commons stores on upload.

    Before May 2026 this code only broke a leading namespace-prefix colon
    and left mid-title colons intact on the false premise that Commons
    accepts them. The mismatch produced 5 NARA orphan duplicates whose
    legacy 2011 NARA-bot uploads (with `:`) silently coexisted with the
    new DPLA-bot uploads (with `-` after Commons normalised) — see PR #261.
    """
    title = get_page_title("Boston, Mass.: City Hall", "abcd" * 8, ".jpg")
    assert ":" not in title.split(" - DPLA -")[0]
    assert title.startswith("Boston, Mass.- City Hall")


def test_get_page_title_namespace_prefix_collision_still_broken():
    """The DPLA-records-starting-with-`Image:` case still works correctly:
    the colon is replaced (same as any other colon) so the API doesn't
    mis-route the upload into the File namespace alias. The mechanism is
    no longer a namespace-list lookup — it's just the global `:` → `-`
    rule that handles namespace and non-namespace cases identically."""
    title = get_page_title("Image: New England farmhouse", "abcd" * 8, ".jpg")
    assert title.startswith("Image- New England farmhouse")


def test_get_page_title_replaces_every_colon_not_just_the_first():
    """Multiple colons in the source title — all of them must be replaced
    so the constructed title matches what Commons stores. Previously only
    the first colon (after a namespace prefix) would be touched, leaving
    later colons to be normalised by Commons at upload time — exactly
    the source of the May 2026 NARA orphan duplicates."""
    title = get_page_title("Image: Boston, Mass.: City Hall", "abcd" * 8, ".jpg")
    pre_dpla = title.split(" - DPLA -")[0]
    assert ":" not in pre_dpla, f"every colon must be replaced; got: {pre_dpla!r}"
    assert pre_dpla == "Image- Boston, Mass.- City Hall"


def test_get_page_title_strips_backslash_lt_gt():
    """Companion to the colon fix. MediaWiki's stripIllegalFilenameChars
    strips `\\`, and any character not in Title::legalChars() — which
    includes `<` and `>` — from file titles. None of these are likely
    to appear in real DPLA item titles, but if they do, our constructed
    title must match what Commons stores so downstream title-equality
    checks don't silently mismatch (same class of bug as the colon case)."""
    title = get_page_title(
        "Edge<chars>here\\with backslash",
        "abcd" * 8,
        ".jpg",
    )
    pre_dpla = title.split(" - DPLA -")[0]
    for ch in ("\\", "<", ">"):
        assert ch not in pre_dpla, (
            f"character {ch!r} must be replaced; got: {pre_dpla!r}"
        )
    assert pre_dpla == "Edge-chars-here-with backslash"


def test_get_page_title_nara_native_american_delegations_case():
    """Regression pin for the exact NARA item that surfaced the bug:
    `Delegation: "Wooden Lance" (Kiowa)…`. Before the fix, get_page_title
    produced `Delegation: "Wooden Lance"…` and the file was uploaded to
    Commons under that title, which Commons then stored as
    `Delegation- "Wooden Lance"…`. The stored-vs-constructed mismatch
    silently disabled the duplicate_source_sha1s sibling check, producing
    an orphan-duplicate alongside the 2011 NARA-bot upload (also stored
    with the dash form because Commons normalised the same way). The
    fix: construct the dash form ourselves so the comparison succeeds."""
    title = get_page_title(
        'Delegation: "Wooden Lance" (Kiowa), "Apache John" (Apache)',
        "2617677db09ca8ae0dada8408e400191",
        ".tiff",
        page="1",
    )
    assert title.startswith(
        'Delegation- "Wooden Lance" (Kiowa), "Apache John" (Apache)'
    )
    assert ":" not in title


def test_license_to_markup_code():
    rights_uri = "http://creativecommons.org/licenses/by/4.0/"
    markup_code = license_to_markup_code(rights_uri)
    expected_code = "Cc-by-4.0"
    assert markup_code == expected_code


def test_get_permissions_template():
    rights_uri = "http://rightsstatements.org/vocab/NKC/1.0/"
    template = get_permissions_template(rights_uri)
    expected_template = "NKC"
    assert template == expected_template


def test_get_permissions():
    permissions = get_permissions(
        "http://rightsstatements.org/vocab/NKC/1.0/", "NKC", "Q12345"
    )
    expected_permissions = "NKC | Q12345"
    assert permissions == expected_permissions


def test_escape_wiki_strings():
    unescaped_string = "This is a [[test]] string with {{reserved}} characters."
    escaped_string = escape_wiki_strings(unescaped_string)
    expected_string = "This is a test string with reserved characters."
    assert escaped_string == expected_string


def test_join():
    strings = ["one", "two", "three"]
    joined_string = join(strings)
    expected_string = "one; two; three"
    assert joined_string == expected_string


def test_extract_strings():
    data = {"field": ["value1", "value2"]}
    extracted_string = extract_strings(data, "field")
    expected_string = "value1; value2"
    assert extracted_string == expected_string


def test_extract_strings_dict():
    data = {"field1": [{"field2": "value1"}, {"field2": "value2"}]}
    extracted_string = extract_strings_dict(data, "field1", "field2")
    expected_string = "value1; value2"
    assert extracted_string == expected_string


def test_wiki_file_exists():
    mock_http_response = MagicMock()
    mock_http_response.json.return_value = {
        "query": {"allimages": [{"name": "file1"}, {"name": "file2"}]}
    }
    mock_site = MagicMock()
    mock_site.allimages.return_value = []

    exists = wiki_file_exists(mock_site, "fakehash")
    assert not exists
    mock_site.allimages.assert_called_once()

    mock_site = MagicMock()
    mock_site.allimages.return_value = ["foo"]

    exists = wiki_file_exists(mock_site, "fakehash")
    assert exists
    mock_site.allimages.assert_called_once()


class _FakeHashFilePage:
    """A minimal FilePage stand-in for find_file_by_hash tests.

    ``title(with_ns=False)`` returns ``title``; ``oldest_file_info.timestamp``
    returns ``timestamp`` (or the property raises when ``raise_ts`` to simulate
    unreadable file history). A real class (not a MagicMock) so setting the
    raising property can't leak across tests via the shared mock class."""

    def __init__(self, title: str, timestamp=None, raise_ts: bool = False):
        self._title = title
        self._timestamp = timestamp
        self._raise_ts = raise_ts

    def title(self, with_ns: bool = False) -> str:
        return self._title

    @property
    def oldest_file_info(self):
        if self._raise_ts:
            raise RuntimeError("no history")
        return SimpleNamespace(timestamp=self._timestamp)


def test_find_file_by_hash_preferred_title_fast_path():
    """A file at the preferred_title is returned immediately, regardless of
    upload order or timestamps."""
    match = _FakeHashFilePage("Wanted.jpg", timestamp=500)
    other = _FakeHashFilePage("Other.jpg", timestamp=1)
    site = MagicMock()
    site.allimages.return_value = [other, match]

    result = find_file_by_hash(site, "somesha1", preferred_title="Wanted.jpg")
    assert result is match


def test_find_file_by_hash_returns_earliest_upload():
    """When two files share the SHA1 and neither matches preferred_title, the
    EARLIEST-uploaded file is canonical — even if the API lists a later upload
    first (alphabetically)."""
    newer = _FakeHashFilePage("Aaa newer.jpg", timestamp=200)  # alphabetical first
    older = _FakeHashFilePage("Zzz older.jpg", timestamp=100)  # alphabetical last
    site = MagicMock()
    site.allimages.return_value = [newer, older]

    result = find_file_by_hash(site, "somesha1", preferred_title=None)
    assert result is older


def test_find_file_by_hash_falls_back_to_first_when_timestamps_unreadable():
    """If no upload timestamp can be read, fall back to the API's first
    (alphabetical) result so a usable FilePage is still returned."""
    first = _FakeHashFilePage("A.jpg", raise_ts=True)
    second = _FakeHashFilePage("B.jpg", raise_ts=True)
    site = MagicMock()
    site.allimages.return_value = [first, second]

    result = find_file_by_hash(site, "somesha1", preferred_title=None)
    assert result is first


def test_find_file_by_hash_returns_none_when_no_match():
    site = MagicMock()
    site.allimages.return_value = []
    assert find_file_by_hash(site, "somesha1") is None


def test_escape_template_param_escapes_equals():
    # A positional value containing '=' would otherwise be split by the
    # template parser (name=value); escape_template_param protects it. This is
    # the escaping post_commonsdelinker_request relies on for titles like
    # "height of camera objective = 6.5 feet".
    assert escape_template_param("height = 6.5 feet") == "height {{=}} 6.5 feet"
    assert escape_template_param("a=b=c") == "a{{=}}b{{=}}c"


def test_escape_template_param_leaves_plain_value_untouched():
    assert escape_template_param("no equals here") == "no equals here"


class _FakeUploaderFilePage:
    """Minimal FilePage stand-in for first_uploader: ``oldest_file_info.user``
    returns ``user`` (or the property raises when ``raise_info`` to simulate
    unreadable file history)."""

    def __init__(self, user=None, raise_info=False):
        self._user = user
        self._raise = raise_info

    def title(self, with_ns: bool = False) -> str:
        return "Some File.jpg"

    @property
    def oldest_file_info(self):
        if self._raise:
            raise RuntimeError("no history")
        return SimpleNamespace(user=self._user)


def test_first_uploader_returns_oldest_uploader():
    assert first_uploader(_FakeUploaderFilePage(user="DPLA bot")) == "DPLA bot"


def test_first_uploader_none_when_user_empty():
    assert first_uploader(_FakeUploaderFilePage(user="")) is None


def test_first_uploader_none_when_history_unreadable():
    # oldest_file_info raising (unreadable history) is caught and returns None,
    # so the community-file classifier errs toward hands-off rather than crashing.
    assert first_uploader(_FakeUploaderFilePage(raise_info=True)) is None


NEW_WIKITEXT = "== {{int:filedesc}} ==\n{{DPLA metadata|title=Example}}"


def test_merge_preserved_wikitext_empty_existing():
    result = merge_preserved_wikitext("", NEW_WIKITEXT)
    assert result == NEW_WIKITEXT + "\n"


def test_merge_preserved_wikitext_preserves_pd_usgov():
    existing = "== {{int:license-header}} ==\n{{PD-USGov}}\n"
    result = merge_preserved_wikitext(existing, NEW_WIKITEXT)
    assert result == NEW_WIKITEXT + "\n\n{{PD-USGov}}\n"


def test_merge_preserved_wikitext_preserves_pd_usgov_variants():
    existing = "{{PD-USGov-Military-Army}} and {{PD-USGov-NARA}}"
    result = merge_preserved_wikitext(existing, NEW_WIKITEXT)
    assert "{{PD-USGov-Military-Army}}" in result
    assert "{{PD-USGov-NARA}}" in result


def test_merge_preserved_wikitext_preserves_pd_usgov_with_params():
    existing = "{{PD-USGov|date=1986}}"
    result = merge_preserved_wikitext(existing, NEW_WIKITEXT)
    assert "{{PD-USGov|date=1986}}" in result


def test_merge_preserved_wikitext_preserves_image_extracted():
    existing = "|other versions={{Image extracted|1=Parent Image.jpg}}"
    result = merge_preserved_wikitext(existing, NEW_WIKITEXT)
    assert "{{Image extracted|1=Parent Image.jpg}}" in result


def test_merge_preserved_wikitext_preserves_categories():
    existing = (
        "[[Category:Ronald Reagan in 1986]]\n"
        "[[Category:Nancy and Ronald Reagan]]\n"
        "[[Category:Files uploaded by RandomUserGuy1738]]\n"
    )
    result = merge_preserved_wikitext(existing, NEW_WIKITEXT)
    assert "[[Category:Ronald Reagan in 1986]]" in result
    assert "[[Category:Nancy and Ronald Reagan]]" in result
    assert "[[Category:Files uploaded by RandomUserGuy1738]]" in result


def test_merge_preserved_wikitext_preserves_category_with_sort_key():
    existing = "[[Category:Foo|Bar]]"
    result = merge_preserved_wikitext(existing, NEW_WIKITEXT)
    assert "[[Category:Foo|Bar]]" in result


def test_merge_preserved_wikitext_full_screenshot_case():
    # Mirrors the structure shown in the user-reported screenshot:
    # Information template with Image extracted in |other versions=,
    # PD-USGov license, then several categories.
    existing = (
        "== {{int:filedesc}} ==\n"
        "{{Information\n"
        "|description={{en|1=President Ronald Reagan ...}}\n"
        "|date={{Taken on|1986-07-04|location=United States}}\n"
        "|source=https://catalog.archives.gov/id/75854917\n"
        "|author=Series: Reagan White House Photographs\n"
        "|permission=\n"
        "|other versions={{Image extracted|1=Nancy Reagan during a trip.jpg}}\n"
        "}}\n"
        "\n"
        "== {{int:license-header}} ==\n"
        "{{PD-USGov}}\n"
        "\n"
        "[[Category:Ronald Reagan in 1986]]\n"
        "[[Category:Nancy and Ronald Reagan]]\n"
        "[[Category:Files uploaded by RandomUserGuy1738]]\n"
        "[[Category:1986 International Naval Review in New York]]\n"
    )
    result = merge_preserved_wikitext(existing, NEW_WIKITEXT)

    # New wikitext comes first
    assert result.startswith(NEW_WIKITEXT)
    # PD-USGov appears before any category
    pd_idx = result.index("{{PD-USGov}}")
    img_idx = result.index("{{Image extracted|1=Nancy Reagan during a trip.jpg}}")
    first_cat_idx = result.index("[[Category:Ronald Reagan in 1986]]")
    assert pd_idx < first_cat_idx
    assert img_idx < first_cat_idx
    # PD-USGov above Image extracted (license, then parent link)
    assert pd_idx < img_idx
    # All four categories preserved
    for cat in (
        "[[Category:Ronald Reagan in 1986]]",
        "[[Category:Nancy and Ronald Reagan]]",
        "[[Category:Files uploaded by RandomUserGuy1738]]",
        "[[Category:1986 International Naval Review in New York]]",
    ):
        assert cat in result


def test_merge_preserved_wikitext_dedupes():
    existing = (
        "[[Category:Foo]] [[Category:Foo]] "
        "{{PD-USGov}} {{PD-USGov}} "
        "{{Image extracted|1=A.jpg}} {{Image extracted|1=A.jpg}}"
    )
    result = merge_preserved_wikitext(existing, NEW_WIKITEXT)
    assert result.count("[[Category:Foo]]") == 1
    assert result.count("{{PD-USGov}}") == 1
    assert result.count("{{Image extracted|1=A.jpg}}") == 1


def test_merge_preserved_wikitext_no_metadata_returns_new_wikitext_unchanged():
    existing = (
        "Some unrelated wikitext with no license, image-extracted, or categories."
    )
    result = merge_preserved_wikitext(existing, NEW_WIKITEXT)
    assert result == NEW_WIKITEXT + "\n"


def test_merge_preserved_wikitext_dedupes_against_new_wikitext():
    """If the new wikitext already carries the same category /
    license / image-extracted template that the existing page has,
    the rescue must NOT re-emit it. MediaWiki silently dedupes
    repeated category memberships at render time, but the saved
    wikitext source would show two ``[[Category:X]]`` lines and
    confuse Commons editors reviewing the rescue edit.

    Regression: caught in production on rev 1235738403 (Heartland
    "1813 Five Francs Coin" rescue) where the new file already had
    ``[[Category:Gifts to Charles Lindbergh]]`` and the old file
    contributed the same category — the merged result re-emitted it.
    """
    new_with_category = NEW_WIKITEXT + "\n\n[[Category:Already Present]]\n"
    existing = (
        "{{PD-USGov}}\n"
        "{{Image extracted|1=Parent.jpg}}\n"
        "[[Category:Already Present]]\n"
        "[[Category:Brand New From Existing]]\n"
    )
    result = merge_preserved_wikitext(existing, new_with_category)
    # The duplicate category must appear exactly once.
    assert result.count("[[Category:Already Present]]") == 1
    # The new category from the existing page must still be added.
    assert "[[Category:Brand New From Existing]]" in result
    # Non-category preserves that aren't in new_wikitext go through normally.
    assert "{{PD-USGov}}" in result
    assert "{{Image extracted|1=Parent.jpg}}" in result


def test_merge_preserved_wikitext_dedupes_non_category_templates_too():
    """Same dedup contract applies to PD-USGov, Image-extracted,
    and Assessment templates — not just categories."""
    new_with_pd = NEW_WIKITEXT + "\n{{PD-USGov}}\n"
    existing = "{{PD-USGov}}\n[[Category:Foo]]\n"
    result = merge_preserved_wikitext(existing, new_with_pd)
    assert result.count("{{PD-USGov}}") == 1
    assert "[[Category:Foo]]" in result


def test_merge_preserved_wikitext_categories_flow_without_blank_line():
    """When the new wikitext already ends with a category line,
    the rescued categories must append directly — no blank-line
    separator between the existing categories and the rescued ones.

    Regression: same Heartland rescue rev 1235738403 had a blank
    line between the new wikitext's last category and the rescued
    block, making the source diff look like two separate category
    groups rather than one contiguous block.
    """
    new_ending_in_category = (
        "{{ Artwork | title = X }}\n\n[[Category:NewA]]\n[[Category:NewB]]\n"
    )
    existing = "[[Category:OldA]]\n[[Category:OldB]]\n"
    result = merge_preserved_wikitext(existing, new_ending_in_category)
    # All four categories should appear, contiguous, no blank line between.
    lines = [ln for ln in result.splitlines() if ln.strip()]
    cat_indices = [i for i, ln in enumerate(lines) if ln.startswith("[[Category:")]
    # The four category lines must occupy four consecutive positions.
    assert cat_indices == list(range(cat_indices[0], cat_indices[0] + 4))
    # The rescued categories come after the existing ones (preserve order).
    assert lines[cat_indices[0]] == "[[Category:NewA]]"
    assert lines[cat_indices[0] + 1] == "[[Category:NewB]]"
    assert lines[cat_indices[0] + 2] == "[[Category:OldA]]"
    assert lines[cat_indices[0] + 3] == "[[Category:OldB]]"
    # No blank line inside the category block.
    cat_block_text = "\n".join(result.splitlines()[cat_indices[0] : cat_indices[0] + 4])
    assert "\n\n" not in cat_block_text


def test_merge_preserved_wikitext_overlapping_category_names():
    """Two categories that share a name-prefix (e.g.
    ``[[Category:American]]`` and ``[[Category:American history]]``)
    are distinct categories — the dedup must NOT filter one because
    it appears as a substring of the other. The closing ``]]``
    terminator on every preserve pattern already prevents this in
    practice (``[[Category:American]]`` is not a substring of
    ``[[Category:American history]]``), but pinning the contract by
    test rules out the regression if the regex shape changes."""
    new_with_longer = NEW_WIKITEXT + "\n\n[[Category:American history]]\n"
    existing = "[[Category:American]]\n"
    result = merge_preserved_wikitext(existing, new_with_longer)
    assert "[[Category:American history]]" in result
    assert "[[Category:American]]" in result
    # Both appear exactly once.
    assert (
        result.count("[[Category:American]]\n")
        + result.count("[[Category:American]]")
        - result.count("[[Category:American history]]")
        == 1
    )


def test_merge_preserved_wikitext_blank_line_still_present_when_new_does_not_end_in_category():
    """The blank-line separator is only suppressed when the new
    wikitext already ends with a category line. When it ends with
    a template (the common path — new wikitext is a freshly
    generated ``{{DPLA metadata}}`` block with no categories), the
    rescue still emits a blank line before the categories for
    readability."""
    existing = "[[Category:OldA]]\n[[Category:OldB]]\n"
    # NEW_WIKITEXT ends with `{{DPLA metadata|title=Example}}` — not a category.
    result = merge_preserved_wikitext(existing, NEW_WIKITEXT)
    assert "{{DPLA metadata|title=Example}}\n\n[[Category:OldA]]" in result


# ---------------------------------------------------------------------------
# Assessment-template preservation (Media of the day, etc.)
# ---------------------------------------------------------------------------


def test_merge_preserved_wikitext_preserves_media_of_the_day():
    """Regression: {{Media of the day|YEAR|MONTH|DAY}} (and the
    `=={{Assessment}}== ` header that wraps it) must survive the metadata
    rescue overwrite. Earlier the rescue path silently dropped the entire
    Assessment block — observed on the May-Ling Soong Chiang address
    file (revid 1222566342), where the file lost its MOTD designation
    for 2023-02-18 in a title-drift rewrite.
    """
    existing = (
        "== {{int:filedesc}} ==\n"
        "{{Information |description=...|date=1943-02-18 }}\n"
        "\n"
        "=={{Assessment}}==\n"
        "{{Media of the day|2023|2|18}}\n"
        "\n"
        "=={{int:license-header}}==\n"
        "{{PD-USGov-Congress}}\n"
    )
    result = merge_preserved_wikitext(existing, NEW_WIKITEXT)

    # The MOTD template survives with all three date components.
    assert "{{Media of the day|2023|2|18}}" in result
    # The Assessment header is emitted above it (Commons convention; an
    # MOTD-archive scraper looking for that wrapper would miss a bare
    # template).
    assessment_idx = result.index("=={{Assessment}}==")
    motd_idx = result.index("{{Media of the day|2023|2|18}}")
    assert assessment_idx < motd_idx, (
        "Assessment header must appear above the preserved MOTD template; "
        f"got assessment_idx={assessment_idx}, motd_idx={motd_idx}"
    )
    # Assessment block goes BETWEEN the new-wikitext block and the license
    # (matches Commons page-structure convention).
    license_idx = result.index("{{PD-USGov-Congress}}")
    new_wikitext_end = len(NEW_WIKITEXT)
    assert new_wikitext_end <= assessment_idx < license_idx, (
        "Assessment block must go after the new wikitext and before the license; "
        f"got new_wikitext_end={new_wikitext_end}, assessment_idx={assessment_idx}, "
        f"license_idx={license_idx}"
    )


def test_merge_preserved_wikitext_preserves_picture_of_the_day():
    """POTD has the same shape as MOTD and must round-trip identically."""
    existing = (
        "== {{int:filedesc}} ==\nold description\n{{Picture of the day|2024|6|15}}\n"
    )
    result = merge_preserved_wikitext(existing, NEW_WIKITEXT)
    assert "{{Picture of the day|2024|6|15}}" in result
    assert "=={{Assessment}}==" in result


def test_merge_preserved_wikitext_preserves_quality_image_featured_valued():
    """Status templates other than the time-stamped MOTD/POTD pair —
    {{Featured picture}}, {{Quality image}}, {{Valued image}} — must
    survive too. They're conferred by Commons editorial processes and
    losing them in a rescue rewrite quietly demotes the file."""
    existing = "{{Featured picture}}\n{{Quality image}}\n{{Valued image}}\n"
    result = merge_preserved_wikitext(existing, NEW_WIKITEXT)
    assert "{{Featured picture}}" in result
    assert "{{Quality image}}" in result
    assert "{{Valued image}}" in result
    # Single Assessment header for the whole block, not one per template.
    assert result.count("=={{Assessment}}==") == 1


def test_merge_preserved_wikitext_preserves_assessments_bundle():
    """The {{Assessments|...}} bundle template encodes multiple statuses
    in one call (e.g. `{{Assessments|featured=1|quality=1}}`). Must be
    preserved with its parameters intact."""
    existing = "{{Assessments|featured=1|quality=1|com_nom=Foo.jpg}}"
    result = merge_preserved_wikitext(existing, NEW_WIKITEXT)
    assert "{{Assessments|featured=1|quality=1|com_nom=Foo.jpg}}" in result


def test_merge_preserved_wikitext_assessment_template_case_insensitive():
    """Commons editors use both `{{Media of the day|...}}` and the
    lowercase `{{media of the day|...}}` variant. Both must be picked
    up by the preservation pattern."""
    existing = "{{media of the day|2023|2|18}}"
    result = merge_preserved_wikitext(existing, NEW_WIKITEXT)
    assert "{{media of the day|2023|2|18}}" in result


def test_merge_preserved_wikitext_no_assessment_header_when_no_templates():
    """When the existing page has no assessment-class templates at all,
    no synthetic Assessment header must be added to the rewrite.
    Otherwise every rescue would emit a dangling header above bare
    license templates."""
    existing = "{{PD-USGov}}\n[[Category:Foo]]"
    result = merge_preserved_wikitext(existing, NEW_WIKITEXT)
    assert "=={{Assessment}}==" not in result


def test_merge_preserved_wikitext_dedupes_assessment_templates():
    """Repeated MOTD designations (rare but possible if a file was
    featured twice) must collapse to a single entry — same dedup
    treatment as the other preserved-content groups."""
    existing = "{{Media of the day|2023|2|18}}\n{{Media of the day|2023|2|18}}\n"
    result = merge_preserved_wikitext(existing, NEW_WIKITEXT)
    assert result.count("{{Media of the day|2023|2|18}}") == 1


def test_merge_preserved_wikitext_assessment_does_not_false_match_other_templates():
    """The pattern must not pick up unrelated templates whose names
    happen to share a prefix (e.g. `{{Picture of the day request|...}}`
    is NOT the same as `{{Picture of the day|...}}` and shouldn't be
    preserved as if it were)."""
    existing = "{{Picture of the day request|reason=test}}"
    result = merge_preserved_wikitext(existing, NEW_WIKITEXT)
    # The whole template should not be picked up — it's a request, not a
    # designation. (If it WERE matched, the Assessment header would also
    # appear.)
    assert "{{Picture of the day request" not in result
    assert "=={{Assessment}}==" not in result


def test_extract_page_ordinal_multipage():
    title = "Plant Life - DPLA - 002b0f7ad761858506721b83e3370c5f (page 17).jpg"
    assert extract_page_ordinal_from_commons_title(title) == 17


def test_extract_page_ordinal_single_page():
    # Single-page DPLA items have no (page N) suffix
    title = "President Ronald Reagan - DPLA - b3fb229b867046ddd9418c00289245ce.jpg"
    assert extract_page_ordinal_from_commons_title(title) is None


def test_extract_page_ordinal_no_dpla_format():
    # Non-DPLA titles also return None
    title = "President Ronald Reagan talking aboard Air Force One.jpg"
    assert extract_page_ordinal_from_commons_title(title) is None


def test_extract_page_ordinal_high_page_number():
    title = "Pennsylvania Company Volume - DPLA - 148712806694602ddddfdec4a84e0229 (page 700).jpg"
    assert extract_page_ordinal_from_commons_title(title) == 700


def test_extract_page_ordinal_ignores_non_suffix_page_token():
    # "(page 12)" embedded in the descriptive title text — not the DPLA
    # filename suffix — must not be parsed as the ordinal.
    title = "Diary notes (page 12) - DPLA - 002b0f7ad761858506721b83e3370c5f.jpg"
    assert extract_page_ordinal_from_commons_title(title) is None


def test_extract_page_ordinal_ignores_non_suffix_with_multipage():
    # Same as above but the file IS multi-page; ordinal must come from the
    # tail, not from the spurious "(page 12)" in the title text.
    title = (
        "Diary notes (page 12) - DPLA - 002b0f7ad761858506721b83e3370c5f (page 5).jpg"
    )
    assert extract_page_ordinal_from_commons_title(title) == 5


# is_same_item_redirect_relic — the oscillation guard for redirect handling
ITEM = "190aa1b74ca34559e61c25e9dbb97a61"
PHYS = "Physical Apparatus for Universities and Colleges Manufactured and Imported by Central Scientific Co."


def test_relic_same_item_different_pages_is_true():
    intended = f"{PHYS} - DPLA - {ITEM} (page 83).jpg"
    target = f"{PHYS} - DPLA - {ITEM} (page 103).jpg"
    assert is_same_item_redirect_relic(intended, target, ITEM) is True


def test_relic_same_item_same_page_is_false():
    # Same DPLA ID and same ordinal — legitimate title-text rename, let
    # _resolve_redirect_move handle it.
    intended = f"New text - DPLA - {ITEM} (page 5).jpg"
    target = f"Old text - DPLA - {ITEM} (page 5).jpg"
    assert is_same_item_redirect_relic(intended, target, ITEM) is False


def test_relic_different_item_is_false():
    # Different DPLA IDs — not a same-item case at all.
    other_id = "fe9a56003cde86d71a7f68bcc42c9216"
    intended = f"Foo - DPLA - {ITEM} (page 1).jpg"
    target = f"Emblema XIII - DPLA - {other_id}.jpg"
    assert is_same_item_redirect_relic(intended, target, ITEM) is False


def test_relic_single_page_item_is_false():
    # No (page N) suffix on either side — single-page item, title-text
    # rename. Let _resolve_redirect_move handle it.
    intended = f"New title - DPLA - {ITEM}.jpg"
    target = f"Old title - DPLA - {ITEM}.jpg"
    assert is_same_item_redirect_relic(intended, target, ITEM) is False


def test_relic_target_lacks_dpla_id_is_false():
    # Target was uploaded under an institutional title (no DPLA ID) — that
    # IS the legitimate Case 1 title-drift scenario; allow the move.
    intended = f"Reagan - DPLA - {ITEM}.jpg"
    target = "President Ronald Reagan original NARA title.jpg"
    assert is_same_item_redirect_relic(intended, target, ITEM) is False


def test_relic_only_one_side_has_page_ordinal_is_false():
    # Target has (page N) but intended doesn't (or vice versa). Treat as
    # not-a-relic — let the existing move path handle it.
    intended = f"Foo - DPLA - {ITEM}.jpg"
    target = f"Foo - DPLA - {ITEM} (page 5).jpg"
    assert is_same_item_redirect_relic(intended, target, ITEM) is False


def test_relic_intended_belongs_to_different_item_is_false():
    # intended_title carries a parseable (page N) ordinal but its DPLA ID
    # is a third item, not the one being processed. Must not be classified
    # as a same-item relic.
    other_id = "fe9a56003cde86d71a7f68bcc42c9216"
    intended = f"Foo - DPLA - {other_id} (page 1).jpg"
    target = f"Foo - DPLA - {ITEM} (page 3).jpg"
    assert is_same_item_redirect_relic(intended, target, ITEM) is False


# compute_ordinal_exts_and_page_labels — uploader-side gap-squashing
def _fake_s3_client(mime_by_ord: dict[int, str | None]) -> MagicMock:
    """Build a mock S3Client whose Object().content_type returns the configured
    MIME per ordinal. None means the object doesn't exist (raises ClientError)."""
    from botocore.exceptions import ClientError

    s3_client = MagicMock()

    def get_obj(bucket, path):
        ord_num = int(path.rsplit("/", 1)[-1].split("_", 1)[0])
        mime = mime_by_ord.get(ord_num)
        if mime is None:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "404"}}, "HeadObject"
            )
        obj = MagicMock()
        obj.content_type = mime
        return obj

    boto3_resource = MagicMock()
    boto3_resource.Object.side_effect = get_obj
    s3_client.get_s3.return_value = boto3_resource
    s3_client.get_media_s3_path.side_effect = lambda d, i, p: (
        f"{p}/images/{d[0]}/{d[1]}/{d[2]}/{d[3]}/{d}/{i}_{d}"
    )
    return s3_client


def test_page_labels_dense_jpegs_match_ordinal():
    # Happy path: all 5 ordinals are valid jpegs → page_label == ordinal.
    s3 = _fake_s3_client(
        {
            1: "image/jpeg",
            2: "image/jpeg",
            3: "image/jpeg",
            4: "image/jpeg",
            5: "image/jpeg",
        }
    )
    exts, labels = compute_ordinal_exts_and_page_labels(s3, "abc" * 11, "pa", 5)
    assert exts == {1: ".jpg", 2: ".jpg", 3: ".jpg", 4: ".jpg", 5: ".jpg"}
    assert labels == {1: "1", 2: "2", 3: "3", 4: "4", 5: "5"}


def test_page_labels_zero_byte_stub_squashes():
    # Stub at ord 3 (octet-stream) shifts subsequent .jpg labels back by 1.
    # This is the gap-squashing behavior the bot intentionally uses.
    s3 = _fake_s3_client(
        {
            1: "image/jpeg",
            2: "image/jpeg",
            3: "binary/octet-stream",
            4: "image/jpeg",
            5: "image/jpeg",
        }
    )
    exts, labels = compute_ordinal_exts_and_page_labels(s3, "abc" * 11, "pa", 5)
    assert exts == {1: ".jpg", 2: ".jpg", 3: "", 4: ".jpg", 5: ".jpg"}
    # ord 1-2 = "1","2"; ord 3 ext="" only one, page_label=""; ord 4-5 = "3","4"
    assert labels == {1: "1", 2: "2", 3: "", 4: "3", 5: "4"}


def test_page_labels_clienterror_treated_as_stub_placeholder():
    # Missing S3 object — ordinal_exts gets "" placeholder per the
    # "every continue path must write a placeholder slot" lesson.
    s3 = _fake_s3_client({1: "image/jpeg", 2: None, 3: "image/jpeg"})
    exts, labels = compute_ordinal_exts_and_page_labels(s3, "abc" * 11, "pa", 3)
    assert exts == {1: ".jpg", 2: "", 3: ".jpg"}
    assert labels == {1: "1", 2: "", 3: "2"}


# collect_duplicate_source_sha1s — drift logic must recognize legit duplicates
def _fake_s3_client_with_sha1s(sha1_by_ord: dict[int, str | None]):
    """Mock S3Client whose Object().metadata['sha1'] returns the configured sha1
    per ordinal. None means the ordinal raises (treated as missing)."""
    s3_client = MagicMock()

    def get_obj(bucket, path):
        ord_num = int(path.rsplit("/", 1)[-1].split("_", 1)[0])
        sha1 = sha1_by_ord.get(ord_num)
        if sha1 is None:
            raise RuntimeError("missing")
        obj = MagicMock()
        obj.metadata = {"sha1": sha1}
        return obj

    boto3_resource = MagicMock()
    boto3_resource.Object.side_effect = get_obj
    s3_client.get_s3.return_value = boto3_resource
    s3_client.get_media_s3_path.side_effect = lambda d, i, p: (
        f"{p}/images/{d[0]}/{d[1]}/{d[2]}/{d[3]}/{d}/{i}_{d}"
    )
    return s3_client


def test_collect_duplicate_source_sha1s_all_unique():
    s3 = _fake_s3_client_with_sha1s({1: "aaa", 2: "bbb", 3: "ccc"})
    assert collect_duplicate_source_sha1s(s3, "abc" * 11, "pa", 3) == set()


def test_collect_duplicate_source_sha1s_one_pair_repeats():
    # Sherman pattern: same content at positions 1 and 12.
    s3 = _fake_s3_client_with_sha1s(
        {
            1: "aaa",
            2: "bbb",
            3: "ccc",
            4: "ddd",
            5: "eee",
            6: "fff",
            7: "ggg",
            8: "hhh",
            9: "iii",
            10: "jjj",
            11: "kkk",
            12: "aaa",
            13: "bbb",
        }
    )
    assert collect_duplicate_source_sha1s(s3, "abc" * 11, "pa", 13) == {"aaa", "bbb"}


def test_collect_duplicate_source_sha1s_same_sha1_three_times():
    s3 = _fake_s3_client_with_sha1s({1: "aaa", 2: "bbb", 3: "aaa", 4: "aaa"})
    assert collect_duplicate_source_sha1s(s3, "abc" * 11, "pa", 4) == {"aaa"}


def test_collect_duplicate_source_sha1s_handles_missing_metadata():
    # Read failures are tolerated; affected ordinals are absent from the count.
    s3 = _fake_s3_client_with_sha1s({1: "aaa", 2: None, 3: "aaa"})
    assert collect_duplicate_source_sha1s(s3, "abc" * 11, "pa", 3) == {"aaa"}


def test_collect_duplicate_source_sha1s_logs_skipped_ordinals(caplog):
    """Silent skips would hide drift miscorrection; ensure each failed read
    produces a WARNING line so an operator can see why the count is low."""
    import logging as _logging

    s3 = _fake_s3_client_with_sha1s({1: "aaa", 2: None, 3: "aaa"})
    with caplog.at_level(_logging.WARNING):
        collect_duplicate_source_sha1s(s3, "abc" * 11, "pa", 3)
    messages = " | ".join(r.message for r in caplog.records)
    assert "skipped ordinal 2" in messages
    assert "under-count" in messages


def test_collect_duplicate_source_sha1s_skips_empty_sha1():
    # Empty sha1 metadata (e.g. truncated) is not counted as duplicate-of-itself.
    s3 = _fake_s3_client_with_sha1s({1: "", 2: "", 3: "aaa"})
    assert collect_duplicate_source_sha1s(s3, "abc" * 11, "pa", 3) == set()


def test_page_labels_non_404_clienterror_propagates():
    # Transient S3 failures (AccessDenied, InternalError, throttling) must
    # not be silently swallowed as "object missing" — that would corrupt
    # the page-label assignment on what's actually a valid file.
    import pytest
    from botocore.exceptions import ClientError

    s3 = MagicMock()
    s3.get_media_s3_path.side_effect = lambda d, i, p: (
        f"{p}/images/{d[0]}/{d[1]}/{d[2]}/{d[3]}/{d}/{i}_{d}"
    )
    s3.get_s3.return_value.Object.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "nope"}}, "HeadObject"
    )
    with pytest.raises(ClientError):
        compute_ordinal_exts_and_page_labels(s3, "abc" * 11, "pa", 3)


def test_page_labels_single_file_item_no_pagination():
    # num_files == 1 → no pre-scan, no page label, no (page N) suffix.
    s3 = _fake_s3_client({1: "image/jpeg"})
    exts, labels = compute_ordinal_exts_and_page_labels(s3, "abc" * 11, "pa", 1)
    assert exts == {}
    assert labels == {1: ""}


def test_page_labels_mixed_extensions_per_ext_counter():
    # 3 jpegs and 2 pdfs → each extension gets its own 1..N counter.
    s3 = _fake_s3_client(
        {
            1: "image/jpeg",
            2: "application/pdf",
            3: "image/jpeg",
            4: "application/pdf",
            5: "image/jpeg",
        }
    )
    exts, labels = compute_ordinal_exts_and_page_labels(s3, "abc" * 11, "pa", 5)
    assert exts == {1: ".jpg", 2: ".pdf", 3: ".jpg", 4: ".pdf", 5: ".jpg"}
    # jpg counter: ord 1→"1", ord 3→"2", ord 5→"3"
    # pdf counter: ord 2→"1", ord 4→"2"
    assert labels == {1: "1", 2: "1", 3: "2", 4: "2", 5: "3"}


def test_page_labels_unique_extension_gets_empty_label():
    # When only one file has a given extension, no (page N) suffix.
    s3 = _fake_s3_client({1: "image/jpeg", 2: "image/jpeg", 3: "application/pdf"})
    exts, labels = compute_ordinal_exts_and_page_labels(s3, "abc" * 11, "pa", 3)
    assert exts == {1: ".jpg", 2: ".jpg", 3: ".pdf"}
    # ext_counts: .jpg→2, .pdf→1. Only .jpg gets page numbers.
    assert labels == {1: "1", 2: "2", 3: ""}


# ---------------------------------------------------------------------------
# build_title_drift_move_reason
# ---------------------------------------------------------------------------

_DPLA_ID = "093240d1a832ebe2a5fe2af4f5847762"
_USERNAME = "DPLA bot"


def _composed_move_comment(old: str, new: str, reason: str, user: str) -> str:
    """Mirror MediaWiki's auto-composed move comment for length verification."""
    return f"{user} moved page [[File:{old}]] to [[File:{new}]]: {reason}"


def test_move_reason_short_filenames_keeps_link():
    """Default reason (with [[dpla:...|...]] link) is used when there is room."""
    old = "Short - NARA - 123.gif"
    new = f"Short - DPLA - {_DPLA_ID}.gif"
    reason = build_title_drift_move_reason(old, new, _DPLA_ID, _USERNAME)
    assert "[[dpla:" in reason
    assert _DPLA_ID in reason
    assert (
        len(_composed_move_comment(old, new, reason, _USERNAME).encode("utf-8"))
        <= MAX_COMMENT_BYTES
    )


def test_move_reason_long_filenames_drops_link():
    """When the link would push the comment over 500 bytes, drop the link
    wrapper but keep the bare DPLA ID."""
    # Filenames from the real overflow case (revid 1218006806).
    long_title = (
        "Undated typescript copy of the citation as signed by President "
        "Franklin D. Roosevelt awarding the Medal of Honor to Major Gregory "
        "Boyington."
    )
    old = f"{long_title} - NARA - 299738.gif"
    new = f"{long_title} - DPLA - {_DPLA_ID}.gif"
    reason = build_title_drift_move_reason(old, new, _DPLA_ID, _USERNAME)
    assert "[[dpla:" not in reason
    assert _DPLA_ID in reason
    assert reason.startswith("Title drift correction")
    composed = _composed_move_comment(old, new, reason, _USERNAME)
    assert len(composed.encode("utf-8")) <= MAX_COMMENT_BYTES


def test_move_reason_picks_longest_that_fits():
    """When the link form is just over budget, the next-longest variant
    (link dropped, full descriptive text kept) is selected."""
    # Construct filenames so that the full link form is exactly 1 byte over
    # budget but the no-link form fits.
    link_reason = (
        f"Title drift correction: updating to current DPLA title "
        f"(DPLA ID [[dpla:{_DPLA_ID}|{_DPLA_ID}]])"
    )
    no_link_reason = (
        f"Title drift correction: updating to current DPLA title (DPLA ID {_DPLA_ID})"
    )
    # Per the helper: prefix_len = len(user) + 36 + len(old) + len(new).
    # Pick filename lengths so total comment with link is 501 bytes.
    overhead = len(_USERNAME) + 36
    target_filename_total = MAX_COMMENT_BYTES + 1 - overhead - len(link_reason)
    old = "a" * (target_filename_total // 2)
    new = "b" * (target_filename_total - len(old))
    reason = build_title_drift_move_reason(old, new, _DPLA_ID, _USERNAME)
    assert reason == no_link_reason
    composed = _composed_move_comment(old, new, reason, _USERNAME)
    assert len(composed.encode("utf-8")) <= MAX_COMMENT_BYTES


def test_move_reason_extreme_filenames_falls_back_to_minimum():
    """When filenames alone consume the entire budget, the shortest reason
    is returned (helper does its best; MW will still truncate)."""
    # Pick filenames so even "Title drift correction" doesn't fit.
    old = "x" * 240
    new = "y" * 240
    reason = build_title_drift_move_reason(old, new, _DPLA_ID, _USERNAME)
    assert reason == "Title drift correction"


# ---------------------------------------------------------------------------
# post_commonsdelinker_request — atomic-append behaviour
# ---------------------------------------------------------------------------


def test_post_commonsdelinker_request_uses_appendtext_not_read_modify_write():
    """post_commonsdelinker_request must call site.editpage(...) with the
    `appendtext` kwarg, not the read-modify-write `page.text = page.text +
    template; page.save()` form.

    The naive form pulls the entire (~230 KB and growing) filemovers page
    into the process on every call AND races itself when our bot is the
    only editor: pywikibot's GET is load-balanced across MediaWiki replica
    databases that may lag the primary, so the basetimestamp pywikibot
    sends with the POST is older than the revision OUR previous successful
    edit just produced. The primary rejects with EditConflictError — a
    self-inflicted conflict from the perspective of the only editor.

    `appendtext` is server-side atomic: no read, no basetimestamp, no
    race. This test pins both halves of that contract.
    """
    # Old.jpg has inbound usage, so the usage gate lets the post through.
    site = _usage_site(
        {"query": {"pages": {"-1": {"globalusage": [{"title": "w:en:Example"}]}}}}
    )
    # If anything tries to read page.text the test fails loudly: that
    # would mean we slipped back into the naive form.
    page_text_accessed = MagicMock(
        side_effect=AssertionError("page.text must not be accessed — use appendtext")
    )
    with (
        patch("ingest_wikimedia.wikimedia.pywikibot.Page") as PageCtor,
    ):
        page = PageCtor.return_value
        type(page).text = property(page_text_accessed)
        post_commonsdelinker_request(
            site, old_filename="Old.jpg", new_filename="New.jpg"
        )

    # Page constructed for the right target.
    PageCtor.assert_called_once_with(site, COMMONSDELINKER_PAGE)
    # editpage called on the *site* (the API surface that accepts appendtext),
    # not via page.save (which would re-introduce the read).
    assert site.editpage.call_count == 1, (
        f"expected exactly one site.editpage() call, got {site.editpage.call_count}"
    )
    _, kwargs = site.editpage.call_args
    assert "appendtext" in kwargs, (
        f"editpage call must use appendtext; got kwargs={kwargs!r}"
    )
    # The leading newline matters: it guarantees the new request lands on
    # its own line regardless of whether the existing page ends with one.
    assert kwargs["appendtext"].startswith("\n"), (
        "appendtext must begin with a newline so the new request is "
        f"line-separated from the existing content; got {kwargs['appendtext']!r}"
    )
    # The request template carries the universal-replace shape.
    assert "{{universal replace" in kwargs["appendtext"]
    assert "|Old.jpg" in kwargs["appendtext"]
    assert "|New.jpg" in kwargs["appendtext"]
    # page.save must not be called — that path re-introduces the read.
    page.save.assert_not_called()


def test_post_commonsdelinker_request_escapes_equals_in_filenames():
    """A filename containing ``=`` must reach the {{universal replace}} template
    in its ``{{=}}`` form. A raw ``=`` in a positional filename would be parsed
    as a param name/value split and mangle the relink request (DPLA preserves
    ``=`` from source titles, so this shape occurs in the wild). Pins that
    post_commonsdelinker_request applies escape_template_param to the filenames
    it emits — an integration guard the escape_template_param unit test alone
    can't provide (a caller that dropped the escaping would still pass it)."""
    site = MagicMock()
    with patch("ingest_wikimedia.wikimedia.pywikibot.Page"):
        post_commonsdelinker_request(
            site,
            old_filename="Old = one.jpg",
            new_filename="New = two.jpg",
            check_usage=False,
        )
    appendtext = site.editpage.call_args.kwargs["appendtext"]
    # Both filenames land in escaped form...
    assert "Old {{=}} one.jpg" in appendtext
    assert "New {{=}} two.jpg" in appendtext
    # ...and no raw, unescaped filename '=' leaks into the positional params.
    # (The legitimate ``|reason=`` '=' is a different substring, so scoping the
    # assertion to the filename strings keeps it precise.)
    assert "Old = one.jpg" not in appendtext
    assert "New = two.jpg" not in appendtext


def _usage_site(submit_return=None, submit_exc=None):
    """A mock site whose simple_request(...).submit() returns the given
    query result (or raises). Used to drive the usage gate."""
    site = MagicMock()
    sub = site.simple_request.return_value.submit
    if submit_exc is not None:
        sub.side_effect = submit_exc
    else:
        sub.return_value = submit_return
    return site


def test_file_has_inbound_usage_one_combined_request_global_or_local():
    """Global and local usage are fetched in a SINGLE request
    (prop=globalusage|fileusage); usage in either class counts as used,
    and neither counts as unused. ``redirects`` must not be set."""
    # global only
    site = _usage_site({"query": {"pages": {"-1": {"globalusage": [{"x": 1}]}}}})
    assert file_has_inbound_usage(site, "F.jpg") is True
    # exactly one API request, with the combined prop and no redirect-following
    assert site.simple_request.call_count == 1
    _, kwargs = site.simple_request.call_args
    assert kwargs["action"] == "query"
    assert set(kwargs["prop"].split("|")) == {"globalusage", "fileusage"}
    assert kwargs["titles"] == "File:F.jpg"
    assert "redirects" not in kwargs
    # fileusage limit must be >=2: the file's own page always occupies one
    # row (self-reference), so a limit of 1 could only ever return self and
    # would mask a genuine second user.
    assert kwargs["fulimit"] == 2

    # local only (another page embeds it)
    site = _usage_site({"query": {"pages": {"-1": {"fileusage": [{"title": "X"}]}}}})
    assert file_has_inbound_usage(site, "F.jpg") is True

    # neither
    site = _usage_site({"query": {"pages": {"-1": {}}}})
    assert file_has_inbound_usage(site, "F.jpg") is False


def test_file_has_inbound_usage_ignores_self_reference():
    """A DPLA file page lists itself in fileusage — {{Artwork}}/{{Information}}
    with no image param auto-displays the page's own image. That self-row is
    not an external relink target, so a file used ONLY by itself is unused.
    (This is what made the CommonsDelinker gate fire for every file.)"""
    site = _usage_site(
        {"query": {"pages": {"-1": {"fileusage": [{"title": "File:F.jpg"}]}}}}
    )
    assert file_has_inbound_usage(site, "F.jpg") is False


def test_file_has_inbound_usage_counts_other_user_alongside_self():
    """Self-reference plus a genuine external user → used. fulimit=2 ensures
    the non-self row is fetched even though self occupies one slot."""
    site = _usage_site(
        {
            "query": {
                "pages": {
                    "-1": {
                        "fileusage": [
                            {"title": "File:F.jpg"},
                            {"title": "File:Some gallery page.jpg"},
                        ]
                    }
                }
            }
        }
    )
    assert file_has_inbound_usage(site, "F.jpg") is True


def test_file_has_inbound_usage_fails_open_on_error():
    """A query error must fail OPEN (return True) so a needed relink is
    never silently dropped on a transient API failure."""
    site = _usage_site(submit_exc=RuntimeError("api down"))
    assert file_has_inbound_usage(site, "F.jpg") is True


def test_file_has_inbound_usage_fails_open_on_malformed_payload():
    """An unexpected payload shape (parse error) must also fail OPEN, not
    raise or fall through to False — the parse runs inside the try."""
    # `pages` as a list rather than the expected dict → .values() raises.
    site = _usage_site({"query": {"pages": ["unexpected"]}})
    assert file_has_inbound_usage(site, "F.jpg") is True


def test_post_commonsdelinker_request_skips_when_no_usage():
    """No inbound usage → no filemovers edit (nothing to relink)."""
    site = _usage_site({"query": {"pages": {"-1": {}}}})
    with patch("ingest_wikimedia.wikimedia.pywikibot.Page") as PageCtor:
        post_commonsdelinker_request(
            site, old_filename="Old.jpg", new_filename="New.jpg"
        )
    site.editpage.assert_not_called()
    PageCtor.assert_not_called()


def test_post_commonsdelinker_request_check_usage_false_skips_query_and_posts():
    """``check_usage=False`` posts without querying usage at all. Callers
    that move-then-relink pass this after running the usage check BEFORE the
    move, where the old title is still the live file rather than a redirect."""
    site = MagicMock()
    with patch("ingest_wikimedia.wikimedia.pywikibot.Page"):
        post_commonsdelinker_request(
            site, old_filename="Old.jpg", new_filename="New.jpg", check_usage=False
        )
    # No usage query issued, and the request was posted.
    site.simple_request.assert_not_called()
    site.editpage.assert_called_once()


# ---------------------------------------------------------------------------
# get_wiki_text: the wikitext the uploader writes for new files and for the
# title-drift metadata-rescue overwrite path.
# ---------------------------------------------------------------------------


def _minimal_item_metadata():
    """Just-enough DPLA-item metadata to drive get_wiki_text without
    blowing up. Exercises the typical-record path (creator + title +
    description + date + permission + source) so the emitted template
    has every parameter we care about."""
    return {
        "rights": "http://creativecommons.org/publicdomain/zero/1.0/",
        "isShownAt": "https://example.org/item/123",
        "sourceResource": {
            "creator": ["A Creator"],
            "title": ["A Title"],
            "description": ["A description"],
            "date": [{"displayDate": "1900"}],
            "identifier": ["local-123"],
        },
    }


def test_get_wiki_text_emits_dpla_metadata_template():
    """The uploader writes a {{DPLA metadata}} block (not {{Artwork}}).

    DPLA's Commons template was designed to fully replicate the
    {{Artwork}} parameter set the uploader had been emitting, so the
    transition is a one-line template-name swap with no parameter
    changes. This test pins the name so a future refactor or revert
    can't quietly flip back to {{Artwork}}.
    """
    result = get_wiki_text(
        dpla_id="abc123",
        item_metadata=_minimal_item_metadata(),
        provider={"Wikidata": "Q1"},
        data_provider={"Wikidata": "Q2"},
    )
    # The template literal is left-justified — no leading whitespace
    # before ``{{DPLA metadata`` (the indented form leaked Python
    # source-code indentation into the rendered wikitext).
    assert "{{DPLA metadata" in result, (
        f"get_wiki_text must emit {{{{DPLA metadata}}}}; got:\n{result}"
    )
    assert "{{Artwork" not in result, (
        "get_wiki_text must NOT emit {{Artwork}} (replaced by "
        f"{{{{DPLA metadata}}}}); got:\n{result}"
    )


def test_get_wiki_text_emits_flat_param_shape():
    """Locking-in regression: the flat-param shape Module:DPLA's
    dual-path renderer expects. No nested ``{{DPLA|...}}`` /
    ``{{Institution|...}}`` / ``{{InFi|Creator|...}}`` sub-templates
    inside the template params — those caused the ``{|`` wikitable
    leakage inside Module:DPLA's HTML ``<td>``. Pin each flat row so a
    structural refactor of ``get_wiki_text`` can't silently re-introduce
    a nested form."""
    result = get_wiki_text(
        dpla_id="abc123",
        item_metadata=_minimal_item_metadata(),
        provider={"Wikidata": "Q1"},
        data_provider={"Wikidata": "Q2"},
    )
    for fragment in (
        # Creator row is conditional — emitted only when DPLA has a
        # creator string. Our fixture does, so pin the flat row.
        "| creator = A Creator",
        "| title = A Title",
        "| description = A description",
        "| date = 1900",
        "| permission = {{cc-zero}}",
        "| hub = Q1",
        "| institution = Q2",
        "| url = https://example.org/item/123",
        "| dpla_id = abc123",
        "| local_id = local-123",
    ):
        assert fragment in result, (
            f"missing expected fragment {fragment!r} in:\n{result}"
        )
    # No nested sub-templates inside the param values (the outer
    # ``{{ DPLA metadata`` is the wrapper itself, which is expected;
    # what we're guarding against is a *nested* ``{{ DPLA |...}}``
    # source sub-template, recognisable by the ``|`` immediately
    # after the name).
    assert "{{ DPLA\n" not in result and "{{DPLA\n" not in result
    assert "{{ DPLA |" not in result and "{{DPLA|" not in result
    assert "{{ Institution " not in result and "{{Institution " not in result
    assert "{{ InFi " not in result and "{{InFi " not in result
    # No leading whitespace on any param row — the rendered wikitext
    # is left-justified. Python-source indentation in the previous
    # triple-quoted template literal leaked into the output, which
    # editors viewing the page source saw as cosmetic noise.
    for line in result.splitlines():
        if line.startswith(" ") or line.startswith("\t"):
            raise AssertionError(f"line has leading whitespace: {line!r} in:\n{result}")
