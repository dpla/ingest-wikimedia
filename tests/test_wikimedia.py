from unittest.mock import patch, MagicMock
from ingest_wikimedia.wikimedia import (
    COMMONSDELINKER_PAGE,
    MAX_COMMENT_BYTES,
    build_title_drift_move_reason,
    escape_template_param,
    get_site,
    get_page_title,
    get_wiki_text,
    license_to_markup_code,
    get_permissions_template,
    get_permissions,
    escape_wiki_strings,
    join,
    collect_duplicate_source_sha1s,
    compute_ordinal_exts_and_page_labels,
    extract_page_ordinal_from_commons_title,
    extract_strings,
    extract_strings_dict,
    file_has_inbound_usage,
    is_same_item_redirect_relic,
    merge_preserved_wikitext,
    post_commonsdelinker_request,
    tag_as_duplicate,
    wiki_file_exists,
    check_content_type,
)


@patch("ingest_wikimedia.wikimedia.pywikibot.Site")
def test_get_site(mock_site):
    mock_site_instance = MagicMock()
    mock_site.return_value = mock_site_instance

    site = get_site()
    assert site == mock_site_instance
    mock_site.assert_called_once_with("commons")
    mock_site_instance.login.assert_called_once()


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
    assert "filter-value+other-thing" in title


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
# post_commonsdelinker_request / tag_as_duplicate — atomic-append behaviour
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


def test_tag_as_duplicate_uses_prependtext_not_read_modify_write():
    """tag_as_duplicate must use site.editpage(prependtext=...) rather
    than `file_page.text = tag + file_page.text; file_page.save()`.

    The page-text read is now required for the idempotency check (see
    test_tag_as_duplicate_is_idempotent_when_already_tagged), but the
    write side must still go through prependtext — MediaWiki concatenates
    atomically on the primary, no basetimestamp, no read-modify-write
    save() that would re-introduce edit-conflict risk.
    """
    site = MagicMock()
    file_page = MagicMock()
    # No prior tag — idempotency check sees empty text and proceeds to write.
    type(file_page).text = property(lambda self: "")
    file_page.title.return_value = "Stranded.jpg"

    tag_as_duplicate(
        site,
        file_page,
        correct_filename="Correct.jpg",
        reason="Other file has the correct title.",
    )

    assert site.editpage.call_count == 1
    _, kwargs = site.editpage.call_args
    assert "prependtext" in kwargs, (
        f"editpage call must use prependtext; got kwargs={kwargs!r}"
    )
    # Tag goes first, followed by a newline so the existing page wikitext
    # starts on its own line.
    assert kwargs["prependtext"].startswith("{{Duplicate|Correct.jpg|"), (
        f"prependtext must begin with the Duplicate template; got "
        f"{kwargs['prependtext']!r}"
    )
    assert kwargs["prependtext"].endswith("\n"), (
        "prependtext must end with a newline so the original wikitext is "
        f"line-separated; got {kwargs['prependtext']!r}"
    )
    file_page.save.assert_not_called()


def test_tag_as_duplicate_is_idempotent_when_already_tagged():
    """Regression: a page that already carries a {{Duplicate}} template
    must NOT receive a second one. Two uploader code paths (per-asset
    hash-drift correction during upload + the post-item trailing-orphan
    sweep) can both identify the same file as a duplicate of the same
    target. Unconditionally prepending each time stacks redundant
    `{{Duplicate|Correct.jpg|...}}` templates on the page — seen in
    production on a NARA file that got tagged twice within three
    seconds with the same correct title and two different reasons.
    """
    site = MagicMock()
    file_page = MagicMock()
    # Page already tagged by an earlier upstream caller in this run.
    type(file_page).text = property(
        lambda self: (
            "{{Duplicate|Correct.jpg|Other file has the correct title.}}\n"
            "{{Information|description=...}}\n"
        )
    )
    file_page.title.return_value = "Stranded.jpg"

    tag_as_duplicate(
        site,
        file_page,
        correct_filename="Correct.jpg",
        reason="Trailing-page orphan: this title has no corresponding asset.",
    )

    site.editpage.assert_not_called()
    file_page.save.assert_not_called()


def test_tag_as_duplicate_idempotency_detects_lowercase_template():
    """The duplicate-detection regex must also catch `{{duplicate}}`
    (lowercase variant some Commons editors use) — otherwise our bot
    would add `{{Duplicate}}` on top of an existing `{{duplicate}}` and
    re-introduce the double-tag we just fixed."""
    site = MagicMock()
    file_page = MagicMock()
    type(file_page).text = property(lambda self: "{{duplicate|Correct.jpg|reason}}")
    file_page.title.return_value = "Stranded.jpg"

    tag_as_duplicate(site, file_page, "Correct.jpg", "reason")
    site.editpage.assert_not_called()


def test_tag_as_duplicate_idempotency_detects_whitespace_variants():
    """The regex must also catch the spaced/newline variants of the
    template invocation that wikitext allows: `{{ Duplicate|...}}`,
    `{{Duplicate |...}}`, and `{{Duplicate\\n|...}}` are all valid
    forms Commons editors emit. Earlier the regex required `|` or `}`
    to follow `Duplicate` with no whitespace, which would have missed
    every one of these and re-introduced the double-tag bug whenever
    the prior tagger used a less-compact form.
    """
    for prior_tag in (
        "{{ Duplicate|Correct.jpg|reason}}",  # space after `{{`
        "{{Duplicate |Correct.jpg|reason}}",  # space before `|`
        "{{Duplicate\n|Correct.jpg|reason}}",  # newline before `|`
        "{{Duplicate}}",  # no args (closes immediately)
        "{{Duplicate }}",  # no args with trailing whitespace
    ):
        site = MagicMock()
        file_page = MagicMock()
        type(file_page).text = property(lambda self, t=prior_tag: t)
        file_page.title.return_value = "Stranded.jpg"
        tag_as_duplicate(site, file_page, "Correct.jpg", "reason")
        assert site.editpage.call_count == 0, (
            f"prior tag {prior_tag!r} should have been detected; "
            f"editpage was called {site.editpage.call_count} time(s)"
        )


def test_escape_template_param_replaces_equals_with_magic_word():
    """`=` inside a template positional parameter is interpreted by
    the MediaWiki parser as a named-parameter separator. The escape
    must replace every `=` with the ``{{=}}`` magic word so the
    rendered text is identical but the parser sees no splitter."""
    assert escape_template_param("no equals") == "no equals"
    assert escape_template_param("a=b") == "a{{=}}b"
    assert escape_template_param("multi=one=two") == "multi{{=}}one{{=}}two"
    assert escape_template_param("") == ""


def test_tag_as_duplicate_escapes_equals_in_filename():
    """Regression: a Spot Pond Reservoir file with a DPLA-preserved
    source title containing ``"height of camera objective = 6.5
    feet"`` produced a `{{Duplicate|...=...|...}}` template that
    MediaWiki parsed as a named parameter, breaking the link and
    showing an empty positional-1 slot. The bug was visible on
    Commons rev 1234551826.

    The fix: escape `=` in the correct-filename parameter via the
    standard ``{{=}}`` magic word."""
    site = MagicMock()
    file_page = MagicMock()
    type(file_page).text = property(lambda self: "")
    file_page.title.return_value = "Stranded.jpg"

    nasty_title = (
        'Distribution Department, "height of camera objective = 6.5 feet", '
        "Ston - DPLA - 01e81012a2a12fa66704075773b08b0c.jpg"
    )
    tag_as_duplicate(
        site,
        file_page,
        correct_filename=nasty_title,
        reason="Other file has the correct title.",
    )

    assert site.editpage.call_count == 1
    _, kwargs = site.editpage.call_args
    rendered = kwargs["prependtext"]
    # A bare `=` between the first `|` and the second `|` would be the
    # bug — assert there's no raw `=` between the template-open and the
    # reason parameter separator.
    duplicate_open = rendered.index("{{Duplicate|")
    first_pipe = duplicate_open + len("{{Duplicate|")
    # Find the SECOND pipe (parameter separator between filename and reason).
    second_pipe = rendered.index("|", first_pipe + 1)
    filename_param = rendered[first_pipe:second_pipe]
    # The filename param must NOT contain a raw `=` — only the {{=}} form.
    assert "=" not in filename_param.replace("{{=}}", ""), (
        f"raw `=` survived inside template-parameter position: {filename_param!r}"
    )
    assert "{{=}}" in filename_param, (
        f"escape did not fire on title with `=`: {filename_param!r}"
    )


def test_tag_as_duplicate_escapes_equals_in_reason():
    """The reason parameter is the second positional param of
    ``{{Duplicate}}`` — same template-parser hazard. Today the
    default reason has no `=`, but any future change that introduces
    one (e.g. embedding a URL fragment) must not break the template."""
    site = MagicMock()
    file_page = MagicMock()
    type(file_page).text = property(lambda self: "")
    file_page.title.return_value = "Stranded.jpg"

    tag_as_duplicate(
        site,
        file_page,
        correct_filename="Correct.jpg",
        reason="see https://example.org/?id=42 for context",
    )

    rendered = site.editpage.call_args.kwargs["prependtext"]
    assert "?id{{=}}42" in rendered
    assert "?id=42" not in rendered.replace("?id{{=}}42", "")


def test_post_commonsdelinker_request_escapes_equals_in_filenames():
    """Same `=` escape contract applies to the
    ``{{universal replace|<old>|<new>|reason=...}}`` template that
    ``post_commonsdelinker_request`` posts to the CommonsDelinker
    page. Filenames with `=` would otherwise corrupt the positional
    params and the delinker would silently no-op."""
    site = MagicMock()
    # ``pywikibot.Page(site, ...)`` rejects a MagicMock site, so stub
    # the page constructor too. ``file_has_inbound_usage`` returns True
    # so the post path is reached.
    with (
        patch("ingest_wikimedia.wikimedia.pywikibot.Page", return_value=MagicMock()),
        patch("ingest_wikimedia.wikimedia.file_has_inbound_usage", return_value=True),
    ):
        post_commonsdelinker_request(
            site,
            old_filename="Foo = bar.jpg",
            new_filename="Foo = bar - DPLA - abc.jpg",
            check_usage=True,
        )

    assert site.editpage.call_count == 1
    appended = site.editpage.call_args.kwargs.get("appendtext", "")
    # Both positional params must use the {{=}} escape.
    assert "|Foo {{=}} bar.jpg|" in appended
    assert "|Foo {{=}} bar - DPLA - abc.jpg|" in appended
    # And no raw `=` in the positional-param region (the `reason=` named
    # param is fine; that's a legitimate named-param assignment).
    template_body = appended[
        appended.index("{{universal replace") : appended.index("|reason=")
    ]
    assert "=" not in template_body.replace("{{=}}", ""), (
        f"raw `=` survived in positional region: {template_body!r}"
    )


def test_tag_as_duplicate_idempotency_does_not_match_unrelated_templates():
    """The regex must NOT false-positive on unrelated templates whose
    name happens to start with 'Duplicate' (e.g. {{DuplicateImageFinder}}).
    A page with only such a template should still receive its first
    `{{Duplicate}}` tag from us."""
    site = MagicMock()
    file_page = MagicMock()
    type(file_page).text = property(
        lambda self: "{{DuplicateImageFinder|param=value}}\n"
    )
    file_page.title.return_value = "Stranded.jpg"

    tag_as_duplicate(site, file_page, "Correct.jpg", "reason")
    # First tag still applied — the unrelated template should not count.
    assert site.editpage.call_count == 1


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
