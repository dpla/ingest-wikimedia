from unittest.mock import patch, MagicMock
from ingest_wikimedia.wikimedia import (
    get_site,
    get_page_title,
    license_to_markup_code,
    get_permissions_template,
    get_permissions,
    escape_wiki_strings,
    join,
    compute_ordinal_exts_and_page_labels,
    extract_page_ordinal_from_commons_title,
    extract_strings,
    extract_strings_dict,
    is_same_item_redirect_relic,
    merge_preserved_wikitext,
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


ARTWORK = "== {{int:filedesc}} ==\n{{Artwork|title=Example}}"


def test_merge_preserved_wikitext_empty_existing():
    result = merge_preserved_wikitext("", ARTWORK)
    assert result == ARTWORK + "\n"


def test_merge_preserved_wikitext_preserves_pd_usgov():
    existing = "== {{int:license-header}} ==\n{{PD-USGov}}\n"
    result = merge_preserved_wikitext(existing, ARTWORK)
    assert result == ARTWORK + "\n\n{{PD-USGov}}\n"


def test_merge_preserved_wikitext_preserves_pd_usgov_variants():
    existing = "{{PD-USGov-Military-Army}} and {{PD-USGov-NARA}}"
    result = merge_preserved_wikitext(existing, ARTWORK)
    assert "{{PD-USGov-Military-Army}}" in result
    assert "{{PD-USGov-NARA}}" in result


def test_merge_preserved_wikitext_preserves_pd_usgov_with_params():
    existing = "{{PD-USGov|date=1986}}"
    result = merge_preserved_wikitext(existing, ARTWORK)
    assert "{{PD-USGov|date=1986}}" in result


def test_merge_preserved_wikitext_preserves_image_extracted():
    existing = "|other versions={{Image extracted|1=Parent Image.jpg}}"
    result = merge_preserved_wikitext(existing, ARTWORK)
    assert "{{Image extracted|1=Parent Image.jpg}}" in result


def test_merge_preserved_wikitext_preserves_categories():
    existing = (
        "[[Category:Ronald Reagan in 1986]]\n"
        "[[Category:Nancy and Ronald Reagan]]\n"
        "[[Category:Files uploaded by RandomUserGuy1738]]\n"
    )
    result = merge_preserved_wikitext(existing, ARTWORK)
    assert "[[Category:Ronald Reagan in 1986]]" in result
    assert "[[Category:Nancy and Ronald Reagan]]" in result
    assert "[[Category:Files uploaded by RandomUserGuy1738]]" in result


def test_merge_preserved_wikitext_preserves_category_with_sort_key():
    existing = "[[Category:Foo|Bar]]"
    result = merge_preserved_wikitext(existing, ARTWORK)
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
    result = merge_preserved_wikitext(existing, ARTWORK)

    # Artwork comes first
    assert result.startswith(ARTWORK)
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
    result = merge_preserved_wikitext(existing, ARTWORK)
    assert result.count("[[Category:Foo]]") == 1
    assert result.count("{{PD-USGov}}") == 1
    assert result.count("{{Image extracted|1=A.jpg}}") == 1


def test_merge_preserved_wikitext_no_metadata_returns_artwork_unchanged():
    existing = (
        "Some unrelated wikitext with no license, image-extracted, or categories."
    )
    result = merge_preserved_wikitext(existing, ARTWORK)
    assert result == ARTWORK + "\n"


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
