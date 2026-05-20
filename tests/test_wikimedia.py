from unittest.mock import patch, MagicMock
from ingest_wikimedia.wikimedia import (
    get_site,
    get_page_title,
    license_to_markup_code,
    get_permissions_template,
    get_permissions,
    escape_wiki_strings,
    join,
    extract_page_ordinal_from_commons_title,
    extract_strings,
    extract_strings_dict,
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
