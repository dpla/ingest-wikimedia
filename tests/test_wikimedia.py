from unittest.mock import patch, MagicMock
from ingest_wikimedia.wikimedia import (
    get_site,
    get_page_title,
    license_to_markup_code,
    get_permissions_template,
    get_permissions,
    escape_wiki_strings,
    join,
    extract_strings,
    extract_strings_dict,
    get_page,
    wiki_file_exists,
    wikimedia_url,
    get_wiki_text,
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


@patch("ingest_wikimedia.wikimedia.get_http_session")
def test_wiki_file_exists(mock_get_http_session):
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "query": {"allimages": [{"name": "file1"}, {"name": "file2"}]}
    }
    mock_get_http_session.return_value.get.return_value = mock_response

    exists = wiki_file_exists("fakehash")
    assert exists
    mock_get_http_session.return_value.get.assert_called_once()


@patch("ingest_wikimedia.wikimedia.pywikibot.FilePage")
def test_get_page(mock_file_page):
    mock_site = MagicMock()
    title = "Sample Title"
    page = get_page(mock_site, title)
    assert page == mock_file_page.return_value
    mock_file_page.assert_called_once_with(mock_site, title=title)


def test_wikimedia_url():
    title = "Sample Title"
    url = wikimedia_url(title)
    expected_url = "https://commons.wikimedia.org/wiki/File:Sample_Title"
    assert url == expected_url


def test_get_wiki_text():
    dpla_id = "12345"
    item_metadata = {
        "sourceResource": {
            "creator": ["John Doe"],
            "title": ["Sample Title"],
            "description": ["Sample Description"],
            "date": [{"displayDate": "2023"}],
            "identifier": ["ID12345"],
        },
        "rights": "http://rightsstatements.org/vocab/NKC/1.0/",
        "isShownAt": "http://example.com/item/12345",
    }
    provider = {"Wikidata": "Q67890"}
    data_provider = {"Wikidata": "Q12345"}

    text = get_wiki_text(dpla_id, item_metadata, provider, data_provider)
    expected_text = (
        "== {{int:filedesc}} ==\n"
        "     {{ Artwork\n"
        "        | Other fields 1 = {{ InFi | Creator | John Doe | id=fileinfotpl_aut}}\n"
        "        | title = Sample Title\n"
        "        | description = Sample Description\n"
        "        | date = 2023\n"
        "        | permission = {{NKC | Q12345}}\n"
        "        | source = {{ DPLA\n"
        "            | Q12345\n"
        "            | hub = Q67890\n"
        "            | url = http://example.com/item/12345\n"
        "            | dpla_id = 12345\n"
        "            | local_id = ID12345\n"
        "        }}\n"
        "        | Institution = {{ Institution | wikidata = Q12345 }}\n"
        "     }}"
    )
    assert text == expected_text

    item_metadata["sourceResource"].pop("creator")
    text = get_wiki_text(dpla_id, item_metadata, provider, data_provider)
    expected_text = (
        "== {{int:filedesc}} ==\n"
        "     {{ Artwork\n"
        "        | title = Sample Title\n"
        "        | description = Sample Description\n"
        "        | date = 2023\n"
        "        | permission = {{NKC | Q12345}}\n"
        "        | source = {{ DPLA\n"
        "            | Q12345\n"
        "            | hub = Q67890\n"
        "            | url = http://example.com/item/12345\n"
        "            | dpla_id = 12345\n"
        "            | local_id = ID12345\n"
        "        }}\n"
        "        | Institution = {{ Institution | wikidata = Q12345 }}\n"
        "     }}"
    )
    assert text == expected_text
