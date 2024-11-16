import logging
import mimetypes
import re
import time
from string import Template

import click
import pywikibot

from tqdm import tqdm
from pywikibot import FilePage
from pywikibot.tools.chars import replace_invisible

from common import (
    get_str,
    get_list,
    get_dict,
    load_ids,
)
from logs import setup_logging
from s3 import get_s3_path, get_s3, S3_BUCKET, S3_KEY_CHECKSUM, s3_file_exists
from tracker import Result, Tracker
from temp import setup_temp_dir, cleanup_temp_dir, get_temp_file, clean_up_tmp_file
from dpla import (
    check_partner,
    get_item_metadata,
    is_wiki_eligible,
    get_provider_and_data_provider,
    get_providers_data,
    provider_str,
    SOURCE_RESOURCE_FIELD_NAME,
    EDM_IS_SHOWN_AT,
    EDM_RIGHTS_FIELD_NAME,
    EDM_TIMESPAN_PREF_LABEL,
    DC_CREATOR_FIELD_NAME,
    DC_DATE_FIELD_NAME,
    DC_DESCRIPTION_FIELD_NAME,
    DC_TITLE_FIELD_NAME,
    DC_IDENTIFIER_FIELD_NAME,
    WIKIDATA_FIELD_NAME,
    extract_urls,
)
from web import get_http_session
from wikimedia import (
    INVALID_CONTENT_TYPES,
    COMMONS_URL_PREFIX,
    ERROR_FILEEXISTS,
    ERROR_MIME,
    ERROR_BANNED,
    ERROR_DUPLICATE,
    ERROR_NOCHANGE,
    COMMONS_SITE_NAME,
    WMC_UPLOAD_CHUNK_SIZE,
    VALUE_JOIN_DELIMITER,
    RESERVED_WIKITEXT_STRINGS,
    IGNORE_WIKIMEDIA_WARNINGS,
    FIND_BY_HASH_URL_PREFIX,
    FIND_BY_HASH_QUERY_FIELD_NAME,
    FIND_BY_HASH_ALLIMAGES_FIELD_NAME,
)

CC_URL_REGEX = "^http://creativecommons.org/licenses/(.*)"

RIGHTS_STATEMENTS_URL_BASE = "http://rightsstatements.org"
RS_NKC_URL_BASE = RIGHTS_STATEMENTS_URL_BASE + "/vocab/NKC/"
RS_NOC_URL_BASE = RIGHTS_STATEMENTS_URL_BASE + "/vocab/NoC-US/"
CC_URL_BASE = "http://creativecommons.org"
CC_PD_URL_BASE = CC_URL_BASE + "/publicdomain/mark/"
CC_ZERO_URL_BASE = CC_URL_BASE + "/publicdomain/zero/"
CC_BY_URL_BASE = CC_URL_BASE + "/licenses/by/"
CC_BY_SA_URL_BASE = CC_URL_BASE + "/licenses/by-sa/"

CC_ZERO_TEMPLATE = "cc-zero"
RS_NKC_TEMPLATE = "NKC"
NOC_US_TEMPLATE = "NoC-US"
PD_US_TEMPLATE = "PD-US"


def get_page(site: pywikibot.Site, title: str) -> FilePage:
    """
    Get the pywikibot object representing the page on Commons.
    """
    try:
        return pywikibot.FilePage(site, title=title)
    except pywikibot.exceptions.InvalidTitleError as e:
        raise Exception(f"Invalid title {title}: {str(e)}") from e
    except Exception as e:
        raise Exception(f"Unable to create page {title}: {str(e)}") from e


def get_page_title(
    item_title: str, dpla_identifier: str, suffix: str, page=None
) -> str:
    """
    Makes a proper Wikimedia page title from the DPLA identifier and
    the title of the image.
    """
    escaped_title = (
        item_title[:181]
        .replace("[", "(")
        .replace("]", ")")
        .replace("{", "(")
        .replace("}", ")")
        .replace("/", "-")
        .replace(":", "-")
        .replace("#", "-")
    )

    escaped_visible_title = replace_invisible(escaped_title)

    # Add pagination to page title if needed
    if page:
        return (
            f"{escaped_visible_title} - DPLA - {dpla_identifier} (page {page}){suffix}"
        )
    return f"{escaped_visible_title} - DPLA - {dpla_identifier}{suffix}"


def license_to_markup_code(rights_uri: str) -> str:
    match_result = re.match(CC_URL_REGEX, rights_uri)
    if not match_result:
        return ""
    else:
        port = match_result.group(1).replace("/", "-")[:-1]
        return f"Cc-{port}"


def get_permissions_template(rights_uri: str) -> str:
    """Looks up the right wikitext template call for the rights_uri."""
    if rights_uri.startswith(RS_NKC_URL_BASE):
        return RS_NKC_TEMPLATE
    if rights_uri.startswith(RS_NOC_URL_BASE):
        return NOC_US_TEMPLATE
    if rights_uri.startswith(CC_PD_URL_BASE):
        return PD_US_TEMPLATE
    if rights_uri.startswith(CC_ZERO_URL_BASE):
        return CC_ZERO_TEMPLATE
    if rights_uri.startswith(CC_BY_URL_BASE):
        return license_to_markup_code(rights_uri)
    if rights_uri.startswith(CC_BY_SA_URL_BASE):
        return license_to_markup_code(rights_uri)
    return ""


def get_permissions(
    rights_uri: str, permissions_template_name: str, data_provider_wiki_q: str
) -> str:
    """Builds the wikitext for the commons item permissions."""
    if rights_uri.startswith(RIGHTS_STATEMENTS_URL_BASE):
        return f"{permissions_template_name} | {data_provider_wiki_q}"
    else:
        return permissions_template_name


def escape_wiki_strings(unescaped_string: str) -> str:
    """Removes specific character sequences from string to be safe for wikitext."""
    for reserved_string in RESERVED_WIKITEXT_STRINGS:
        unescaped_string = unescaped_string.replace(reserved_string, "")
    return unescaped_string


def join(strs: list[str]) -> str:
    """Convenience method for joining lists of strings."""
    return VALUE_JOIN_DELIMITER.join(strs)


def extract_strings(data: dict, field_name: str) -> str:
    """Convenience method for building a string
    out of escaped strings from a dict field"""
    return join([escape_wiki_strings(value) for value in get_list(data, field_name)])


def extract_strings_dict(data: dict, field_name1: str, field_name2: str) -> str:
    """Convenience method for building a string
    out of escaped strings from a dict field from an inner dict"""
    return join(
        [
            escape_wiki_strings(get_str(value, field_name2))
            for value in get_list(data, field_name1)
        ]
    )


def get_wiki_text(
    dpla_id: str, item_metadata: dict, provider: dict, data_provider: dict
) -> str:
    """Turns DPLA item info into a wikitext document."""
    data_provider_wiki_q = escape_wiki_strings(
        get_str(data_provider, WIKIDATA_FIELD_NAME)
    )
    provider_wiki_q = escape_wiki_strings(get_str(provider, WIKIDATA_FIELD_NAME))
    rights_uri = get_str(item_metadata, EDM_RIGHTS_FIELD_NAME)
    permissions = get_permissions(
        rights_uri, get_permissions_template(rights_uri), data_provider_wiki_q
    )
    source_resource = get_dict(item_metadata, SOURCE_RESOURCE_FIELD_NAME)
    creator_string = extract_strings(source_resource, DC_CREATOR_FIELD_NAME)
    title_string = extract_strings(source_resource, DC_TITLE_FIELD_NAME)
    description_string = extract_strings(source_resource, DC_DESCRIPTION_FIELD_NAME)
    date_string = extract_strings_dict(
        source_resource, DC_DATE_FIELD_NAME, EDM_TIMESPAN_PREF_LABEL
    )
    is_shown_at = escape_wiki_strings(get_str(item_metadata, EDM_IS_SHOWN_AT))
    local_id = extract_strings(source_resource, DC_IDENTIFIER_FIELD_NAME)

    template_string = """== {{int:filedesc}} ==
     {{ Artwork
        | Other fields 1 = {{ InFi | Creator | $creator }}
        | title = $title
        | description = $description
        | date = $date_string
        | permission = {{$permissions}}
        | source = {{ DPLA
            | $data_provider
            | hub = $provider
            | url = $is_shown_at
            | dpla_id = $dpla_id
            | local_id = $local_id
        }}
        | Institution = {{ Institution | wikidata = $data_provider }}
     }}"""

    return Template(template_string).substitute(
        creator=creator_string,
        title=title_string,
        description=description_string,
        date_string=date_string,
        permissions=permissions,
        data_provider=data_provider_wiki_q,
        provider=provider_wiki_q,
        is_shown_at=is_shown_at,
        dpla_id=dpla_id,
        local_id=local_id,
    )


def wikimedia_url(title: str) -> str:
    """Return the URL for the Wikimedia page"""
    return f"{COMMONS_URL_PREFIX}{title.replace(' ', '_')}"


def get_site() -> pywikibot.Site:
    """Returns the Site object for wikimedia commons."""
    site = pywikibot.Site(COMMONS_SITE_NAME)
    site.login()
    logging.info(f"Logged: {site.user()} in {site.family}")
    return site


def wiki_file_exists(sha1: str) -> bool:
    """Calls the find by hash api on commons to see if the file already exists."""
    response = get_http_session().get(FIND_BY_HASH_URL_PREFIX + sha1)
    sha1_response = response.json()
    if "error" in sha1_response:
        raise Exception(
            f"Received bad response from find by hash endpoint.\n{str(sha1_response)}"
        )

    all_images = get_list(
        get_dict(sha1_response, FIND_BY_HASH_QUERY_FIELD_NAME),
        FIND_BY_HASH_ALLIMAGES_FIELD_NAME,
    )
    return len(all_images) > 0


@click.command()
@click.argument("ids_file", type=click.File("r"))
@click.argument("partner")
@click.argument("api_key")
@click.option("--dry-run", is_flag=True)
@click.option("--verbose", is_flag=True)
def main(ids_file, partner: str, api_key: str, dry_run: bool, verbose: bool) -> None:
    start_time = time.time()
    tracker = Tracker()

    check_partner(partner)

    try:
        setup_temp_dir()
        setup_logging(partner, "upload", logging.INFO)
        if dry_run:
            logging.warning("---=== DRY RUN ===---")
        s3 = get_s3()
        site = get_site()
        providers_json = get_providers_data()
        logging.info(f"Starting upload for {partner}")

        dpla_ids = load_ids(ids_file)

        for dpla_id in tqdm(dpla_ids, desc="Uploading Items", unit="Item"):
            try:
                logging.info(f"DPLA ID: {dpla_id}")

                item_metadata = get_item_metadata(dpla_id, api_key)

                provider, data_provider = get_provider_and_data_provider(
                    item_metadata, providers_json
                )

                if not is_wiki_eligible(item_metadata, provider, data_provider):
                    tracker.increment(Result.SKIPPED)
                    continue

                titles = get_list(
                    get_dict(item_metadata, SOURCE_RESOURCE_FIELD_NAME),
                    DC_TITLE_FIELD_NAME,
                )

                # playing it safe in case titles is empty
                title = titles[0] if titles else ""

                ordinal = 0
                # todo should we walk s3 instead of trusting file list
                # todo manifest of files?
                files = extract_urls(item_metadata)

                for file in tqdm(
                    files, desc="Uploading Files", leave=False, unit="File"
                ):
                    ordinal += 1  # todo if we're walking s3, this comes from the name
                    logging.info(f"Page {ordinal}")
                    # one-pagers don't have page numbers in their titles
                    page_label = None if len(files) == 1 else ordinal
                    temp_file = get_temp_file()
                    try:
                        wiki_markup = get_wiki_text(
                            dpla_id, item_metadata, provider, data_provider
                        )
                        s3_path = get_s3_path(dpla_id, ordinal, partner)
                        upload_comment = (
                            f'Uploading DPLA ID "[[dpla:{dpla_id}|{dpla_id}]]".'
                        )
                        if not s3_file_exists(s3_path, s3):
                            logging.info(f"{dpla_id} {ordinal} not present.")
                            tracker.increment(Result.SKIPPED)
                            continue
                        s3_object = s3.Object(S3_BUCKET, s3_path)
                        file_size = s3_object.content_length

                        sha1 = s3_object.metadata.get(S3_KEY_CHECKSUM, "")

                        mime = s3_object.content_type
                        if mime in INVALID_CONTENT_TYPES:
                            logging.info(
                                f"Skipping {dpla_id} {ordinal}: Bad content type: {mime}"
                            )
                            tracker.increment(Result.SKIPPED)
                            continue

                        ext = mimetypes.guess_extension(mime)

                        if not ext:
                            logging.info(
                                f"Skipping {dpla_id} {ordinal}: "
                                f"Unable to guess extension for {mime}"
                            )
                            tracker.increment(Result.SKIPPED)
                            continue

                        page_title = get_page_title(
                            item_title=title,
                            dpla_identifier=dpla_id,
                            suffix=ext,
                            page=page_label,
                        )

                        if verbose:
                            logging.info(f"DPLA ID: {dpla_id}")
                            logging.info(f"Title: {title}")
                            logging.info(f"Page title: {page_title}")
                            logging.info(f"Provider: {provider_str(provider)}")
                            logging.info(
                                f"Data Provider: {provider_str(data_provider)}"
                            )
                            logging.info(f"MIME: {mime}")
                            logging.info(f"Extension: {ext}")
                            logging.info(f"File size: {file_size}")
                            logging.info(f"SHA-1: {sha1}")
                            logging.info(f"Upload comment: {upload_comment}")
                            logging.info(f"Wikitext: \n {wiki_markup}")

                        if wiki_file_exists(sha1):
                            logging.info(
                                f"Skipping {dpla_id} {ordinal}: Already exists on commons."
                            )
                            tracker.increment(Result.SKIPPED)
                            continue

                        if not dry_run:
                            with tqdm(
                                total=s3_object.content_length,
                                leave=False,
                                desc="S3 Download",
                                unit="B",
                                unit_scale=1024,
                                unit_divisor=True,
                                delay=2,
                            ) as t:
                                s3_object.download_file(
                                    temp_file.name,
                                    Callback=lambda bytes_xfer: t.update(bytes_xfer),
                                )

                            wiki_file_page = get_page(site, page_title)

                            result = site.upload(
                                filepage=wiki_file_page,
                                source_filename=temp_file.name,
                                comment=upload_comment,
                                text=wiki_markup,
                                ignore_warnings=IGNORE_WIKIMEDIA_WARNINGS,
                                asynchronous=True,
                                chunk_size=WMC_UPLOAD_CHUNK_SIZE,
                            )

                            if not result:
                                # These error message accounts for Page does not exist,
                                # but File does exist and is linked to another Page
                                # (ex. DPLA ID drift)
                                tracker.increment(Result.FAILED)
                                raise Exception(
                                    "File linked to another page (possible ID drift)"
                                )

                            logging.info(f"Uploaded to {wikimedia_url(page_title)}")
                            tracker.increment(Result.UPLOADED)
                            tracker.increment(Result.BYTES, file_size)

                    except Exception as ex:
                        handle_upload_exception(ex)
                    finally:
                        clean_up_tmp_file(temp_file)
            except Exception as ex:
                logging.warning(
                    f"Caught exception getting item info for {dpla_id}", exc_info=ex
                )
                tracker.increment(Result.FAILED)

    finally:
        logging.info("\n" + str(tracker))
        logging.info(f"{time.time() - start_time} seconds.")
        cleanup_temp_dir()


def handle_upload_exception(ex) -> None:
    error_string = str(ex)
    message = "Unknown"
    error = False

    if ERROR_FILEEXISTS in error_string:
        # A file with this name exists at the Wikimedia Commons.
        message = "File already uploaded"
        error = True
    elif ERROR_MIME in error_string:
        message = "Invalid MIME type"
        error = True
    elif ERROR_BANNED in error_string:
        message = "Banned file type"
        error = True
    elif ERROR_DUPLICATE in error_string:
        # The file is a duplicate of a deleted file or
        # The upload is an exact duplicate of older version(s) of this file
        message = f"File already exists, {error_string}"
    elif ERROR_NOCHANGE in error_string:
        message = f"File exists, no change, {error_string}"

    if error:
        logging.error(f"Failed: {message}", exc_info=ex)
    else:
        logging.warning(f"Failed: {message}", exc_info=ex)


if __name__ == "__main__":
    main()
