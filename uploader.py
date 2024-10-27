import csv
import logging
import mimetypes
import re
import time
from string import Template

import click
import pywikibot
import requests
from pywikibot import FilePage
from pywikibot.tools.chars import replace_invisible

from common import (
    get_item_metadata,
    extract_urls,
    get_s3_path,
    get_temp_file,
    setup_temp_dir,
    cleanup_temp_dir,
    get_s3,
    setup_logging,
    clean_up_tmp_file,
    Tracker,
    Result,
    is_wiki_eligible,
    get_provider_and_data_provider,
    get_providers_data,
    check_partner,
    provider_str,
    get_str,
    get_list,
    get_dict,
)
from constants import (
    COMMONS_SITE_NAME,
    WMC_UPLOAD_CHUNK_SIZE,
    IGNORE_WIKIMEDIA_WARNINGS,
    S3_BUCKET,
    CHECKSUM_KEY,
    INVALID_CONTENT_TYPES,
    WIKIDATA_FIELD_NAME,
    EDM_RIGHTS_FIELD_NAME,
    RESERVED_WIKITEXT_STRINGS,
    SOURCE_RESOURCE_FIELD_NAME,
    VALUE_JOIN_DELIMITER,
    DC_CREATOR_FIELD_NAME,
    DC_TITLE_FIELD_NAME,
    DC_DESCRIPTION_FIELD_NAME,
    DC_DATE_FIELD_NAME,
    EDM_TIMESPAN_PREF_LABEL,
    EDM_IS_SHOWN_AT,
    DC_IDENTIFIER_FIELD_NAME,
    CC_URL_REGEX,
    CC_BY_SA_URL_BASE,
    CC_BY_URL_BASE,
    CC_ZERO_URL_BASE,
    CC_PD_URL_BASE,
    RS_NOC_URL_BASE,
    RS_NKC_URL_BASE,
    RS_NKC_TEMPLATE,
    NOC_US_TEMPLATE,
    PD_US_TEMPLATE,
    CC_ZERO_TEMPLATE,
    RIGHTS_STATEMENTS_URL_BASE,
    COMMONS_URL_PREFIX,
    FIND_BY_HASH_URL_PREFIX,
    FIND_BY_HASH_QUERY_FIELD_NAME,
    FIND_BY_HASH_ALLIMAGES_FIELD_NAME,
    ERROR_FILEEXISTS,
    ERROR_MIME,
    ERROR_BANNED,
    ERROR_DUPLICATE,
    ERROR_NOCHANGE,
)


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
    response = requests.get(FIND_BY_HASH_URL_PREFIX + sha1)
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

        csv_reader = csv.reader(ids_file)
        for row in csv_reader:
            dpla_id = row[0]

            logging.info(f"DPLA ID: {dpla_id}")

            item_metadata = get_item_metadata(dpla_id, api_key)

            provider, data_provider = get_provider_and_data_provider(
                item_metadata, providers_json
            )

            if not is_wiki_eligible(item_metadata, provider, data_provider):
                tracker.increment(Result.SKIPPED)
                continue

            titles = get_list(
                get_dict(item_metadata, SOURCE_RESOURCE_FIELD_NAME), DC_TITLE_FIELD_NAME
            )

            # playing it safe in case titles is empty
            title = titles[0] if titles else ""

            ordinal = 0
            # todo should we walk s3 instead of trusting file list
            files = extract_urls(item_metadata)

            for file in files:
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
                    # todo what if file doesn't exist
                    s3_object = s3.Object(S3_BUCKET, s3_path)
                    file_size = s3_object.content_length
                    sha1 = s3_object.metadata.get(CHECKSUM_KEY, "")

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
                        logging.info(f"Data Provider: {provider_str(data_provider)}")
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
                        s3_object.download_file(
                            temp_file.name,
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

    finally:
        logging.info("\n" + str(tracker))
        logging.info(f"{time.time() - start_time} seconds.")
        cleanup_temp_dir()


def handle_upload_exception(ex) -> None:
    error_string = str(ex)
    message = ""
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
        logging.error(f"Failed: {message}", exc_info=True, stack_info=True)
    else:
        logging.warning(f"Failed: {message}", exc_info=True, stack_info=True)


if __name__ == "__main__":
    main()
