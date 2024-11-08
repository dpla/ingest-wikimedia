DPLA_PARTNERS = [
    "bpl",
    "georgia",
    "il",
    "indiana",
    "nara",
    "northwest-heritage",
    "ohio",
    "p2p",
    "pa",
    "texas",
    "minnesota",
]

LOGS_DIR_BASE = "./logs"

# For temporarily storing local downloads.
TMP_DIR_BASE = "./tmp"

# Wikimedia constants
WIKIDATA_URL_BASE = "http://www.wikidata.org/entity/"
COMMONS_URL_PREFIX = "https://commons.wikimedia.org/wiki/File:"
ERROR_FILEEXISTS = "fileexists-shared-forbidden"
ERROR_MIME = "filetype-badmime"
ERROR_BANNED = "filetype-banned"
ERROR_DUPLICATE = "duplicate"
ERROR_NOCHANGE = "no-change"
COMMONS_SITE_NAME = "commons"
WMC_UPLOAD_CHUNK_SIZE = 20_000_000  # 20 MB
VALUE_JOIN_DELIMITER = "; "
RESERVED_WIKITEXT_STRINGS = ["|", "=", "[[", "]]", "{{", "}}", "''"]


# This list exists mainly to exclude 'duplicate' records/images from being uploaded
# Full list of warnings:
# https://doc.wikimedia.org/pywikibot/master/_modules/pywikibot/site/_upload.html
IGNORE_WIKIMEDIA_WARNINGS = [
    # Target filename has a bad prefix {msg}.
    "bad-prefix",
    # Target filename is invalid.
    "badfilename",
    # The file is a duplicate of a deleted file {msg}.
    "duplicate-archive",
    # The upload is an exact duplicate of older version(s) of this file
    "duplicate-version",
    # File {msg} is empty.
    "empty-file",
    # File [Page] {msg} already exists
    "exists",
    # File exists with different extension as {msg}.
    "exists-normalized",
    # File {msg} type is unwanted type.
    "filetype-unwanted-type",
    # Target filename exists but with a different file {msg}
    "page-exists",
    # The file {msg} was previously deleted.
    "was-deleted",
    # Not ignored:
    # Uploaded file is a duplicate of {msg}
    # 'duplicate',
    # The upload is an exact duplicate of the current version  of this file
    # 'no-change',
]
INVALID_CONTENT_TYPES = [
    "text/html",
    "application/json",
    "application/xml",
    "text/plain",
]

# API documentation: https://www.mediawiki.org/wiki/API:Allimages
FIND_BY_HASH_URL_PREFIX: str = (
    "https://commons.wikimedia.org/w/api.php?action=query&format=json"
    "&list=allimages&aisha1="
)

FIND_BY_HASH_QUERY_FIELD_NAME = "query"
FIND_BY_HASH_ALLIMAGES_FIELD_NAME = "allimages"

# API documentation: https://www.mediawiki.org/wiki/API:Imageinfo
FIND_BY_TITLE_URL_PREFIX: str = (
    "https://commons.wikimedia.org/w/api.php?action=query&format=json&prop=imageinfo"
    "&iiprop=sha1&titles="
)

# rights statements
RIGHTS_STATEMENTS_URL_BASE = "http://rightsstatements.org"
CC_URL_BASE = "http://creativecommons.org"
CC_URL_REGEX = "^http://creativecommons.org/licenses/(.*)"
RS_NKC_URL_BASE = RIGHTS_STATEMENTS_URL_BASE + "/vocab/NKC/"
RS_NKC_TEMPLATE = "NKC"
RS_NOC_URL_BASE = RIGHTS_STATEMENTS_URL_BASE + "/vocab/NoC-US/"
NOC_US_TEMPLATE = "NoC-US"
CC_PD_URL_BASE = CC_URL_BASE + "/publicdomain/mark/"
PD_US_TEMPLATE = "PD-US"
CC_ZERO_URL_BASE = CC_URL_BASE + "/publicdomain/zero/"
CC_ZERO_TEMPLATE = "cc-zero"
CC_BY_URL_BASE = CC_URL_BASE + "/licenses/by/"
CC_BY_SA_URL_BASE = CC_URL_BASE + "/licenses/by-sa/"


# DPLA API
DPLA_API_URL_BASE = "https://api.dp.la/v2/items/"
DPLA_API_DOCS = "docs"

# DPLA MAP field names
SOURCE_RESOURCE_FIELD_NAME = "sourceResource"
MEDIA_MASTER_FIELD_NAME = "mediaMaster"
IIIF_MANIFEST_FIELD_NAME = "iiifManifest"
PROVIDER_FIELD_NAME = "provider"
DATA_PROVIDER_FIELD_NAME = "dataProvider"
EXACT_MATCH_FIELD_NAME = "exactMatch"
EDM_AGENT_NAME = "name"
EDM_IS_SHOWN_AT = "isShownAt"
RIGHTS_CATEGORY_FIELD_NAME = "rightsCategory"
EDM_RIGHTS_FIELD_NAME = "rights"
EDM_TIMESPAN_PREF_LABEL = "prefLabel"
UNLIMITED_RE_USE = "Unlimited Re-Use"
DC_CREATOR_FIELD_NAME = "creator"
DC_DATE_FIELD_NAME = "date"
DC_DESCRIPTION_FIELD_NAME = "description"
DC_TITLE_FIELD_NAME = "title"
DC_IDENTIFIER_FIELD_NAME = "identifier"

# Institutions file constants
INSTITUTIONS_URL = (
    "https://raw.githubusercontent.com/dpla/ingestion3"
    "/refs/heads/develop/src/main/resources/wiki/institutions_v2.json"
)
UPLOAD_FIELD_NAME = "upload"
INSTITUTIONS_FIELD_NAME = "institutions"
WIKIDATA_FIELD_NAME = "Wikidata"

# AWS constants
S3_RETRIES = 3
S3_BUCKET = "dpla-mdpdb"  # TODO change for prod
# we use sha1 because that's what commons uses for identifying files
S3_KEY_CHECKSUM = "sha1"
S3_KEY_METADATA = "Metadata"
S3_KEY_CONTENT_TYPE = "ContentType"


# http
HTTP_REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
            (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
}
AUTHORIZATION_HEADER = "Authorization"

# IIIF
JSON_LD_AT_CONTEXT = "@context"
JSON_LD_AT_ID = "@id"
IIIF_DEFAULT_JPG_SUFFIX = "default.jpg"
IIIF_ID = "id"
IIIF_BODY = "body"
IIIF_ITEMS = "items"
IIIF_RESOURCE = "resource"
IIIF_IMAGES = "images"
IIIF_CANVASES = "canvases"
IIIF_SEQUENCES = "sequences"
IIIF_FULL_RES_JPG_SUFFIX = "/full/full/0/default.jpg"
IIIF_PRESENTATION_API_MANIFEST_V2 = "http://iiif.io/api/presentation/2/context.json"
IIIF_PRESENTATION_API_MANIFEST_V3 = "http://iiif.io/api/presentation/3/context.json"

CONTENTDM_IIIF_MANIFEST_JSON = "/manifest.json"
CONTENTDM_IIIF_INFO = "/iiif/info/"
CONTENT_DM_ISSHOWNAT_REGEX = r"^/cdm/ref/collection/(.*)/id/(.*)$"  # todo
