INVALID_CONTENT_TYPES = [
    "text/html",
    "application/json",
    "application/xml",
    "text/plain",
]
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
FIND_BY_HASH_URL_PREFIX: str = (
    "https://commons.wikimedia.org/w/api.php?action=query&format=json"
    "&list=allimages&aisha1="
)
FIND_BY_HASH_QUERY_FIELD_NAME = "query"
FIND_BY_HASH_ALLIMAGES_FIELD_NAME = "allimages"
