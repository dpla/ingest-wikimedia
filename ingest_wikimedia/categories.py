import logging

import pywikibot
from pywikibot.site import BaseSite

from ingest_wikimedia.csrf import CsrfRecoveryFailed, with_csrf_recovery
from ingest_wikimedia.wikimedia import get_wikidata_site

# Stable Wikimedia ontology constants — these items will not change
WD_WIKIMEDIA_CONTENT_PARTNERSHIP = "Q97580368"  # "Wikimedia content partnership"
WD_WIKIMEDIA_CATEGORY = "Q4167836"  # "Wikimedia category"
WD_CONTAINS = "Q60474998"  # P4224 qualifier value on P7084

COMMONS_CATEGORY_PREFIX = "Category:Media contributed by "


class CategoryEnsurer:
    """
    Ensures that a Wikimedia Commons category page and corresponding Wikidata item
    exist for a DPLA institution before any of its files are uploaded.

    Idempotent: each institution Q-ID is only acted on once per session (tracked in
    _ensured). Raises on failure so callers can skip the item and preserve the invariant
    that no file is uploaded without its institution category already existing.
    """

    def __init__(
        self,
        commons_site: BaseSite,
        dry_run: bool = False,
    ):
        self.commons_site = commons_site
        self.dry_run = dry_run
        self._ensured: set[str] = set()
        # Institutions for which this session actually created new P8464
        # infrastructure (i.e. took the slow path in ensure()).  Callers can read
        # this to know whose files may have lost the race against Wikidata
        # replication lag and should be touched after upload to force re-render.
        self._newly_created: set[str] = set()
        self._hub_category_qids: dict[str, str] = {}
        self._wikidata_repo: BaseSite | None = None

    @property
    def newly_created(self) -> set[str]:
        """Q-IDs of institutions whose P8464 was first added in this session.

        Subsequent uploads of these institutions' files can race Wikidata's
        replication to Commons and land in `Media contributed by the Digital
        Public Library of America with unknown institution`.  See
        :func:`touch_institution_files`.
        """
        return self._newly_created.copy()

    @property
    def _repo(self) -> BaseSite:
        """Connect to Wikidata on first use, not at construction time.

        Deferring this avoids a MaxlagTimeoutError crash at uploader startup
        when Wikimedia is under load — the connection is only attempted when a
        category actually needs to be created or verified.
        """
        if self._wikidata_repo is None:
            self._wikidata_repo = get_wikidata_site().data_repository()
        return self._wikidata_repo

    def ensure(
        self,
        institution_qid: str,
        institution_name: str,
        hub_institution_qid: str,
    ) -> None:
        """
        Ensure the Commons category page and Wikidata item exist for the given
        institution. Does nothing if already ensured this session or if the
        institution's Wikidata item already has a P8464 claim.
        """
        if institution_qid in self._ensured:
            return

        institution_name = institution_name.strip()
        if not institution_name:
            raise ValueError(
                f"Institution {institution_qid} has no usable name; cannot create category."
            )

        category_name = COMMONS_CATEGORY_PREFIX + institution_name

        # Fast path: Commons category already exists — no Wikidata connection needed.
        # The Wikidata P8464 link was created when the category was, so skipping the
        # check here is safe and keeps uploads working when Wikidata is lagged.
        if self._commons_category_exists(category_name):
            logging.info(
                f"Category already set up for {institution_name} ({institution_qid})"
            )
            self._ensured.add(institution_qid)
            return

        # Commons category absent — check whether Wikidata already has P8464 set
        # (e.g. category exists under a different name or was created out-of-band).
        if self._institution_has_category(institution_qid):
            logging.info(
                f"Category already set up for {institution_name} ({institution_qid})"
            )
            self._ensured.add(institution_qid)
            return

        hub_category_qid = self._get_hub_category_qid(hub_institution_qid)

        logging.info(
            f"Creating category infrastructure for {institution_name} ({institution_qid})"
        )

        if self.dry_run:
            logging.info(f"Dry run: would create Commons page '{category_name}'")
            logging.info(f"Dry run: would create Wikidata item for '{category_name}'")
            logging.info(f"Dry run: would add P8464 to {institution_qid}")
            self._ensured.add(institution_qid)
            return

        if not self._commons_category_exists(category_name):
            self._create_commons_category(category_name)
            logging.info(f"Created Commons category '{category_name}'")
        else:
            logging.info(f"Commons category '{category_name}' already exists")

        category_qid = self._get_or_create_wikidata_category_item(
            institution_name, institution_qid, hub_category_qid, category_name
        )
        logging.info(f"Using Wikidata item {category_qid} for '{category_name}'")

        self._add_p8464_to_institution(institution_qid, category_qid)
        logging.info(f"Added P8464 to {institution_qid} → {category_qid}")

        self._ensured.add(institution_qid)
        # Reaching this point means we actually wrote new infrastructure this
        # session.  Track separately so callers can force-rerender the
        # institution's files once Wikidata replication has settled.
        self._newly_created.add(institution_qid)

    def _get_hub_category_qid(self, hub_institution_qid: str) -> str:
        """
        Returns the hub's Commons category item Q-ID by reading the P8464 claim on
        the hub's Wikidata item. Result is cached for the session.
        """
        if hub_institution_qid in self._hub_category_qids:
            return self._hub_category_qids[hub_institution_qid]

        repo = self._repo
        hub_item = pywikibot.ItemPage(repo, hub_institution_qid)
        hub_item.get()

        if "P8464" not in hub_item.claims:
            raise ValueError(
                f"Hub {hub_institution_qid} has no P8464 claim — "
                "cannot determine hub category item Q-ID"
            )

        target = hub_item.claims["P8464"][0].getTarget()
        if not isinstance(target, pywikibot.ItemPage):
            raise ValueError(
                f"Hub {hub_institution_qid} P8464 claim has unexpected target: {target!r}"
            )
        hub_category_qid = target.getID()
        self._hub_category_qids[hub_institution_qid] = hub_category_qid
        return hub_category_qid

    def _institution_has_category(self, institution_qid: str) -> bool:
        repo = self._repo
        item = pywikibot.ItemPage(repo, institution_qid)
        item.get()
        return "P8464" in item.claims

    def _commons_category_exists(self, category_name: str) -> bool:
        return pywikibot.Category(self.commons_site, category_name).exists()

    def _create_commons_category(self, category_name: str) -> None:
        page = pywikibot.Page(self.commons_site, category_name)
        page.text = "{{dpla cat}}"
        page.save(summary="Create institutional category")

    def _get_or_create_wikidata_category_item(
        self,
        institution_name: str,
        institution_qid: str,
        hub_category_qid: str,
        category_name: str,
    ) -> str:
        """Return existing Wikidata item Q-ID for the Commons category if one exists,
        otherwise create it. Guards against duplicate item creation on retry."""
        commons_page = pywikibot.Page(self.commons_site, category_name)
        try:
            existing = commons_page.data_item()
            return existing.getID()
        except pywikibot.exceptions.NoPageError:
            pass
        try:
            return self._create_wikidata_category_item(
                institution_name, institution_qid, hub_category_qid, category_name
            )
        except Exception as create_exc:
            # Another process may have created the item concurrently. Re-read before failing.
            try:
                return commons_page.data_item().getID()
            except pywikibot.exceptions.NoPageError:
                raise create_exc

    def _item_claim(self, property_id: str, target_qid: str) -> pywikibot.Claim:
        claim = pywikibot.Claim(self._repo, property_id)
        claim.setTarget(pywikibot.ItemPage(self._repo, target_qid))
        return claim

    def _create_wikidata_category_item(
        self,
        institution_name: str,
        institution_qid: str,
        hub_category_qid: str,
        category_name: str,
    ) -> str:
        new_item = pywikibot.ItemPage(self._repo)

        combines_partnership = self._item_claim(
            "P971", WD_WIKIMEDIA_CONTENT_PARTNERSHIP
        )
        combines_institution = self._item_claim("P971", institution_qid)
        instance_of = self._item_claim("P31", WD_WIKIMEDIA_CATEGORY)

        commons_cat_claim = pywikibot.Claim(self._repo, "P373")
        commons_cat_claim.setTarget(f"Media contributed by {institution_name}")

        related_cat = self._item_claim("P7084", hub_category_qid)
        contains_qualifier = self._item_claim("P4224", WD_CONTAINS)
        related_cat.addQualifier(contains_qualifier)

        data = {
            "labels": {"en": f"Media contributed by {institution_name}"},
            "sitelinks": {
                "commonswiki": {
                    "site": "commonswiki",
                    "title": category_name,
                }
            },
            "claims": [
                combines_partnership.toJSON(),
                combines_institution.toJSON(),
                instance_of.toJSON(),
                commons_cat_claim.toJSON(),
                related_cat.toJSON(),
            ],
        }

        with_csrf_recovery(
            self._repo,
            f"editEntity new-category ({institution_name})",
            lambda: new_item.editEntity(
                data, summary="Create new Wikimedia Commons category item."
            ),
        )
        return new_item.getID()

    def _add_p8464_to_institution(
        self, institution_qid: str, category_qid: str
    ) -> None:
        institution_item = pywikibot.ItemPage(self._repo, institution_qid)
        institution_item.get()

        existing_ids = []
        for existing_claim in institution_item.claims.get("P8464", []):
            target = existing_claim.getTarget()
            if (
                isinstance(target, pywikibot.ItemPage)
                and target.getID() == category_qid
            ):
                logging.info(f"P8464 already set on {institution_qid} → {category_qid}")
                return
            existing_ids.append(
                target.getID()
                if isinstance(target, pywikibot.ItemPage)
                else repr(target)
            )

        if existing_ids:
            raise ValueError(
                f"Institution {institution_qid} already has P8464 → {existing_ids}, "
                f"none match expected {category_qid}"
            )

        with_csrf_recovery(
            self._repo,
            f"addClaim P8464 to {institution_qid}",
            lambda: institution_item.addClaim(
                self._item_claim("P8464", category_qid),
                summary="Add Commons content partnership category.",
            ),
        )


def touch_institution_files(
    commons_site: BaseSite,
    institution_qid: str,
    log_each: bool = False,
) -> int:
    """Force-rerender all Commons file pages that reference this institution.

    Used to clear the Wikidata-replication-lag race: when CategoryEnsurer first
    adds a ``P8464`` claim to an institution's Wikidata item, files uploaded in
    the seconds immediately after may have rendered before the claim
    propagated to Commons' Wikibase client cache.  Those files land in
    ``Category:Media contributed by the Digital Public Library of America with
    unknown institution`` and stay there until something forces re-render.

    A null edit (``page.touch()``) is enough — MediaWiki re-expands the
    ``{{ Institution | wikidata = Q… }}`` template, picks up the now-visible
    ``P8464``, and the file moves to the correct category.

    If ``log_each`` is True, every touched file is logged at INFO level (used
    by ``fix-unknown-categories --verbose``).  Per-page errors are always
    logged as warnings and counted as failures but don't abort the loop.

    Returns the number of files successfully touched.
    """
    count = 0
    for page in commons_site.search(
        f'insource:"Institution" insource:"wikidata = {institution_qid}"',
        namespaces=[6],
    ):
        if log_each:
            logging.info(f"  Touching: {page.title()}")
        try:
            with_csrf_recovery(commons_site, f"touch {page.title()}", page.touch)
            count += 1
        except CsrfRecoveryFailed:
            # Session-level fatal — propagate past the per-page catch
            # so the caller can abort the run rather than logging one
            # warning per remaining file.
            raise
        except Exception as e:
            logging.warning(f"Failed to touch '{page.title()}'", exc_info=e)
    return count
