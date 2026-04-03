import logging

import pywikibot
from pywikibot.site import BaseSite

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
        wikidata_site: BaseSite,
        dry_run: bool = False,
    ):
        self.commons_site = commons_site
        self.wikidata_site = wikidata_site
        self.dry_run = dry_run
        self._ensured: set[str] = set()
        self._hub_category_qids: dict[str, str] = {}
        self._repo = wikidata_site.data_repository()

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

        if self._institution_has_category(institution_qid):
            logging.info(
                f"Category already set up for {institution_name} ({institution_qid})"
            )
            self._ensured.add(institution_qid)
            return

        institution_name = institution_name.strip()
        if not institution_name:
            raise ValueError(
                f"Institution {institution_qid} has no usable name; cannot create category."
            )

        category_name = COMMONS_CATEGORY_PREFIX + institution_name
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

        hub_category_qid = hub_item.claims["P8464"][0].getTarget().getID()
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
        if commons_page.exists():
            try:
                existing = commons_page.data_item()
                return existing.getID()
            except pywikibot.exceptions.NoWikidataItemError:
                pass
        return self._create_wikidata_category_item(
            institution_name, institution_qid, hub_category_qid, category_name
        )

    def _create_wikidata_category_item(
        self,
        institution_name: str,
        institution_qid: str,
        hub_category_qid: str,
        category_name: str,
    ) -> str:
        repo = self._repo
        new_item = pywikibot.ItemPage(repo)

        combines_partnership = pywikibot.Claim(repo, "P971")
        combines_partnership.setTarget(
            pywikibot.ItemPage(repo, WD_WIKIMEDIA_CONTENT_PARTNERSHIP)
        )

        combines_institution = pywikibot.Claim(repo, "P971")
        combines_institution.setTarget(pywikibot.ItemPage(repo, institution_qid))

        instance_of = pywikibot.Claim(repo, "P31")
        instance_of.setTarget(pywikibot.ItemPage(repo, WD_WIKIMEDIA_CATEGORY))

        commons_cat_claim = pywikibot.Claim(repo, "P373")
        commons_cat_claim.setTarget(f"Media contributed by {institution_name}")

        related_cat = pywikibot.Claim(repo, "P7084")
        related_cat.setTarget(pywikibot.ItemPage(repo, hub_category_qid))

        contains_qualifier = pywikibot.Claim(repo, "P4224")
        contains_qualifier.setTarget(pywikibot.ItemPage(repo, WD_CONTAINS))
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

        new_item.editEntity(data, summary="Create new Wikimedia Commons category item.")
        return new_item.getID()

    def _add_p8464_to_institution(
        self, institution_qid: str, category_qid: str
    ) -> None:
        repo = self._repo
        institution_item = pywikibot.ItemPage(repo, institution_qid)
        institution_item.get()

        for existing_claim in institution_item.claims.get("P8464", []):
            target = existing_claim.getTarget()
            if (
                isinstance(target, pywikibot.ItemPage)
                and target.getID() == category_qid
            ):
                logging.info(f"P8464 already set on {institution_qid} → {category_qid}")
                return
            existing_id = (
                target.getID()
                if isinstance(target, pywikibot.ItemPage)
                else repr(target)
            )
            raise ValueError(
                f"Institution {institution_qid} already has P8464 → {existing_id}, "
                f"expected {category_qid}"
            )

        claim = pywikibot.Claim(repo, "P8464")
        claim.setTarget(pywikibot.ItemPage(repo, category_qid))
        institution_item.addClaim(
            claim, summary="Add Commons content partnership category."
        )
