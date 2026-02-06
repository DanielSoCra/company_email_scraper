"""Stadtbranchenbuch scraper service with streaming, retries, and rate limiting."""

import logging
import random
import re
import time
from typing import Any, Dict, Iterator, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StadtbranchenbuchScraper:
    """Scraper for extracting company data from Stadtbranchenbuch.

    This service supports streaming results, city validation, retry logic,
    rate limiting, and user agent rotation.

    Example:
        scraper = StadtbranchenbuchScraper(min_delay=1.0, max_delay=2.0)
        if scraper.validate_city("Bad Boll", "73087"):
            for company in scraper.scrape_city("Bad Boll", "73087"):
                print(company["name"])
    """

    BASE_URL = "https://{city}.stadtbranchenbuch.com"
    SEARCH_PATH = "/F/firmenverzeichnis.html"
    USER_AGENTS = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101",
    ]

    def __init__(self, min_delay: float = 1.0, max_delay: float = 2.0) -> None:
        """Initialize scraper.

        Args:
            min_delay: Minimum delay between requests in seconds.
            max_delay: Maximum delay between requests in seconds.
        """
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.user_agents = list(self.USER_AGENTS)
        self.session = requests.Session()

    def validate_city(self, city: str, zip_code: str) -> bool:
        """Validate whether a city has a Stadtbranchenbuch directory.

        Args:
            city: City name to validate.
            zip_code: Postal code for logging context.

        Returns:
            True if the city URL exists, False otherwise.
        """
        normalized_city = self._normalize_city_name(city)
        url = f"{self.BASE_URL.format(city=normalized_city)}{self.SEARCH_PATH}"
        headers = {"User-Agent": self._get_random_user_agent()}

        try:
            # Use GET directly - HEAD returns 404 on this website
            response = self.session.get(url, headers=headers, timeout=10)
            is_valid = response.status_code == 200
        except requests.RequestException:
            logger.exception(
                "City validation request failed for city=%s zip=%s url=%s",
                city,
                zip_code,
                url,
            )
            return False

        if is_valid:
            logger.info(
                "City validation succeeded for city=%s zip=%s url=%s",
                city,
                zip_code,
                url,
            )
        else:
            logger.info(
                "City validation failed for city=%s zip=%s url=%s status=%s",
                city,
                zip_code,
                url,
                response.status_code,
            )

        return is_valid

    def scrape_city(self, city: str, zip_code: str = "") -> Iterator[Dict[str, Any]]:
        """Scrape companies for a given city and yield them as they are processed.

        Args:
            city: City name to scrape.
            zip_code: Optional postal code for logging context.

        Yields:
            Company dictionaries matching the Company model fields.
        """
        normalized_city = self._normalize_city_name(city)
        base_url = self.BASE_URL.format(city=normalized_city)

        logger.info("Starting scrape for city=%s zip=%s", city, zip_code)
        processed = 0
        page = 1
        max_pages = 50  # Safety limit

        while page <= max_pages:
            # Page 1: firmenverzeichnis.html, Page 2+: firmenverzeichnis_N.html
            if page == 1:
                url = f"{base_url}/F/firmenverzeichnis.html"
            else:
                url = f"{base_url}/F/firmenverzeichnis_{page}.html"

            logger.info("Scraping page %d: %s", page, url)
            page_count = 0

            for company in self._scrape_page(url, normalized_city, is_first_page=(page == 1)):
                processed += 1
                page_count += 1
                logger.info(
                    "Yielding company %d: %s",
                    processed,
                    company.get("name", ""),
                )
                yield company

            # Stop if no companies found on this page
            if page_count == 0:
                logger.info("No companies found on page %d, stopping pagination", page)
                break

            page += 1

        logger.info("Completed scraping city=%s, total companies=%d", city, processed)

    @staticmethod
    def _normalize_city_name(city: str) -> str:
        """Normalize city name to URL format.

        Args:
            city: City name (e.g., "Bad Boll", "bad boll", "Köln").

        Returns:
            Normalized city name (e.g., "bad-boll", "koeln").
        """
        normalized = city.lower().strip()
        # Convert German umlauts
        normalized = normalized.replace("ä", "ae")
        normalized = normalized.replace("ö", "oe")
        normalized = normalized.replace("ü", "ue")
        normalized = normalized.replace("ß", "ss")
        # Replace spaces/hyphens with single hyphen
        normalized = re.sub(r"[\s-]+", "-", normalized)
        # Remove any remaining special characters
        normalized = re.sub(r"[^a-z0-9-]", "", normalized)
        # Remove leading/trailing hyphens
        normalized = normalized.strip("-")
        return normalized

    def _make_request_with_retry(self, url: str, max_retries: int = 3) -> requests.Response:
        """Make a request with retry and exponential backoff.

        Args:
            url: URL to request.
            max_retries: Maximum number of retries after the initial attempt.

        Returns:
            Successful HTTP response.

        Raises:
            requests.RequestException: If the request fails after retries.
        """
        retry_statuses = {429, 500, 502, 503, 504}
        backoff_delays = [5, 15, 45]

        for attempt in range(max_retries + 1):
            headers = {"User-Agent": self._get_random_user_agent()}
            try:
                response = self.session.get(url, headers=headers, timeout=30)
            except requests.RequestException as exc:
                if attempt >= max_retries:
                    logger.exception(
                        "Request failed after %d attempts for url=%s",
                        attempt + 1,
                        url,
                    )
                    raise

                delay = backoff_delays[min(attempt, len(backoff_delays) - 1)]
                logger.warning(
                    "Request error on attempt %d/%d for url=%s: %s; retrying in %s seconds",
                    attempt + 1,
                    max_retries + 1,
                    url,
                    exc,
                    delay,
                )
                time.sleep(delay)
                continue

            if response.status_code in retry_statuses:
                if attempt >= max_retries:
                    logger.error(
                        "Retryable status %s after %d attempts for url=%s",
                        response.status_code,
                        attempt + 1,
                        url,
                    )
                    response.raise_for_status()

                delay = backoff_delays[min(attempt, len(backoff_delays) - 1)]
                logger.warning(
                    "Retryable status %s on attempt %d/%d for url=%s; retrying in %s seconds",
                    response.status_code,
                    attempt + 1,
                    max_retries + 1,
                    url,
                    delay,
                )
                time.sleep(delay)
                continue

            try:
                response.raise_for_status()
            except requests.RequestException:
                logger.exception(
                    "Non-retryable response for url=%s status=%s",
                    url,
                    response.status_code,
                )
                raise

            return response

        raise requests.RequestException(f"Failed to fetch {url} after {max_retries} retries")

    def _apply_rate_limit(self) -> None:
        """Sleep for a randomized delay between requests."""
        delay = random.uniform(self.min_delay, self.max_delay)
        logger.debug("Applying rate limit delay of %.2f seconds", delay)
        time.sleep(delay)

    def _scrape_page(
        self, url: str, city: str, is_first_page: bool = True
    ) -> Iterator[Dict[str, Any]]:
        """Scrape company listings from a page and stream companies.

        Args:
            url: URL to scrape.
            city: City name for logging context.
            is_first_page: If True, use retry logic. If False, 503/404 means end of pagination.

        Yields:
            Company dictionaries. Empty if page doesn't exist.
        """
        if is_first_page:
            # First page uses retry logic
            try:
                response = self._make_request_with_retry(url)
            except requests.RequestException:
                logger.exception("Error fetching listings for city=%s url=%s", city, url)
                raise
        else:
            # Pagination pages: 503/404 means no more pages
            headers = {"User-Agent": self._get_random_user_agent()}
            try:
                response = self.session.get(url, headers=headers, timeout=30)
            except requests.RequestException:
                logger.exception("Error fetching listings for city=%s url=%s", city, url)
                raise

            if response.status_code in (404, 503):
                logger.info("Page returned %s (no more pages) for url=%s", response.status_code, url)
                return

            response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")
        try:
            yield from self._parse_listings(soup)
        finally:
            self._apply_rate_limit()

    def _parse_listings(self, soup: BeautifulSoup) -> Iterator[Dict[str, Any]]:
        """Parse company listings from page HTML and stream companies.

        Args:
            soup: BeautifulSoup object of the page.

        Yields:
            Company data dictionaries.
        """
        # Stadtbranchenbuch uses serp-listing class for company entries
        company_elements = soup.find_all("div", class_="serp-listing")

        logger.info("Found %d company entries", len(company_elements))

        for element in company_elements:
            company = self._extract_company_data(element)
            if company and company.get("name"):
                yield company

    def _extract_company_data(self, element) -> Optional[Dict[str, Any]]:
        """Extract company data from a listing element.

        Args:
            element: BeautifulSoup element containing company data.

        Returns:
            Dictionary with company data or None if parsing fails.
        """
        try:
            # h3 can be in div.address (premium) or div.image (regular)
            name_elem = element.find("h3")
            if not name_elem:
                return None

            name_text = name_elem.get_text(strip=True)

            company: Dict[str, Any] = {
                "source": "stadtbranchenbuch",
                "name": name_text,
                "address": "",
                "street": "",
                "postal_code": "",
                "city": "",
                "phone": "",
                "category": "",
                "website": "",
                "detail_url": "",
                "email": "",
                "ai_classification": None,
                "filtered_out": False,
            }

            # Extract detail URL from the link wrapping h3
            detail_link = name_elem.find_parent("a")
            if detail_link and detail_link.get("href"):
                company["detail_url"] = detail_link["href"]

            # Extract address data from div.address > address
            address_div = element.find("div", class_="address")
            if address_div:
                address_elem = address_div.find("address")
                if address_elem:
                    self._extract_address_data(address_elem, company)

            # Extract category from infos div
            infos_div = element.find("div", class_="infos")
            if infos_div:
                company["category"] = self._extract_category(infos_div, company["city"])

            # Look for homepage link with data-follow-link (actual URL) or href
            homepage_link = element.find("a", class_="homepage")
            if homepage_link:
                # Prefer data-follow-link as it contains the actual destination
                company["website"] = homepage_link.get("data-follow-link") or homepage_link.get("href", "")

            # Build full address string
            if company["street"] and company["postal_code"] and company["city"]:
                company["address"] = (
                    f"{company['street']}, {company['postal_code']} {company['city']}"
                )

            return company
        except Exception:
            logger.warning(
                "Error extracting company data for name=%s",
                element.find("h3").get_text(strip=True) if element.find("h3") else "",
                exc_info=True,
            )
            return None

    def _extract_address_data(self, address_elem, company: Dict[str, Any]) -> None:
        """Extract address components from address element.

        Args:
            address_elem: BeautifulSoup address element.
            company: Company dictionary to update.
        """
        street_div = address_elem.find("div")
        if street_div:
            company["street"] = street_div.get_text(strip=True)

        spans = address_elem.find_all("span")
        if len(spans) >= 2:
            company["postal_code"] = spans[0].get_text(strip=True)
            company["city"] = spans[1].get_text(strip=True)

        phone_div = address_elem.find("div", class_="phone")
        if phone_div:
            company["phone"] = phone_div.get_text(strip=True)

    def _extract_category(self, infos_div, city: str) -> str:
        """Extract category from infos div.

        Args:
            infos_div: BeautifulSoup element containing category info.
            city: City name to exclude from category.

        Returns:
            Category string.
        """
        spans = infos_div.find_all("span")
        for span in spans:
            text = span.get_text(strip=True)
            if text and text != city:
                return text

        divs = infos_div.find_all("div", recursive=False)
        for div in divs:
            if "datasource-badge" not in div.get("class", []):
                text = div.get_text(strip=True)
                if text and city:
                    parts = text.split(city)
                    if parts[0].strip():
                        return parts[0].strip()

        return ""

    def _get_random_user_agent(self) -> str:
        """Select a random user agent string.

        Returns:
            User agent string.
        """
        user_agent = random.choice(self.user_agents)
        logger.debug("Selected user agent: %s", user_agent)
        return user_agent


def create_scraper(min_delay: float = 1.0, max_delay: float = 2.0) -> StadtbranchenbuchScraper:
    """Factory function to create scraper instance with configuration.

    Args:
        min_delay: Minimum delay between requests in seconds.
        max_delay: Maximum delay between requests in seconds.

    Returns:
        Configured StadtbranchenbuchScraper instance.
    """
    return StadtbranchenbuchScraper(min_delay=min_delay, max_delay=max_delay)
