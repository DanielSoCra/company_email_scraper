"""Gemini AI filter service for B2B prospect classification.

This module integrates with Google's Gemini API to classify company records
as valid B2B prospects or exclusions. It uses batch processing for efficiency,
retries with exponential backoff for resilience, and a rule-based fallback
filter when API calls fail or responses are malformed.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any

import httpx

from ..config import AI_BATCH_SIZE_MAX, AI_BATCH_SIZE_MIN, get_settings

logger = logging.getLogger(__name__)

_shared_gemini_client: httpx.AsyncClient | None = None


def _get_gemini_http_client() -> httpx.AsyncClient:
    """Return a shared AsyncClient for Gemini API calls."""
    global _shared_gemini_client
    if _shared_gemini_client is None:
        timeout = httpx.Timeout(60.0)
        _shared_gemini_client = httpx.AsyncClient(timeout=timeout)
    return _shared_gemini_client


async def close_gemini_http_client() -> None:
    """Close the shared AsyncClient used for Gemini API calls."""
    global _shared_gemini_client
    if _shared_gemini_client is not None:
        await _shared_gemini_client.aclose()
        _shared_gemini_client = None


class GeminiFilterService:
    """Service for classifying companies via Google's Gemini API."""

    API_ENDPOINT = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    DEFAULT_MODEL = "gemini-3-flash-preview"
    RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
    BACKOFF_DELAYS = [5, 15, 45]

    def __init__(
        self,
        client: httpx.AsyncClient,
        api_key: str,
        model: str | None = None,
        batch_size: int = 25,
        max_retries: int = 3,
    ) -> None:
        """Initialize the filter service.

        Args:
            client: Async HTTP client for Gemini API requests.
            api_key: Google API key for authentication.
            model: Gemini model name (defaults to gemini-2.0-flash).
            batch_size: Target batch size for classification requests.
            max_retries: Maximum number of retry attempts on API failures.
        """
        self.client = client
        self.api_key = api_key or ""
        self.model = model or self.DEFAULT_MODEL
        self.batch_size = min(
            max(batch_size, AI_BATCH_SIZE_MIN),
            AI_BATCH_SIZE_MAX,
        )
        self.max_retries = max(0, max_retries)
        self.use_fallback_only = not bool(self.api_key)

        if self.use_fallback_only:
            logger.warning(
                "GEMINI_API_KEY is missing; AI filtering will use fallback only."
            )

    async def classify_batch(self, companies: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Classify a batch of companies as valid B2B prospects or exclusions.

        Args:
            companies: List of company dictionaries containing name, category,
                website, and address fields.

        Returns:
            List of company dictionaries updated with:
            - ai_classification: "valid_b2b" or "exclude"
            - ai_classification_reason: short explanation
            - filtered_out: boolean flag
        """
        if not companies:
            return []

        logger.info(
            "Classifying %d companies using Gemini (batch size=%d)",
            len(companies),
            self.batch_size,
        )

        if self.use_fallback_only:
            logger.error("Gemini API key unavailable; using fallback filter.")
            return self.fallback_filter(companies)

        results: list[dict[str, Any]] = []
        for batch in self._split_batch(companies, self.batch_size):
            prompt = self._build_classification_prompt(batch)
            try:
                response_data = await self._make_api_request(
                    prompt, max_retries=self.max_retries
                )
                classifications = self._parse_classification_response(
                    response_data, expected_count=len(batch)
                )
                self._merge_classifications(batch, classifications)
                results.extend(batch)
            except Exception as exc:
                logger.error(
                    "Gemini classification failed for batch size %d: %s",
                    len(batch),
                    exc,
                )
                logger.exception("Falling back to keyword filter for batch.")
                results.extend(self.fallback_filter(batch))

        return results

    async def _make_api_request(self, prompt: str, max_retries: int = 3) -> dict[str, Any]:
        """Send a classification request to Gemini with retry and backoff.

        Args:
            prompt: Prompt string to send to Gemini.
            max_retries: Maximum number of retries on retryable errors.

        Returns:
            Parsed JSON response body from Gemini.

        Raises:
            httpx.HTTPStatusError: If a non-retryable error occurs.
            httpx.RequestError: If the request fails after retries.
        """
        url = self.API_ENDPOINT.format(model=self.model)
        params = {"key": self.api_key}
        payload = {
            "contents": [
                {
                    "parts": [{"text": prompt}]
                }
            ],
            "generationConfig": {
                "temperature": 0.1,
                "maxOutputTokens": 16384,
            },
        }

        logger.debug("Gemini request to model: %s", self.model)

        for attempt in range(max_retries + 1):
            try:
                response = await self.client.post(
                    url,
                    params=params,
                    json=payload,
                )
            except (httpx.RequestError, httpx.TimeoutException) as exc:
                if attempt >= max_retries:
                    logger.exception(
                        "Gemini request failed after %d attempts", attempt + 1
                    )
                    raise

                delay = self.BACKOFF_DELAYS[min(attempt, len(self.BACKOFF_DELAYS) - 1)]
                logger.warning(
                    "Gemini request error on attempt %d/%d: %s; retrying in %s seconds",
                    attempt + 1,
                    max_retries + 1,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
                continue

            if response.status_code in self.RETRY_STATUS_CODES:
                if attempt >= max_retries:
                    logger.error(
                        "Retryable status %s after %d attempts",
                        response.status_code,
                        attempt + 1,
                    )
                    response.raise_for_status()

                delay = self.BACKOFF_DELAYS[min(attempt, len(self.BACKOFF_DELAYS) - 1)]
                logger.warning(
                    "Retryable status %s on attempt %d/%d; retrying in %s seconds",
                    response.status_code,
                    attempt + 1,
                    max_retries + 1,
                    delay,
                )
                await asyncio.sleep(delay)
                continue

            try:
                response.raise_for_status()
            except httpx.HTTPStatusError:
                logger.exception(
                    "Non-retryable response status %s: %s",
                    response.status_code,
                    response.text,
                )
                raise

            logger.debug("Gemini response status %s", response.status_code)
            response_data = response.json()
            logger.debug("Gemini response: %s", response_data)
            return response_data

        raise httpx.RequestError("Gemini request failed after retries", request=None)

    def _build_classification_prompt(self, companies: list[dict[str, Any]]) -> str:
        """Build a structured prompt for B2B classification.

        Args:
            companies: List of company dictionaries with name, category, and website.

        Returns:
            Prompt string for Gemini classification.
        """
        lines = [
            "Classify these German companies as B2B prospects or exclude them.",
            "",
            "Exclude: Schools, kindergartens, government offices, churches,",
            "non-profits, residential properties, private individuals without a business.",
            "Include: Businesses, shops, restaurants, professional services,",
            "manufacturers, craftsmen, freelancers with businesses.",
            "",
            "Companies:",
        ]

        for idx, company in enumerate(companies):
            name = company.get("name") or ""
            category = company.get("category") or ""
            website = company.get("website") or "null"
            address = company.get("address") or ""
            line = (
                f"[{idx}] Name: \"{name}\", Category: \"{category}\", "
                f"Website: \"{website}\", Address: \"{address}\""
            )
            lines.append(line)

        lines.extend(
            [
                "",
                "Respond with ONLY a JSON array, no other text:",
                "[",
                "  {\"company_index\": 0, \"classification\": \"valid_b2b\", "
                "\"reason\": \"Professional craft business\"},",
                "  {\"company_index\": 1, \"classification\": \"exclude\", "
                "\"reason\": \"Elementary school\"}",
                "]",
            ]
        )

        return "\n".join(lines)

    def fallback_filter(self, companies: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Apply a keyword-based fallback filter when the API fails.

        Args:
            companies: List of company dictionaries to classify.

        Returns:
            List of updated company dictionaries.
        """
        exclude_keywords = [
            "schule",
            "kindergarten",
            "kita",
            "rathaus",
            "gemeinde",
            "kirche",
            "verein",
            "friedhof",
            "bibliothek",
        ]

        for company in companies:
            name = (company.get("name") or "").lower()
            category = (company.get("category") or "").lower()
            matched_keyword = next(
                (kw for kw in exclude_keywords if kw in name or kw in category),
                "",
            )

            if matched_keyword:
                company["ai_classification"] = "exclude"
                company["ai_classification_reason"] = (
                    f"Keyword match: {matched_keyword}"
                )
                company["filtered_out"] = True
            else:
                company["ai_classification"] = "valid_b2b"
                company["ai_classification_reason"] = "No exclusion keywords matched"
                company["filtered_out"] = False

        return companies

    def _split_batch(
        self, companies: list[dict[str, Any]], target_size: int = 50
    ) -> list[list[dict[str, Any]]]:
        """Split a list of companies into batches of a target size."""
        if target_size <= 0 or len(companies) <= target_size:
            return [companies]

        return [companies[i : i + target_size] for i in range(0, len(companies), target_size)]

    def _parse_classification_response(
        self, response_data: dict[str, Any], expected_count: int
    ) -> list[dict[str, Any]]:
        """Parse the Gemini response into a list of classification dicts.

        Args:
            response_data: JSON response from Gemini.
            expected_count: Expected number of classifications.

        Returns:
            List of classification dictionaries.

        Raises:
            ValueError: If the response is missing or malformed.
            json.JSONDecodeError: If the response text is not valid JSON.
        """
        text = self._extract_response_text(response_data)

        # Extract JSON from response (Gemini sometimes wraps it in markdown)
        json_match = re.search(r'\[[\s\S]*\]', text)
        if json_match:
            text = json_match.group()

        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            logger.error("Failed to parse Gemini response as JSON: %s", exc)
            logger.debug("Gemini response text: %s", text)
            raise

        if not isinstance(data, list):
            raise ValueError("Gemini response is not a JSON array")

        if expected_count and len(data) != expected_count:
            logger.warning(
                "Gemini response count mismatch: expected=%d actual=%d",
                expected_count,
                len(data),
            )

        return data

    def _merge_classifications(
        self,
        companies: list[dict[str, Any]],
        classifications: list[dict[str, Any]],
    ) -> None:
        """Merge classification results into company dictionaries."""
        company_map = {idx: company for idx, company in enumerate(companies)}

        for item in classifications:
            if not isinstance(item, dict):
                continue
            index = item.get("company_index")
            classification = item.get("classification")
            reason = item.get("reason") or ""
            if not isinstance(index, int) or index not in company_map:
                continue
            if classification not in {"valid_b2b", "exclude"}:
                continue

            company = company_map[index]
            company["ai_classification"] = classification
            company["ai_classification_reason"] = reason
            company["filtered_out"] = classification == "exclude"

        missing = [
            company
            for company in companies
            if "ai_classification" not in company
        ]
        if missing:
            logger.warning(
                "Missing classifications for %d companies; applying fallback filter",
                len(missing),
            )
            self.fallback_filter(missing)

    @staticmethod
    def _extract_response_text(response_data: dict[str, Any]) -> str:
        """Extract the text content from a Gemini API response.

        Args:
            response_data: Response data returned by Gemini.

        Returns:
            Text content containing JSON.

        Raises:
            ValueError: If no text content is found.
        """
        candidates = response_data.get("candidates", [])
        if candidates:
            content = candidates[0].get("content", {})
            parts = content.get("parts", [])
            if parts:
                text = parts[0].get("text", "")
                if text:
                    return text

        raise ValueError("Gemini response missing text content")


def create_ai_filter() -> GeminiFilterService:
    """Factory function to create a GeminiFilterService instance.

    Returns:
        Configured GeminiFilterService instance.
    """
    settings = get_settings()
    client = _get_gemini_http_client()

    return GeminiFilterService(
        client=client,
        api_key=settings.api.gemini_api_key,
        model=settings.api.gemini_model,
        batch_size=settings.api.ai_batch_size,
        max_retries=settings.api.ai_max_retries,
    )
