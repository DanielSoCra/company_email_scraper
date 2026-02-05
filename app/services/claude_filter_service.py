"""Claude AI filter service for B2B prospect classification.

This module integrates with the Anthropic Messages API (Claude Haiku) to classify
company records as valid B2B prospects or exclusions. It uses batch processing
for cost efficiency, retries with exponential backoff for resilience, and a
rule-based fallback filter when API calls fail or responses are malformed.

Key behaviors:
- Build structured prompts for consistent JSON responses.
- Process companies in configurable batches for token/cost optimization.
- Retry transient failures (429/5xx) with exponential backoff.
- Fall back to keyword-based filtering when API calls fail.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import httpx

from ..config import CLAUDE_BATCH_SIZE_MAX, CLAUDE_BATCH_SIZE_MIN, get_settings

logger = logging.getLogger(__name__)

_shared_claude_client: httpx.AsyncClient | None = None


def _get_claude_http_client() -> httpx.AsyncClient:
    """Return a shared AsyncClient for Claude API calls."""
    global _shared_claude_client
    if _shared_claude_client is None:
        timeout = httpx.Timeout(30.0)
        _shared_claude_client = httpx.AsyncClient(timeout=timeout)
    return _shared_claude_client


async def close_claude_http_client() -> None:
    """Close the shared AsyncClient used for Claude API calls."""
    global _shared_claude_client
    if _shared_claude_client is not None:
        await _shared_claude_client.aclose()
        _shared_claude_client = None


class ClaudeFilterService:
    """Service for classifying companies via Claude's Messages API."""

    API_ENDPOINT = "https://api.anthropic.com/v1/messages"
    DEFAULT_MODEL = "claude-3-haiku-20240307"
    RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
    BACKOFF_DELAYS = [5, 15, 45]

    def __init__(
        self,
        client: httpx.AsyncClient,
        api_key: str,
        model: str | None = None,
        batch_size: int = 75,
        max_retries: int = 3,
    ) -> None:
        """Initialize the filter service.

        Args:
            client: Async HTTP client for Claude API requests.
            api_key: Anthropic API key for authentication.
            model: Claude model name (defaults to Claude Haiku).
            batch_size: Target batch size for classification requests.
            max_retries: Maximum number of retry attempts on API failures.
        """
        self.client = client
        self.api_key = api_key or ""
        self.model = model or self.DEFAULT_MODEL
        self.batch_size = min(
            max(batch_size, CLAUDE_BATCH_SIZE_MIN),
            CLAUDE_BATCH_SIZE_MAX,
        )
        self.max_retries = max(0, max_retries)
        self.use_fallback_only = not bool(self.api_key)

        if self.use_fallback_only:
            logger.warning(
                "ANTHROPIC_API_KEY is missing; Claude filtering will use fallback only."
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

        Example:
            filter_service = create_claude_filter()
            results = await filter_service.classify_batch(companies)

        Raises:
            Exception: Propagates unexpected errors during classification.
        """
        if not companies:
            return []

        logger.info(
            "Classifying %d companies using Claude (batch size=%d)",
            len(companies),
            self.batch_size,
        )
        logger.debug(
            "Company names for classification: %s",
            [company.get("name", "") for company in companies],
        )

        if self.use_fallback_only:
            logger.error("Claude API key unavailable; using fallback filter.")
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
                    "Claude classification failed for batch size %d: %s",
                    len(batch),
                    exc,
                )
                logger.exception("Falling back to keyword filter for batch.")
                results.extend(self.fallback_filter(batch))

        return results

    async def _make_api_request(self, prompt: str, max_retries: int = 3) -> dict[str, Any]:
        """Send a classification request to Claude with retry and backoff.

        Args:
            prompt: Prompt string to send to Claude.
            max_retries: Maximum number of retries on retryable errors.

        Returns:
            Parsed JSON response body from Claude.

        Raises:
            httpx.HTTPStatusError: If a non-retryable error occurs.
            httpx.RequestError: If the request fails after retries.
            httpx.TimeoutException: If the request times out after retries.
        """
        headers = {
            "x-api-key": self.api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        payload = {
            "model": self.model,
            "max_tokens": 4096,
            "messages": [{"role": "user", "content": prompt}],
        }

        logger.debug("Claude request payload: %s", payload)

        for attempt in range(max_retries + 1):
            try:
                response = await self.client.post(
                    self.API_ENDPOINT,
                    headers=headers,
                    json=payload,
                )
            except (httpx.RequestError, httpx.TimeoutException) as exc:
                if attempt >= max_retries:
                    logger.exception(
                        "Claude request failed after %d attempts", attempt + 1
                    )
                    raise

                delay = self.BACKOFF_DELAYS[min(attempt, len(self.BACKOFF_DELAYS) - 1)]
                logger.warning(
                    "Claude request error on attempt %d/%d: %s; retrying in %s seconds",
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
                    "Non-retryable response status %s", response.status_code
                )
                raise

            logger.debug("Claude response status %s", response.status_code)
            response_data = response.json()
            logger.debug("Claude response payload: %s", response_data)
            return response_data

        raise httpx.RequestError("Claude request failed after retries", request=None)

    def _build_classification_prompt(self, companies: list[dict[str, Any]]) -> str:
        """Build a structured prompt for B2B classification.

        Args:
            companies: List of company dictionaries with name, category, and website.

        Returns:
            Prompt string for Claude classification.

        Example:
            prompt = service._build_classification_prompt(companies)
        """
        lines = [
            "Classify these German companies as B2B prospects or exclude them.",
            "",
            "Exclude: Schools, kindergartens, government offices, churches,",
            "non-profits, residential properties.",
            "Include: Businesses, shops, restaurants, professional services,",
            "manufacturers.",
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
                "Respond with JSON array:",
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

        Example:
            filtered = service.fallback_filter(companies)
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
        """Split a list of companies into batches of a target size.

        Args:
            companies: List of company dictionaries.
            target_size: Desired maximum batch size.

        Returns:
            List of company batches.
        """
        if target_size <= 0 or len(companies) <= target_size:
            return [companies]

        return [companies[i : i + target_size] for i in range(0, len(companies), target_size)]

    def _parse_classification_response(
        self, response_data: dict[str, Any], expected_count: int
    ) -> list[dict[str, Any]]:
        """Parse the Claude response into a list of classification dicts.

        Args:
            response_data: JSON response from Claude.
            expected_count: Expected number of classifications.

        Returns:
            List of classification dictionaries.

        Raises:
            ValueError: If the response is missing or malformed.
            json.JSONDecodeError: If the response text is not valid JSON.
        """
        text = self._extract_response_text(response_data)
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            logger.error("Failed to parse Claude response as JSON: %s", exc)
            logger.debug("Claude response text: %s", text)
            raise

        if not isinstance(data, list):
            raise ValueError("Claude response is not a JSON array")

        if expected_count and len(data) != expected_count:
            logger.warning(
                "Claude response count mismatch: expected=%d actual=%d",
                expected_count,
                len(data),
            )

        return data

    def _merge_classifications(
        self,
        companies: list[dict[str, Any]],
        classifications: list[dict[str, Any]],
    ) -> None:
        """Merge classification results into company dictionaries.

        Args:
            companies: Company dictionaries to update.
            classifications: Parsed classification results from Claude.
        """
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
        """Extract the text content from a Claude Messages API response.

        Args:
            response_data: Response data returned by Claude.

        Returns:
            Text content containing JSON.

        Raises:
            ValueError: If no text content is found.
        """
        content = response_data.get("content")
        if isinstance(content, list) and content:
            first = content[0]
            if isinstance(first, dict):
                text = first.get("text", "")
                if text:
                    return text
            if isinstance(first, str) and first:
                return first

        if isinstance(content, str) and content:
            return content

        raise ValueError("Claude response missing text content")


def create_claude_filter() -> ClaudeFilterService:
    """Factory function to create a ClaudeFilterService instance.

    Returns:
        Configured ClaudeFilterService instance.

    Example:
        filter_service = create_claude_filter()
    """
    settings = get_settings()
    client = _get_claude_http_client()

    return ClaudeFilterService(
        client=client,
        api_key=settings.api.anthropic_api_key,
        model=settings.api.claude_model,
        batch_size=settings.api.claude_batch_size,
        max_retries=settings.api.claude_max_retries,
    )


def generate_mock_claude_response(
    classifications: list[dict[str, Any]]
) -> dict[str, Any]:
    """Generate a mock Claude response payload for testing.

    Args:
        classifications: List of classification dicts to serialize.

    Returns:
        Mock response payload mirroring Claude's Messages API structure.

    Example:
        mock = generate_mock_claude_response([
            {"company_index": 0, "classification": "valid_b2b", "reason": "Sample"}
        ])
    """
    return {
        "content": [
            {
                "type": "text",
                "text": json.dumps(classifications),
            }
        ]
    }


def validate_classification_result(company: dict[str, Any]) -> bool:
    """Validate that a company dict contains valid classification results.

    Args:
        company: Company dictionary to validate.

    Returns:
        True if classification fields are present and valid, False otherwise.

    Example:
        is_valid = validate_classification_result(company)
    """
    classification = company.get("ai_classification")
    reason = company.get("ai_classification_reason")
    if classification not in {"valid_b2b", "exclude"}:
        return False
    if not isinstance(reason, str) or not reason:
        return False
    return True
