#!/usr/bin/env python3
"""
Query MRL accepted claims for January 2026 via the Looker API.

Authentication is handled via environment variables (no ini file required):
  LOOKERSDK_BASE_URL      - e.g. https://yourcompany.looker.com
  LOOKERSDK_CLIENT_ID     - Looker API client ID
  LOOKERSDK_CLIENT_SECRET - Looker API client secret

Optional overrides (also available as CLI args or GitHub Actions inputs):
  MODEL_NAME   - LookML model name (skips auto-discovery if set)
  EXPLORE_NAME - LookML explore name (skips auto-discovery if set)
"""

import json
import os
import sys
from typing import Optional

import looker_sdk
from looker_sdk import models40 as models

# Keywords used to discover the right model/explore
MODEL_KEYWORDS = ["mrl", "insurance", "claim", "marshmallow"]
EXPLORE_KEYWORDS = ["claim", "mrl", "insurance"]
DATE_KEYWORDS = ["date", "created", "submitted", "reported", "received"]
STATUS_KEYWORDS = ["status", "decision", "outcome", "accepted", "disposition"]

# January 2026 date range in Looker filter syntax
DATE_FILTER = "2026/01/01 to 2026/02/01"
STATUS_FILTER = "accepted"


def _keyword_score(text: str, keywords: list[str]) -> int:
    text_lower = text.lower()
    return sum(1 for kw in keywords if kw in text_lower)


def discover_model_and_explore(sdk) -> tuple[str, str]:
    """Search all models/explores for one related to MRL claims."""
    print("Discovering Looker models...")
    all_models = sdk.all_lookml_models(fields="name,label,explores")

    candidates: list[tuple[int, str, str]] = []

    for model in all_models:
        model_name = model.name or ""
        model_label = model.label or ""
        model_score = _keyword_score(model_name + " " + model_label, MODEL_KEYWORDS)

        for explore_stub in (model.explores or []):
            explore_name = explore_stub.name or ""
            explore_label = explore_stub.label or ""
            explore_score = _keyword_score(
                explore_name + " " + explore_label, EXPLORE_KEYWORDS
            )
            total_score = model_score + explore_score
            if total_score > 0:
                candidates.append((total_score, model_name, explore_name))

    if not candidates:
        print("\nNo keyword-matched explores found. Listing all available models and explores:")
        for model in all_models:
            for explore_stub in (model.explores or []):
                print(f"  model={model.name}  explore={explore_stub.name}")
        sys.exit(
            "\nCould not auto-discover MRL claims explore. "
            "Set MODEL_NAME and EXPLORE_NAME env vars and re-run."
        )

    candidates.sort(reverse=True)
    print(f"\nTop candidate explores (by keyword score):")
    for score, m, e in candidates[:5]:
        print(f"  score={score}  model={m}  explore={e}")

    _, best_model, best_explore = candidates[0]
    print(f"\nUsing: model={best_model}  explore={best_explore}")
    return best_model, best_explore


def introspect_fields(sdk, model_name: str, explore_name: str) -> tuple[str, str, str]:
    """Return (date_field, status_field, count_field) for the explore."""
    print(f"\nIntrospecting fields for {model_name}/{explore_name}...")
    explore = sdk.lookml_model_explore(
        model_name,
        explore_name,
        fields="fields",
    )

    all_fields = []
    if explore.fields:
        all_fields += list(explore.fields.dimensions or [])
        all_fields += list(explore.fields.measures or [])

    date_field: Optional[str] = None
    status_field: Optional[str] = None
    count_field: Optional[str] = None

    for field in all_fields:
        fname = (field.name or "").lower()
        ftype = (field.type or "").lower()

        if count_field is None and ftype == "count":
            count_field = field.name

        if date_field is None and any(kw in fname for kw in DATE_KEYWORDS):
            if "date" in ftype or "time" in ftype:
                date_field = field.name

        if status_field is None and any(kw in fname for kw in STATUS_KEYWORDS):
            status_field = field.name

    # Fallbacks
    if count_field is None:
        for field in all_fields:
            if (field.type or "").lower() == "count":
                count_field = field.name
                break
    if count_field is None:
        count_field = f"{explore_name}.count"

    if date_field is None:
        sys.exit(
            f"Could not find a date dimension in {explore_name}. "
            "Set MODEL_NAME and EXPLORE_NAME for a different explore, "
            "or open an issue to add DATE_FIELD override support."
        )

    if status_field is None:
        print(
            "WARNING: Could not find a status/accepted dimension. "
            "Running query WITHOUT status filter — result will be ALL claims."
        )

    print(f"  date_field   = {date_field}")
    print(f"  status_field = {status_field}")
    print(f"  count_field  = {count_field}")
    return date_field, status_field, count_field


def run_query(
    sdk,
    model_name: str,
    explore_name: str,
    date_field: str,
    status_field: Optional[str],
    count_field: str,
) -> int:
    """Run the inline query and return the count of accepted claims."""
    filters: dict[str, str] = {date_field: DATE_FILTER}
    if status_field:
        filters[status_field] = STATUS_FILTER

    print(f"\nRunning query with filters: {filters}")

    body = models.WriteQuery(
        model=model_name,
        view=explore_name,
        fields=[count_field],
        filters=filters,
        limit="1",
    )

    raw = sdk.run_inline_query("json", body)
    rows = json.loads(raw)

    if not rows:
        return 0

    first_row = rows[0]
    # The count value may be keyed as the field name or as "count"
    value = first_row.get(count_field) or first_row.get("count") or 0
    return int(value)


def write_summary(count: int, model_name: str, explore_name: str) -> None:
    """Print result and optionally write to GitHub Actions job summary."""
    message = (
        f"\n{'='*60}\n"
        f"  MRL accepted claims — January 2026\n"
        f"  Model:   {model_name}\n"
        f"  Explore: {explore_name}\n"
        f"  Count:   {count:,}\n"
        f"{'='*60}\n"
    )
    print(message)

    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a") as f:
            f.write("## MRL Accepted Claims — January 2026\n\n")
            f.write(f"| Field | Value |\n|---|---|\n")
            f.write(f"| Model | `{model_name}` |\n")
            f.write(f"| Explore | `{explore_name}` |\n")
            f.write(f"| **Accepted Claims (Jan 2026)** | **{count:,}** |\n")


def main() -> None:
    # Allow env var overrides for model/explore (also used by GitHub Actions inputs)
    model_name = os.environ.get("MODEL_NAME", "").strip()
    explore_name = os.environ.get("EXPLORE_NAME", "").strip()

    sdk = looker_sdk.init40()
    print("Connected to Looker successfully.")

    if not model_name or not explore_name:
        model_name, explore_name = discover_model_and_explore(sdk)

    date_field, status_field, count_field = introspect_fields(sdk, model_name, explore_name)
    count = run_query(sdk, model_name, explore_name, date_field, status_field, count_field)
    write_summary(count, model_name, explore_name)


if __name__ == "__main__":
    main()
