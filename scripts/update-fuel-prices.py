#!/usr/bin/env python3
"""
Weekly fuel price updater.
Sources:
  - EIA.gov       → US national average (requires EIA_API_KEY env var)
  - EU Oil Bulletin → All 27 EU countries (no key required)
  - Everything else → left unchanged (updated manually or via future sources)

Usage:
  EIA_API_KEY=your_key python3 scripts/update-fuel-prices.py
"""

import json
import os
import sys
import requests
from datetime import date, datetime

PRICES_FILE = "fuel-prices.json"
TIMEOUT = 15  # seconds per request


# ── EU country name → ISO code mapping (as used in the Oil Bulletin) ─────────
EU_NAME_TO_CODE = {
    "Belgium": "BE", "Bulgaria": "BG", "Czechia": "CZ", "Czech Republic": "CZ",
    "Denmark": "DK", "Germany": "DE", "Estonia": "EE", "Ireland": "IE",
    "Greece": "GR", "Spain": "ES", "France": "FR", "Croatia": "HR",
    "Italy": "IT", "Cyprus": "CY", "Latvia": "LV", "Lithuania": "LT",
    "Luxembourg": "LU", "Hungary": "HU", "Malta": "MT", "Netherlands": "NL",
    "Austria": "AT", "Poland": "PL", "Portugal": "PT", "Romania": "RO",
    "Slovenia": "SI", "Slovakia": "SK", "Finland": "FI", "Sweden": "SE",
}


def load_prices():
    with open(PRICES_FILE, "r") as f:
        return json.load(f)


def save_prices(data):
    with open(PRICES_FILE, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"✓ Saved {PRICES_FILE}")


def fetch_us_price(api_key):
    """
    Fetch US regular gasoline national average from EIA API v2.
    Returns price per litre in USD, or None on failure.
    """
    url = (
        "https://api.eia.gov/v2/petroleum/pri/gnd/data/"
        "?api_key={key}"
        "&frequency=weekly"
        "&data[0]=value"
        "&facets[duoarea][]=NUS"
        "&facets[product][]=EPM0"
        "&sort[0][column]=period"
        "&sort[0][direction]=desc"
        "&offset=0&length=1"
    ).format(key=api_key)

    try:
        r = requests.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        price_per_gallon = float(r.json()["response"]["data"][0]["value"])
        price_per_litre = round(price_per_gallon / 3.78541, 4)
        print(f"  US petrol: ${price_per_gallon:.3f}/gal → ${price_per_litre:.4f}/L")
        return price_per_litre
    except Exception as e:
        print(f"  ✗ EIA fetch failed: {e}")
        return None


def fetch_us_diesel_price(api_key):
    """
    Fetch US on-highway diesel national average from EIA API v2.
    Returns price per litre in USD, or None on failure.
    """
    url = (
        "https://api.eia.gov/v2/petroleum/pri/gnd/data/"
        "?api_key={key}"
        "&frequency=weekly"
        "&data[0]=value"
        "&facets[duoarea][]=NUS"
        "&facets[product][]=EPD2D"
        "&sort[0][column]=period"
        "&sort[0][direction]=desc"
        "&offset=0&length=1"
    ).format(key=api_key)

    try:
        r = requests.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        price_per_gallon = float(r.json()["response"]["data"][0]["value"])
        price_per_litre = round(price_per_gallon / 3.78541, 4)
        print(f"  US diesel: ${price_per_gallon:.3f}/gal → ${price_per_litre:.4f}/L")
        return price_per_litre
    except Exception as e:
        print(f"  ✗ EIA diesel fetch failed: {e}")
        return None


def fetch_eu_bulletin_prices():
    """
    Fetch latest EU Oil Bulletin prices from the EU Open Data Portal.
    Returns dict of { "DE": {"petrol": 1.82, "diesel": 1.71}, ... } or empty dict on failure.
    """
    try:
        # Step 1: get the dataset metadata to find the latest resource URL
        meta_url = "https://data.europa.eu/api/3/action/package_show?id=eu-oil-bulletin-prices-with-taxes"
        meta = requests.get(meta_url, timeout=TIMEOUT).json()

        if not meta.get("success"):
            raise ValueError("EU data portal returned success=false")

        # Step 2: find the latest CSV resource
        resources = meta["result"]["resources"]
        csv_resources = [r for r in resources if r.get("format", "").upper() in ("CSV", "TEXT/CSV")]
        if not csv_resources:
            raise ValueError("No CSV resource found in EU Oil Bulletin dataset")

        # Sort by last_modified descending and pick the latest
        csv_resources.sort(key=lambda r: r.get("last_modified", ""), reverse=True)
        csv_url = csv_resources[0]["url"]
        print(f"  EU Oil Bulletin CSV: {csv_url}")

        # Step 3: download and parse the CSV
        r = requests.get(csv_url, timeout=TIMEOUT)
        r.raise_for_status()

        results = {}
        lines = r.text.splitlines()

        # The bulletin CSV has headers on the first row.
        # Typical columns: Country, Date, Unleaded_95_EUR_L, Diesel_EUR_L, ...
        # We parse flexibly — look for columns containing "unleaded"/"95"/"petrol" and "diesel"
        if not lines:
            raise ValueError("Empty CSV response")

        header = [h.strip().lower() for h in lines[0].split(",")]

        # Find relevant column indices
        country_col = next((i for i, h in enumerate(header) if "country" in h), None)
        date_col    = next((i for i, h in enumerate(header) if "date" in h or "week" in h), None)
        petrol_col  = next((i for i, h in enumerate(header)
                            if any(k in h for k in ("unleaded 95", "euro 95", "petrol", "sp95", "e10"))), None)
        diesel_col  = next((i for i, h in enumerate(header)
                            if "diesel" in h and "pump" not in h), None)

        if country_col is None or petrol_col is None or diesel_col is None:
            raise ValueError(f"Could not identify required columns in: {header[:10]}")

        # Collect the latest entry per country
        latest_by_country = {}
        for line in lines[1:]:
            if not line.strip():
                continue
            cols = line.split(",")
            if len(cols) <= max(country_col, petrol_col, diesel_col):
                continue
            country_name = cols[country_col].strip().strip('"')
            date_str     = cols[date_col].strip() if date_col is not None else ""
            try:
                petrol = float(cols[petrol_col].strip())
                diesel = float(cols[diesel_col].strip())
            except ValueError:
                continue

            code = EU_NAME_TO_CODE.get(country_name)
            if not code:
                continue

            # Keep the entry with the most recent date
            existing = latest_by_country.get(code)
            if existing is None or date_str > existing["date"]:
                latest_by_country[code] = {"date": date_str, "petrol": petrol, "diesel": diesel}

        for code, vals in latest_by_country.items():
            results[code] = {"petrol": round(vals["petrol"], 4), "diesel": round(vals["diesel"], 4)}
            print(f"  {code}: petrol={vals['petrol']:.4f} diesel={vals['diesel']:.4f} EUR/L")

        return results

    except Exception as e:
        print(f"  ✗ EU Oil Bulletin fetch failed: {e}")
        return {}


def main():
    print(f"\n{'='*50}")
    print(f"Fuel price update — {date.today()}")
    print(f"{'='*50}\n")

    prices = load_prices()
    updated_countries = []

    # ── US via EIA ────────────────────────────────────────────────────────────
    eia_key = os.environ.get("EIA_API_KEY", "").strip()
    if eia_key:
        print("→ Fetching US prices from EIA...")
        us_petrol = fetch_us_price(eia_key)
        us_diesel = fetch_us_diesel_price(eia_key)
        if us_petrol is not None:
            prices["countries"]["US"]["petrol"] = us_petrol
            updated_countries.append("US petrol")
        if us_diesel is not None:
            prices["countries"]["US"]["diesel"] = us_diesel
            updated_countries.append("US diesel")
    else:
        print("→ EIA_API_KEY not set — skipping US update")
        print("  Get a free key at: https://www.eia.gov/opendata/register.php")

    # ── EU via Oil Bulletin ───────────────────────────────────────────────────
    print("\n→ Fetching EU prices from EU Oil Bulletin...")
    eu_prices = fetch_eu_bulletin_prices()
    for code, vals in eu_prices.items():
        if code in prices["countries"]:
            prices["countries"][code]["petrol"] = vals["petrol"]
            prices["countries"][code]["diesel"] = vals["diesel"]
            updated_countries.append(code)

    # ── Update metadata ───────────────────────────────────────────────────────
    prices["_meta"]["updated"] = str(date.today())

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*50}")
    if updated_countries:
        print(f"✓ Updated: {', '.join(updated_countries)}")
    else:
        print("⚠ No prices updated (all fetches failed or no API key set)")
        print("  Metadata timestamp updated regardless.")
    print(f"{'='*50}\n")

    save_prices(prices)
    return 0 if updated_countries else 1


if __name__ == "__main__":
    sys.exit(main())
