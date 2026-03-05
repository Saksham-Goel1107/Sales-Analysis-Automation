"""
SW Noida Store — Daily Sales Analysis Pipeline
===============================================
Reads raw sales + stock data from Google Sheets.
Runs all analyses from chicken_analyses.py.
Writes every result back to Google Sheets.
Zero files ever touch disk.

Run:  python analyze.py
Env:  SERVICE_ACCOUNT_FILE  (path to service.json)
      SALES_SHEET_ID        (Google Sheet ID for raw sales)
      STOCK_SHEET_ID        (Google Sheet ID for raw stock)
      OUTPUT_SHEET_ID       (Google Sheet ID for results)
      SALES_WORKSHEET       (worksheet/tab name, default: Sheet1)
      STOCK_WORKSHEET       (worksheet/tab name, default: Sheet1)
      CASHIER_ID            (default: sw-noida-cashier)
      TIMEZONE              (default: Asia/Kolkata)
"""

from __future__ import annotations

import logging
import os
import re
import sys
import time
from datetime import datetime
from typing import TYPE_CHECKING

import gspread
import numpy as np
import pandas as pd
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

# Load .env file for local test runs (Docker supplies env vars automatically)
load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("sw-noida-analysis")


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG — read from environment, fail fast with clear messages
# ─────────────────────────────────────────────────────────────────────────────

def _require_env(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        log.critical("Missing required environment variable: %s", name)
        sys.exit(1)
    return val


SERVICE_ACCOUNT_FILE = _require_env("SERVICE_ACCOUNT_FILE")
SALES_SHEET_ID       = _require_env("SALES_SHEET_ID")
OUTPUT_SHEET_ID      = _require_env("OUTPUT_SHEET_ID")

SALES_WORKSHEET      = os.environ.get("SALES_WORKSHEET", "Sheet1")
CASHIER_ID           = os.environ.get("CASHIER_ID",      "sw-noida-cashier")
TIMEZONE             = os.environ.get("TIMEZONE",         "Asia/Kolkata")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

# Category regex rules → canonical name (applied IN ORDER, first match wins)
CATEGORY_RULES: list[tuple[str, str]] = [
    (r".*jewel.*",           "Jewellery"),
    (r".*bag.*",             "Bags"),
    (r".*(cosmetic|makeup).*", "Cosmetics"),
    (r".*skin.*",            "Skincare"),
    (r".*(f&b|food|beverage).*", "Beverages"),
    (r".*live.*menu.*",      "Live Menu"),
    (r".*snack.*",           "Snack Spot"),
    (r".*frag.*",            "Fragrances"),
    (r".*style.*",           "Style Studio"),
    (r".*croch.*",           "Crochet"),
    (r".*tedd.*",            "Teddy"),
]

ALLOWED_CATEGORIES = [
    "Jewellery", "Bags", "Cosmetics", "Skincare",
    "Beverages", "Live Menu", "Snack Spot", "Fragrances",
    "Style Studio", "Crochet", "Teddy",
]

PRICE_BAND_STEP = 50     # ₹50 buckets
MAX_PRICE_RANGE = 10_000  # ceiling for cut bins


# ─────────────────────────────────────────────────────────────────────────────
# GOOGLE SHEETS CLIENT
# ─────────────────────────────────────────────────────────────────────────────

def _build_client() -> gspread.Client:
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return gspread.authorize(creds)


def _sheet_to_df(client: gspread.Client, sheet_id: str, worksheet: str) -> pd.DataFrame:
    """Read an entire worksheet and return as a DataFrame. Retries on transient errors."""
    if "your_" in sheet_id.lower() or "1abc" in sheet_id.lower():
        log.error("Invalid Sheet ID detected: '%s'. Please update your .env file with real IDs.", sheet_id)
        sys.exit(1)

    for attempt in range(1, 4):
        try:
            sh  = client.open_by_key(sheet_id)
            ws  = sh.worksheet(worksheet)
            data = ws.get_all_values()
            if len(data) < 2:
                log.warning("Sheet %s/%s has no data rows.", sheet_id, worksheet)
                return pd.DataFrame()
            headers = data[0]
            rows    = data[1:]
            df = pd.DataFrame(rows, columns=headers)
            # replace empty strings with NaN so pandas handles them correctly
            df.replace("", np.nan, inplace=True)
            log.info("Loaded %d rows from '%s' (sheet: %s)", len(df), worksheet, sheet_id)
            return df
        except Exception as exc:
            log.warning("Google API error attempt %d/3: %s", attempt, exc)
            time.sleep(5 * attempt)
    log.error("Failed to load sheet %s/%s after 3 attempts.", sheet_id, worksheet)
    sys.exit(1)


def _write_df_to_tab(
    client: gspread.Client,
    sheet_id: str,
    tab_name: str,
    df: pd.DataFrame,
) -> None:
    """Write a DataFrame to a named tab. Creates the tab if it doesn't exist."""
    sh = client.open_by_key(sheet_id)

    # get or create worksheet
    existing = [ws.title for ws in sh.worksheets()]
    if tab_name in existing:
        ws = sh.worksheet(tab_name)
        ws.clear()
    else:
        ws = sh.add_worksheet(title=tab_name, rows=max(len(df) + 10, 100), cols=max(len(df.columns) + 5, 26))

    # Convert everything to strictly safe strings via pure Python
    # This prevents pandas from accidentally retaining float(NaN) in mixed-type columns
    raw_data = [df.columns.tolist()] + df.values.tolist()
    
    def _safe_str(val: object) -> str:
        if pd.isna(val):
            return ""
        s = str(val).strip()
        if s in ("nan", "NaN", "<NA>", "None", "NaT", "inf", "-inf"):
            return ""
        # Optionally, you can leave integers/floats as numbers if you want, 
        # but Google Sheets handles string numbers perfectly under USER_ENTERED.
        return s

    cleaned_data = [[_safe_str(v) for v in row] for row in raw_data]

    ws.update(cleaned_data, value_input_option="USER_ENTERED")
    log.info("Written %d rows × %d cols → tab '%s'", len(df), len(df.columns), tab_name)
    # polite pause to respect Sheets API rate limit (60 writes/min per user)
    time.sleep(1.2)


# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING & CLEANING
# ─────────────────────────────────────────────────────────────────────────────

def load_and_clean_sales(client: gspread.Client) -> pd.DataFrame:
    """Load raw sales, clean types, filter cashier, normalise categories."""
    df = _sheet_to_df(client, SALES_SHEET_ID, SALES_WORKSHEET)
    if df.empty:
        log.error("Sales sheet is empty — aborting.")
        sys.exit(1)

    # normalise column names: strip + lower
    df.columns = df.columns.str.strip()

    # ── date ──────────────────────────────────────────────────────────────────
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
    df["date"] = df["date"].dt.tz_convert(TIMEZONE).dt.tz_localize(None)

    # ── cashier filter ────────────────────────────────────────────────────────
    if "billed_by" not in df.columns:
        log.error("Column 'billed_by' not found. Available: %s", df.columns.tolist())
        sys.exit(1)

    df = df[df["billed_by"] == CASHIER_ID].copy()
    log.info("After cashier filter: %d rows", len(df))

    # ── numeric coercion ──────────────────────────────────────────────────────
    for col in ["saleUnitPrice", "quantity", "totalAmount"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # revenue derived column
    if "saleUnitPrice" in df.columns and "quantity" in df.columns:
        df["revenue"] = df["saleUnitPrice"] * df["quantity"]

    # ── drop critical nulls ───────────────────────────────────────────────────
    df = df.dropna(subset=["brand", "category", "saleUnitPrice", "name"])

    # ── category normalisation ────────────────────────────────────────────────
    df["category"] = df["category"].astype(str).str.strip().str.lower()

    def _normalise_category(raw: str) -> str:
        for pattern, label in CATEGORY_RULES:
            if re.match(pattern, raw):
                return label
        return raw  # keep original if no rule matches

    df["category"] = df["category"].apply(_normalise_category)
    before = len(df)
    df = df[df["category"].isin(ALLOWED_CATEGORIES)].copy()
    log.info("Category filter: %d → %d rows", before, len(df))

    # ── price range ───────────────────────────────────────────────────────────
    max_p   = int(df["saleUnitPrice"].max() + PRICE_BAND_STEP * 2)
    max_p   = max(max_p, MAX_PRICE_RANGE)
    bins    = np.arange(0, max_p, PRICE_BAND_STEP)
    df["price_range"] = pd.cut(df["saleUnitPrice"], bins=bins, right=False)

    # ── sku normalisation ─────────────────────────────────────────────────────
    if "sku" not in df.columns and "code" in df.columns:
        df["sku"] = df["code"]
    elif "sku" in df.columns and df["sku"].isna().all() and "code" in df.columns:
        df["sku"] = df["code"]
        
    if "sku" in df.columns:
        df["sku"] = df["sku"].apply(_clean_sku)

    # ── day/night segment ─────────────────────────────────────────────────────
    hour = df["date"].dt.hour
    df["day_night"] = np.select(
        [(hour >= 9) & (hour < 19), (hour >= 19) | (hour < 6)],
        ["DAY", "NIGHT"],
        default="OTHER",
    )

    log.info("Clean sales DataFrame: %d rows, %d cols", *df.shape)
    return df


def _clean_sku(val: object) -> str:
    if pd.isna(val) or val is None:
        return ""
    s = str(val).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s.lstrip("0")

# ─────────────────────────────────────────────────────────────────────────────
# ANALYSIS FUNCTIONS — each returns a dict[tab_name → DataFrame]
# ─────────────────────────────────────────────────────────────────────────────

def analyse_product_price_range(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """1. Product-level price range summary (category / brand / name)."""
    product_base = (
        df.groupby(["category", "brand", "name"], as_index=False, observed=True)
        .agg(
            total_sales=("totalAmount", "sum"),
            units_sold=("quantity", "sum"),
            min_price=("saleUnitPrice", "min"),
            max_price=("saleUnitPrice", "max"),
        )
    )
    product_base["range_start"] = (product_base["min_price"] // 50) * 50
    product_base["range_end"]   = (np.ceil(product_base["max_price"] / 50)) * 50
    product_base["price_range"] = (
        "[" + product_base["range_start"].astype(int).astype(str)
        + ", " + product_base["range_end"].astype(int).astype(str) + ")"
    )
    out = product_base[
        ["category", "brand", "name", "price_range", "total_sales", "units_sold"]
    ].sort_values(["category", "brand", "name"]).reset_index(drop=True)
    return {"Product_PriceRange": out}


def analyse_category_price_range(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """2. Category-level dominant price range (all transactions, day+night combined)."""
    cat_pr = (
        df.groupby(["category", "price_range"], observed=True, as_index=False)
        .agg(total_sales=("totalAmount", "sum"), units_sold=("quantity", "sum"))
        .sort_values(["category", "price_range"])
    )
    idx   = cat_pr.groupby("category")["total_sales"].idxmax()
    top   = cat_pr.loc[idx, ["category", "price_range"]].rename(columns={"price_range": "top_price_range"})
    cat_pr = cat_pr.merge(top, on="category", how="left")
    cat_pr["price_range"]     = cat_pr["price_range"].astype(str)
    cat_pr["top_price_range"] = cat_pr["top_price_range"].astype(str)
    return {"Category_PriceRange": cat_pr.reset_index(drop=True)}


def analyse_brand_price_range(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """3. Brand-level dominant price range."""
    brand_pr = (
        df.groupby(["category", "brand", "price_range"], observed=True, as_index=False)
        .agg(total_sales=("totalAmount", "sum"), units_sold=("quantity", "sum"))
        .sort_values(["category", "brand", "price_range"])
    )
    idx      = brand_pr.groupby(["category", "brand"])["total_sales"].idxmax()
    top      = brand_pr.loc[idx, ["category", "brand", "price_range"]].rename(columns={"price_range": "top_price_range"})
    brand_pr = brand_pr.merge(top, on=["category", "brand"], how="left")
    brand_pr["price_range"]     = brand_pr["price_range"].astype(str)
    brand_pr["top_price_range"] = brand_pr["top_price_range"].astype(str)
    return {"Brand_PriceRange": brand_pr.reset_index(drop=True)}


def analyse_day_night_split(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """4. Category + Brand price-range analysis with Day/Night pivot."""
    seg_df = df[df["day_night"].isin(["DAY", "NIGHT"])].copy()

    # ── Category Day/Night ────────────────────────────────────────────────────
    cat_dn = (
        seg_df.groupby(["category", "price_range", "day_night"], observed=True, as_index=False)
        .agg(total_sales=("totalAmount", "sum"), units_sold=("quantity", "sum"))
    )
    cat_pivot = (
        cat_dn.pivot_table(
            index=["category", "price_range"],
            columns="day_night",
            values=["total_sales", "units_sold"],
            fill_value=0,
        )
    )
    cat_pivot.columns = [f"{c[0]}_{c[1].lower()}" for c in cat_pivot.columns]
    cat_pivot = cat_pivot.reset_index()
    cat_pivot["total_sales_combined"] = (
        cat_pivot.get("total_sales_day", 0) + cat_pivot.get("total_sales_night", 0)
    )
    idx  = cat_pivot.groupby("category")["total_sales_combined"].idxmax()
    top  = cat_pivot.loc[idx, ["category", "price_range"]].rename(columns={"price_range": "top_price_range"})
    cat_pivot = cat_pivot.merge(top, on="category", how="left")
    cat_pivot["price_range"]     = cat_pivot["price_range"].astype(str)
    cat_pivot["top_price_range"] = cat_pivot["top_price_range"].astype(str)

    # ── Brand Day/Night ───────────────────────────────────────────────────────
    brand_dn = (
        seg_df.groupby(["category", "brand", "price_range", "day_night"], observed=True, as_index=False)
        .agg(total_sales=("totalAmount", "sum"), units_sold=("quantity", "sum"))
    )
    brand_pivot = (
        brand_dn.pivot_table(
            index=["category", "brand", "price_range"],
            columns="day_night",
            values=["total_sales", "units_sold"],
            fill_value=0,
        )
    )
    brand_pivot.columns = [f"{c[0]}_{c[1].lower()}" for c in brand_pivot.columns]
    brand_pivot = brand_pivot.reset_index()
    brand_pivot["total_sales_combined"] = (
        brand_pivot.get("total_sales_day", 0) + brand_pivot.get("total_sales_night", 0)
    )
    idx       = brand_pivot.groupby(["category", "brand"])["total_sales_combined"].idxmax()
    top       = brand_pivot.loc[idx, ["category", "brand", "price_range"]].rename(columns={"price_range": "top_price_range"})
    brand_pivot = brand_pivot.merge(top, on=["category", "brand"], how="left")
    brand_pivot["price_range"]     = brand_pivot["price_range"].astype(str)
    brand_pivot["top_price_range"] = brand_pivot["top_price_range"].astype(str)

    return {
        "Cat_DayNight_PriceRange":   cat_pivot.reset_index(drop=True),
        "Brand_DayNight_PriceRange": brand_pivot.reset_index(drop=True),
    }


def analyse_top5_consumers(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """5. Top-5 consumers for Skincare & Cosmetics (value + volume)."""

    def _clean_first_name(x: object) -> str | float:
        if pd.isna(x):
            return np.nan  # type: ignore[return-value]
        x = re.sub(r"[^A-Za-z\s]", "", str(x)).strip()
        if not x:
            return np.nan  # type: ignore[return-value]
        return x.split()[0].lower()

    sc_cos = df[df["category"].isin(["Skincare", "Cosmetics"])].copy()

    if sc_cos.empty:
        log.warning("No Skincare/Cosmetics data found — skipping top-5 analysis.")
        return {}

    if "customerName" not in sc_cos.columns or "customerMobile" not in sc_cos.columns:
        log.warning("customerName/customerMobile columns missing — skipping top-5 analysis.")
        return {}

    sc_cos["first_name"] = sc_cos["customerName"].apply(_clean_first_name)
    sc_cos["customerMobile"] = sc_cos["customerMobile"].astype(str).str.strip()

    def _display(row: pd.Series) -> str:
        name  = row["first_name"]
        phone = row["customerMobile"]
        has_name  = pd.notna(name) and str(name) not in ("nan", "None", "")
        has_phone = phone not in ("nan", "None", "")
        if has_name and has_phone:
            return f"{name} - {phone}"
        if has_name:
            return str(name)
        if has_phone:
            return phone
        return "unknown"

    sc_cos["customer_display"] = sc_cos.apply(_display, axis=1)
    sc_cos = sc_cos[sc_cos["customer_display"] != "unknown"].copy()

    value_s = sc_cos.groupby(["category", "customer_display"], as_index=False).agg(
        total_value=("totalAmount", "sum")
    )
    volume_s = sc_cos.groupby(["category", "customer_display"], as_index=False).agg(
        total_units=("quantity", "sum")
    )

    results: dict[str, pd.DataFrame] = {}
    for cat in ["Skincare", "Cosmetics"]:
        results[f"Top5_{cat}_Value"] = (
            value_s[value_s["category"] == cat]
            .sort_values("total_value", ascending=False)
            .head(5)
            .reset_index(drop=True)
        )
        results[f"Top5_{cat}_Volume"] = (
            volume_s[volume_s["category"] == cat]
            .sort_values("total_units", ascending=False)
            .head(5)
            .reset_index(drop=True)
        )
    return results


def analyse_time_based_transactions(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """6. Time-based transaction analysis — bill level, day vs night."""
    work_df = df[["number", "date", "totalAmount"]].copy() if "number" in df.columns else df.copy()

    if "number" not in work_df.columns:
        log.warning("'number' column missing — skipping time-based analysis.")
        return {}

    # bill-level collapse
    bill_df = (
        work_df.groupby("number", as_index=False)
        .agg(date=("date", "min"), totalAmount=("totalAmount", "sum"))
    )

    hour = bill_df["date"].dt.hour
    conds   = [(hour >= 9) & (hour < 19), (hour >= 19) | (hour < 6)]
    choices = ["09AM-07PM", "07PM-06AM"]
    bill_df["time_segment"] = np.select(conds, choices, default="ignore")
    bill_df = bill_df[bill_df["time_segment"] != "ignore"].copy()

    analysis = (
        bill_df.groupby("time_segment")
        .agg(
            transactions=("number", "nunique"),
            total_sales=("totalAmount", "sum"),
            avg_order_value=("totalAmount", "mean"),
        )
        .reset_index()
    )

    bill_df["date"] = bill_df["date"].astype(str)

    return {
        "Time_Analysis_Summary": analysis.reset_index(drop=True),
        "Time_Analysis_Bill_Level": bill_df.reset_index(drop=True),
    }


def analyse_cosmetics_price_bands(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """7. Cosmetics-only 100-rupee price band distribution."""
    cos_df = df[df["category"].isin(["Cosmetics", "Skincare"])].copy()
    if cos_df.empty:
        return {}

    bins = np.arange(0, int(cos_df["saleUnitPrice"].max() + 200), 100)
    cos_df["price_band_100"] = pd.cut(cos_df["saleUnitPrice"], bins=bins, right=False)

    result = (
        cos_df.groupby(["category", "price_band_100"], observed=True)
        .agg(total_sales=("totalAmount", "sum"), total_units=("quantity", "sum"))
        .reset_index()
        .sort_values(["category", "price_band_100"])
    )
    result["price_band_100"]  = result["price_band_100"].astype(str)

    top_idx   = result.groupby("category")["total_sales"].idxmax()
    top_bands = result.loc[top_idx, ["category", "price_band_100"]].rename(
        columns={"price_band_100": "dominant_band"}
    )
    result = result.merge(top_bands, on="category", how="left")

    return {"Cosmetics_PriceBands": result.reset_index(drop=True)}


# ─────────────────────────────────────────────────────────────────────────────
# METADATA TAB — run timestamp + row counts
# ─────────────────────────────────────────────────────────────────────────────

def build_run_metadata(results: dict[str, pd.DataFrame]) -> pd.DataFrame:
    now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = [{"tab": "RunMetadata", "run_at": now, "rows": "", "cols": ""}]
    for tab, df in results.items():
        rows.append({"tab": tab, "run_at": now, "rows": len(df), "cols": len(df.columns)})
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("═" * 60)
    log.info("SW Noida Analysis Pipeline — %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("═" * 60)

    client = _build_client()
    log.info("Google Sheets client authenticated ✓")

    # ── Load ──────────────────────────────────────────────────────────────────
    sales_df  = load_and_clean_sales(client)

    # ── Analyse ───────────────────────────────────────────────────────────────
    all_results: dict[str, pd.DataFrame] = {}

    analysers = [
        ("Product Price Range",          lambda: analyse_product_price_range(sales_df)),
        ("Category Price Range",         lambda: analyse_category_price_range(sales_df)),
        ("Brand Price Range",            lambda: analyse_brand_price_range(sales_df)),
        ("Day/Night Split",              lambda: analyse_day_night_split(sales_df)),
        ("Top-5 Consumers",             lambda: analyse_top5_consumers(sales_df)),
        ("Time-Based Transactions",      lambda: analyse_time_based_transactions(sales_df)),
        ("Cosmetics Price Bands",        lambda: analyse_cosmetics_price_bands(sales_df)),
    ]

    for name, fn in analysers:
        try:
            result = fn()
            all_results.update(result)
            log.info("✓ %s → %d tab(s)", name, len(result))
        except Exception as exc:  # noqa: BLE001
            log.error("✗ %s failed: %s", name, exc, exc_info=True)

    # ── Write metadata tab first ──────────────────────────────────────────────
    meta_df = build_run_metadata(all_results)
    all_results = {"RunMetadata": meta_df, **all_results}

    # ── Write to Google Sheets ────────────────────────────────────────────────
    log.info("Writing %d tabs to output sheet…", len(all_results))
    for tab_name, result_df in all_results.items():
        try:
            _write_df_to_tab(client, OUTPUT_SHEET_ID, tab_name, result_df)
        except Exception as exc:  # noqa: BLE001
            log.error("Failed to write tab '%s': %s", tab_name, exc, exc_info=True)

    log.info("═" * 60)
    log.info("Pipeline complete. %d tabs written.", len(all_results))
    log.info("═" * 60)


if __name__ == "__main__":
    main()
