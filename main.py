from flask import Flask, render_template, request, jsonify
import pandas as pd
import os

app = Flask(__name__)

# Serve API and page from the same origin
API_BASE = "http://localhost:5001"

# ---- load data once ----
DATA_DIR = "./data"
RATE_PATH = os.path.join(DATA_DIR, "unodc_sexual_violence_rate_per100k.csv")
CLEAN_PATH = os.path.join(DATA_DIR, "unodc_sexual_violence_clean.csv")

rate_df = pd.read_csv(RATE_PATH)
clean_df = pd.read_csv(CLEAN_PATH)
rate_df.columns = [c.strip().lower() for c in rate_df.columns]
clean_df.columns = [c.strip().lower() for c in clean_df.columns]

if "unit of measurement" in rate_df.columns:
    rate_df = rate_df[rate_df["unit of measurement"].str.lower() == "rate per 100,000 population"]

rate_df["year"] = pd.to_numeric(rate_df["year"], errors="coerce").astype("Int64")

rate_df = (rate_df
           .sort_values(["year", "value"], ascending=[False, False])
           .drop_duplicates(subset=["iso3_code", "country", "year"], keep="first"))

def to_int(x, default):
    try:
        return int(x)
    except:
        return default

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/rates")
def rates():
    """
    Query parameters:
      - year: int (optional, defaults to latest available)
      - limit, offset: for pagination (optional)
      - q: substring filter on country name (optional)
    """
    year_param = request.args.get("year")
    q = (request.args.get("q") or "").strip().lower()
    limit  = to_int(request.args.get("limit"), 200)
    offset = to_int(request.args.get("offset"), 0)

    df = rate_df.copy()
    latest_year = to_int(year_param, int(df["year"].max()))
    df = df[df["year"] == latest_year]

    if q:
        df = df[df["country"].str.lower().str.contains(q, na=False)]

    total = len(df)
    page = df.sort_values("value", ascending=False).iloc[offset:offset+limit]
    data = page.to_dict(orient="records")

    return jsonify({
        "year": latest_year,
        "total": total,
        "count": len(data),
        "offset": offset,
        "limit": limit,
        "results": data
    })

@app.get("/country/<iso3>")
def countries(iso3):
    """
    Return the time-series for a country (rate per 100k + counts if available),
    and attach the correct region.
    """
    iso_code = iso3.strip().upper()

    mask_iso = rate_df.get("iso3_code", pd.Series(dtype=str)).str.upper() == iso_code
    rates = rate_df.loc[mask_iso].copy()
    if rates.empty:
        return jsonify({"error": f"No data available for {iso_code}"}), 404

    rates["year"] = pd.to_numeric(rates["year"], errors="coerce")
    rates = rates.dropna(subset=["year"])
    rates = rates.sort_values(["year", "value"], ascending=[True, False])
    rates = rates.drop_duplicates(subset=["year"], keep="first")

    counts_mask = (
        (clean_df.get("iso3_code", pd.Series(dtype=str)).str.upper() == iso_code) &
        (clean_df.get("unit of measurement", pd.Series(dtype=str)).str.lower() == "counts")
    )
    counts = clean_df.loc[counts_mask, ["year", "value"]].copy()
    if not counts.empty:
        counts["year"] = pd.to_numeric(counts["year"], errors="coerce")
        counts = counts.dropna(subset=["year"])
        counts = (counts.groupby("year", as_index=False)["value"]
                  .sum()
                  .rename(columns={"value": "count"}))

    out = rates[["year", "value", "country"]].rename(columns={"value": "rate_per_100k"})
    if not counts.empty:
        out = out.merge(counts, on="year", how="left")
    else:
        out["count"] = pd.NA

    region_name = None
    subregion_name = None

    if "region" in rates.columns:
        s = rates["region"].dropna().astype(str).str.strip()
        if not s.empty:
            region_name = s.iloc[0]

    if "subregion" in rates.columns:
        s = rates["subregion"].dropna().astype(str).str.strip()
        if not s.empty:
            subregion_name = s.iloc[0]

    if region_name is None or (subregion_name is None and "subregion" in clean_df.columns):
        lookup = clean_df.loc[clean_df.get("iso3_code", pd.Series(dtype=str)).str.upper() == iso_code]
        if region_name is None and "region" in clean_df.columns:
            s = lookup["region"].dropna().astype(str).str.strip()
            if not s.empty:
                region_name = s.iloc[0]
        if subregion_name is None and "subregion" in clean_df.columns:
            s = lookup["subregion"].dropna().astype(str).str.strip()
            if not s.empty:
                subregion_name = s.iloc[0]

    series = (out[["year", "rate_per_100k", "count"]]
              .sort_values("year")
              .to_dict(orient="records"))

    region_avg_latest = None
    region_avg_series = []

    if region_name and not out.empty:
        reg_key = str(region_name).strip()
        latest_year = int(out.sort_values("year").iloc[-1]["year"])

        region_map = pd.DataFrame(columns=["iso3_code", "region"])
        if "iso3_code" in clean_df.columns and "region" in clean_df.columns:
            region_map = clean_df[["iso3_code", "region"]].dropna(subset=["iso3_code", "region"]).copy()
        if not region_map.empty:
            region_map["iso3_code"] = region_map["iso3_code"].astype(str).str.upper().str.strip()
            region_map["region"] = region_map["region"].astype(str).str.strip()
            region_map = region_map.drop_duplicates(subset=["iso3_code"], keep="first")

        if "region" in rate_df.columns:
            rd_map = rate_df[["iso3_code", "region"]].dropna(subset=["iso3_code", "region"]).copy()
            rd_map["iso3_code"] = rd_map["iso3_code"].astype(str).str.upper().str.strip()
            rd_map["region"] = rd_map["region"].astype(str).str.strip()
            rd_map = rd_map.drop_duplicates(subset=["iso3_code"], keep="first")
            if region_map.empty:
                region_map = rd_map
            else:
                region_map = (rd_map.set_index("iso3_code")
                                   .combine_first(region_map.set_index("iso3_code"))
                                   .reset_index())

        regional = rate_df[["iso3_code", "year", "value"]].copy()
        regional["iso3_code"] = regional["iso3_code"].astype(str).str.upper().str.strip()
        regional["year"] = pd.to_numeric(regional["year"], errors="coerce")
        regional["value"] = pd.to_numeric(regional["value"], errors="coerce")

        if not region_map.empty:
            regional = regional.merge(region_map, on="iso3_code", how="left")
        else:
            regional["region"] = pd.NA

        region_means = (
            regional.dropna(subset=["year", "value", "region"])
                    .assign(region=lambda d: d["region"].astype(str).str.strip())
                    .groupby(["region", "year"], as_index=False)["value"].mean()
                    .rename(columns={"value": "rate_per_100k"})
        )

        reg_series_df = (region_means[region_means["region"] == reg_key]
                         .sort_values("year"))
        if not reg_series_df.empty:
            region_avg_series = reg_series_df[["year", "rate_per_100k"]].to_dict(orient="records")

            row = reg_series_df[reg_series_df["year"] <= latest_year].tail(1)
            if not row.empty:
                region_avg_latest = float(row.iloc[-1]["rate_per_100k"])

    return jsonify({
        "iso3": iso_code,
        "country": out["country"].iloc[0],
        "region": region_name,
        "subregion": subregion_name,
        "series": series,
        "region_avg_latest": region_avg_latest,
        "region_avg_series": region_avg_series
    })



@app.get("/top")
def top_countries():
    """
    Top-N countries by rate for a given year.
    Query params: year, n (default 20)
    """
    n = to_int(request.args.get("n"), 20)
    year_param = request.args.get("year")
    df = rate_df.copy()
    year = to_int(year_param, int(df["year"].max()))
    top = df[df["year"] == year].sort_values("value", ascending=False).head(n)
    return jsonify({
        "year": year,
        "results": top.rename(columns={"value": "rate_per_100k"}).to_dict(orient="records")
    })

@app.route("/")
def index():
    return render_template("index.html", api_base=API_BASE)

@app.route("/global-reach")
def global_reach():
    return render_template("./worldmap.html")

@app.route("/raising-awareness")
def awareness():
    return render_template('./awareness.html')

@app.route('/understanding-the-issue')
def the_issue():
    return render_template('./the-issue.html')

@app.route('/what-you-can-do')
def what_you_can_do():
    return render_template('./what-you-can-do.html')

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
