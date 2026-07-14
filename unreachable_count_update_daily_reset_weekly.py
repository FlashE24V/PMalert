#!/usr/bin/env python3
"""
unreachable_count_update_daily_reset_weekly.py

Fleet-Map updater + >24h unreachable alerts, PLUS daily-accumulating /
weekly-resetting counts for Level 2 unreachable chargers and Level 3 power
module faults (GitHub Actions safe, run daily via cron).

Priority rules (with CPF handling):
- Level 3 (DC fast): Charging > Available > Occupied   (single-port behavior)
- Level 2 CPF* (single-port): Charging > Available > Occupied
- Other Level 2 / multi-port: Available > Charging > Occupied

Inputs (GitHub Actions Secrets):
- CP_USERNAME, CP_PASSWORD
- Optional: THRESHOLD_HOURS (default 48)

Outputs (repo root / $GITHUB_WORKSPACE):
- stations_per_station_slim.csv              (NO stationID in output)
- status_latest_slim.csv                     (NO stationID in output; includes hours_unreachable)
- alerts_unreachable_gt24.json
- unreachable_event_counts.csv               (NO stationID in output; includes event counts)
- unreachable_new.csv                        (Level 2 chargers unreachable at any point THIS WEEK so far —
                                               running total, resets when a new ISO week starts)
- pm_new.csv                                 (Level 3 chargers with a Power Module fault THIS WEEK so far —
                                               running total, resets when a new ISO week starts)
- unreachable_weekly_history.csv             (permanent log: one row per completed week, final count)
- pm_weekly_history.csv                      (permanent log: one row per completed week, final count)
- .cp_unreach_state.json (persistent per-station state)
- .cp_weekly_state.json (persistent weekly-accumulator state)
- chargepoint_refresh.log

Unreachable definition (per ChargePoint API 5.1 getStationStatus.networkStatus):
- Unreachable iff StationNetworkStatus == "Unreachable" (case-insensitive)
  (i.e., values like Reachable / Unreachable)

Schedule this to run DAILY (not just weekly) — the weekly counters accumulate
day over day and automatically reset the first time a new ISO week (Mon-Sun)
is detected, archiving the prior week's final total to the history CSVs first.
"""

import os, re, time, shutil, tempfile, json
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import xml.etree.ElementTree as ET

import requests
import pandas as pd

# ===== Credentials from GitHub Secrets =====
USERNAME = os.getenv("CP_USERNAME")
PASSWORD = os.getenv("CP_PASSWORD")
if not USERNAME or not PASSWORD:
    raise RuntimeError(
        "Missing CP_USERNAME/CP_PASSWORD environment variables. "
        "Set these in GitHub → Settings → Secrets → Actions."
    )

# ===== SOAP endpoint / paths =====
ENDPOINT = "https://webservices.chargepoint.com/webservices/chargepoint/services/5.1"
OUTPUT_DIR = os.getenv("GITHUB_WORKSPACE", os.getcwd())

STATUS_OUT   = os.path.join(OUTPUT_DIR, "status_latest_slim.csv")
STATIONS_OUT = os.path.join(OUTPUT_DIR, "stations_per_station_slim.csv")
LOG_FILE     = os.path.join(OUTPUT_DIR, "chargepoint_refresh.log")

# State & alerts paths
STATE_PATH        = os.path.join(OUTPUT_DIR, ".cp_unreach_state.json")   # persisted across runs
ALERTS_PATH       = os.path.join(OUTPUT_DIR, "alerts_unreachable_gt24.json")
ALERT_THRESHOLD_HOURS = int(os.getenv("THRESHOLD_HOURS", "24"))

# Output with unreachable event counts
UNREACH_COUNTS_OUT = os.path.join(OUTPUT_DIR, "unreachable_event_counts.csv")

# NEW: detail outputs requested — Level 2 unreachable list / Level 3 power module fault list
UNREACHABLE_NEW_OUT = os.path.join(OUTPUT_DIR, "unreachable_new.csv")
PM_NEW_OUT = os.path.join(OUTPUT_DIR, "pm_new.csv")

# NEW: weekly accumulator state + permanent history logs (one row written per completed week)
WEEKLY_STATE_PATH = os.path.join(OUTPUT_DIR, ".cp_weekly_state.json")
WEEKLY_HISTORY_L2_OUT = os.path.join(OUTPUT_DIR, "unreachable_weekly_history.csv")
WEEKLY_HISTORY_L3_OUT = os.path.join(OUTPUT_DIR, "pm_weekly_history.csv")

# Substrings (case-insensitive) that identify a "power module" fault in faultReason.
# Confirm the exact wording your account uses by checking a run's pm_new.csv /
# faultReason values in status_latest_slim.csv, then adjust here if needed.
POWER_MODULE_FAULT_KEYWORDS = ["power module"]

# ===== Search footprint (adjust if needed) =====
LAT, LON, RADIUS_MILES, STATE_FILTER = 40.7128, -74.0060, 100, "NY"

# ===== Engine settings =====
PAGE_SIZE, STATUS_CONCURRENCY = 500, 20
HTTP_TIMEOUT, HTTP_RETRIES = 60, 3

# ===== Output schema (NO stationID) =====
FINAL_COLS = [
    "stationName","Address","City","State","postalCode","Lat","Long",
    "Charger type","Charger type (legend)","stationModel",
    "StationNetworkStatus","LastPortStatus","faultReason",
    "station_label","ports_total","ports_charging","ports_occupied","ports_available",
    "StatusTimestamp","hours_unreachable","_loaded_at_utc",
]

def log(msg):
    ts = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    line = f"{ts} {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

_session = requests.Session()

def build_envelope(body_xml: str) -> bytes:
    return f"""
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:urn="urn:dictionary:com.chargepoint.webservices">
  <soapenv:Header xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
    <wsse:Security soapenv:mustUnderstand="1">
      <wsse:UsernameToken>
        <wsse:Username>{USERNAME}</wsse:Username>
        <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">{PASSWORD}</wsse:Password>
      </wsse:UsernameToken>
    </wsse:Security>
  </soapenv:Header>
  <soapenv:Body>{body_xml}</soapenv:Body>
</soapenv:Envelope>
""".strip().encode("utf-8")

def post_soap(body_xml: str) -> bytes:
    headers = {"Content-Type": "text/xml; charset=UTF-8"}
    last_err = None
    for i in range(HTTP_RETRIES):
        try:
            r = _session.post(
                ENDPOINT,
                data=build_envelope(body_xml),
                headers=headers,
                timeout=HTTP_TIMEOUT
            )
            r.raise_for_status()
            b = r.content
            if b and (b.find(b"<Fault") != -1 or b.find(b":Fault") != -1):
                raise RuntimeError("SOAP Fault returned")
            return b
        except Exception as e:
            last_err = e
            log(f"HTTP/SOAP error (try {i+1}/{HTTP_RETRIES}): {e}")
            time.sleep(2 ** i)
    raise last_err

def strip_tag(tag: str) -> str:
    return tag.split("}")[-1] if "}" in tag else tag

# ---------------- Parsers ----------------
def parse_stations(xml_bytes: bytes):
    root = ET.fromstring(xml_bytes)
    body = root.find(".//{http://schemas.xmlsoap.org/soap/envelope/}Body")
    if body is None:
        return []
    rows = []
    for st in body.iter():
        if strip_tag(st.tag) == "stationData":
            rec = {}
            for child in st:
                name = strip_tag(child.tag)
                if name == "Port":
                    continue
                rec[name] = (child.text or "").strip()
            ports = [p for p in st if strip_tag(p.tag) == "Port"]
            if ports:
                for p in ports:
                    row = rec.copy()
                    row["portNumber"] = (p.findtext("./portNumber") or "").strip()
                    lat = p.findtext("./Geo/Lat")
                    lon = p.findtext("./Geo/Long")
                    row["Lat"] = float(lat) if lat else None
                    row["Long"] = float(lon) if lon else None
                    rows.append(row)
            else:
                rows.append(rec)
    return rows

def parse_status(xml_bytes: bytes):
    root = ET.fromstring(xml_bytes)
    body = root.find(".//{http://schemas.xmlsoap.org/soap/envelope/}Body")
    if body is None:
        return []
    out = []
    for st in body.iter():
        if strip_tag(st.tag) == "stationData":
            sid = (st.findtext("./stationID") or "").strip()
            station_net = (st.findtext("./networkStatus") or "").strip()
            for p in st.findall("./Port"):
                out.append({
                    "stationID": sid,
                    "portNumber": (p.findtext("./portNumber") or "").strip(),
                    "PortStatus": (p.findtext("./Status") or "").strip(),
                    "faultReason": (p.findtext("./faultReason") or "").strip(),
                    "StatusTimestamp": (p.findtext("./TimeStamp") or "").strip(),
                    "StationNetworkStatus": station_net,
                })
    return out

def parse_load(xml_bytes: bytes):
    root = ET.fromstring(xml_bytes)
    body = root.find(".//{http://schemas.xmlsoap.org/soap/envelope/}Body")
    if body is None:
        return []
    out = []
    for resp in body.iter():
        if strip_tag(resp.tag) == "getLoadResponse":
            for sd in resp.findall(".//stationData"):
                sid = (sd.findtext("./stationID") or "").strip()
                ports = sd.findall("./Port")
                totals = {"ports_total": 0, "ports_charging": 0, "ports_occupied": 0, "ports_available": 0}
                for p in ports:
                    totals["ports_total"] += 1
                    port_load_txt = (p.findtext("./portLoad") or "0").strip()
                    try:
                        port_load = float(port_load_txt)
                    except Exception:
                        port_load = 0.0
                    session_id = (p.findtext("./sessionID") or "").strip()
                    if port_load > 0:
                        totals["ports_charging"] += 1
                    elif session_id and session_id != "0":
                        totals["ports_occupied"] += 1
                    else:
                        totals["ports_available"] += 1
                out.append({"stationID": sid, **totals})
    return out

# --------------- Fetchers ---------------
def fetch_stations_full_per_station() -> pd.DataFrame:
    log("Fetching stations via getStations…")
    all_rows = []
    start = 0
    while True:
        body = f"""
<urn:getStations>
  <searchQuery>
    <geo><latitude>{LAT}</latitude><longitude>{LON}</longitude><distance>{RADIUS_MILES}</distance></geo>
    <state>{STATE_FILTER}</state>
    <startRecord>{start}</startRecord>
    <maxRecords>{PAGE_SIZE}</maxRecords>
  </searchQuery>
</urn:getStations>""".strip()
        xml = post_soap(body)
        page = parse_stations(xml)
        if not page:
            break
        all_rows.extend(page)
        start += PAGE_SIZE

    df_ports = pd.DataFrame(all_rows)
    if df_ports.empty:
        log("WARNING: getStations returned no rows")
        return df_ports

    if "stationID" not in df_ports.columns:
        df_ports["stationID"] = ""
    df_ports["stationID"] = df_ports["stationID"].astype(str)

    ensure = ["stationID","stationName","stationModel","Address","City","State","postalCode","sgName","sgname","Lat","Long"]
    for c in ensure:
        if c not in df_ports.columns:
            df_ports[c] = None

    def first_non_null(series):
        for v in series:
            if pd.notnull(v) and v != "":
                return v
        return None

    grouped = df_ports.groupby("stationID", as_index=False).agg({
        "stationName": first_non_null,
        "stationModel": first_non_null,
        "Address": first_non_null,
        "City": first_non_null,
        "State": first_non_null,
        "postalCode": first_non_null,
        "sgName": first_non_null,
        "sgname": first_non_null,
        "Lat": first_non_null,
        "Long": first_non_null,
    })

    def classify(model: str) -> str:
        m = (model or "").upper()
        l3 = ["CPE250","CPE200","EXPRESS","EXPRESS 200","EXPRESS 250","DCFC","TRITIUM","PK350","ABB","BTC","RTM","HPC"]
        l2 = ["CT4020","CT4025","CT4000","CT4010","CT4011","CT500","CT600","CT-4000","CPF25","CPF50","CPF32","CT4010-HD2","CT2000","WALLBOX"]
        if "GW" in m and not any(x in m for x in l2 + l3):
            return "Gateway (Not a Charger)"
        if any(x in m for x in l3) or "LEVEL 3" in m or "DC FAST" in m or "FAST" in m:
            return "Level 3"
        if any(x in m for x in l2) or "LEVEL 2" in m or " L2" in m:
            return "Level 2"
        return "Unknown"

    grouped["Charger type"] = grouped["stationModel"].apply(classify)

    # ✅ UPDATED legend logic: keeps old behavior AND supports newer Solar naming
    def legend(base, sg):
        sg_u = (sg or "")

        is_public = bool(re.search(r"\bPublic Stations\b", sg_u, flags=re.IGNORECASE))
        # Backwards-compatible: matches "Solar Stations" AND any group containing "Solar"
        is_solar  = bool(re.search(r"\bSolar\b", sg_u, flags=re.IGNORECASE))

        label = base
        if base and base.startswith("Gateway"):
            return None
        if is_public:
            label += " - Public Stations"
        if is_solar:
            label += " - Solar"
        return label

    sgcol = "sgName" if "sgName" in grouped.columns else ("sgname" if "sgname" in grouped.columns else None)
    sg_series = grouped[sgcol].astype(str) if sgcol else pd.Series([""] * len(grouped), index=grouped.index)
    grouped["Charger type (legend)"] = [legend(bt, sg) for bt, sg in zip(grouped["Charger type"], sg_series)]
    return grouped.drop_duplicates(subset=["stationID"], keep="first")

def fetch_status_for_station(sid: str):
    body = f"""
<urn:getStationStatus>
  <searchQuery><stationID>{sid}</stationID></searchQuery>
</urn:getStationStatus>""".strip()
    try:
        xml = post_soap(body)
        return parse_status(xml)
    except Exception as e:
        log(f"Status fetch failed for {sid}: {e}")
        return []

def fetch_all_statuses(station_ids):
    log(f"Fetching status for {len(station_ids)} stations (concurrency={STATUS_CONCURRENCY})…")
    rows = []
    with ThreadPoolExecutor(max_workers=STATUS_CONCURRENCY) as ex:
        futs = {ex.submit(fetch_status_for_station, sid): sid for sid in station_ids}
        for fut in as_completed(futs):
            try:
                rows.extend(fut.result())
            except Exception as e:
                log(f"Error in status future: {e}")

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["StatusTimestamp_dt"] = pd.to_datetime(df["StatusTimestamp"], errors="coerce", utc=True)
    df = df.sort_values(["stationID", "StatusTimestamp_dt"])

    agg = df.groupby("stationID", as_index=False).agg({
        "StatusTimestamp_dt": "last",
        "StationNetworkStatus": "last",
        "PortStatus": "last",
        "faultReason": "last"
    }).rename(columns={"StatusTimestamp_dt": "StatusTimestamp_dt_latest", "PortStatus": "LastPortStatus"})

    agg["StatusTimestamp"] = agg["StatusTimestamp_dt_latest"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return agg

def fetch_load_for_station(sid: str):
    body = f"""
<urn:getLoad>
  <searchQuery><stationID>{sid}</stationID></searchQuery>
</urn:getLoad>""".strip()
    try:
        xml = post_soap(body)
        return parse_load(xml)
    except Exception as e:
        log(f"Load fetch failed for {sid}: {e}")
        return []

def fetch_all_loads(station_ids):
    log(f"Fetching load for {len(station_ids)} stations (concurrency={STATUS_CONCURRENCY})…")
    rows = []
    with ThreadPoolExecutor(max_workers=STATUS_CONCURRENCY) as ex:
        futs = {ex.submit(fetch_load_for_station, sid): sid for sid in station_ids}
        for fut in as_completed(futs):
            try:
                rows.extend(fut.result())
            except Exception as e:
                log(f"Error in load future: {e}")
    return pd.DataFrame(rows)

# --------------- IO helpers ---------------
def atomic_write_csv(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix="cp_", suffix=".csv", dir=os.path.dirname(path))
    os.close(fd)
    try:
        df.to_csv(tmp, index=False, encoding="utf-8")
        shutil.move(tmp, path)
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass

def slim_output(merged: pd.DataFrame) -> pd.DataFrame:
    out = merged.copy()
    out["_loaded_at_utc"] = datetime.now(timezone.utc).isoformat()
    for c in FINAL_COLS:
        if c not in out.columns:
            out[c] = None
    if "Lat" in out.columns and "Long" in out.columns:
        out = out[pd.notnull(out["Lat"]) & pd.notnull(out["Long"])]
    return out[FINAL_COLS]

# --------------- NEW: detail-report writers ---------------
# --------------- NEW: weekly accumulator helpers ---------------
def iso_week_id(dt: datetime) -> str:
    y, w, _ = dt.isocalendar()
    return f"{y}-W{w:02d}"

def load_weekly_state():
    try:
        with open(WEEKLY_STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"week_id": None, "level2_ids": [], "level3_ids": []}

def save_weekly_state(state):
    tmp = WEEKLY_STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    os.replace(tmp, WEEKLY_STATE_PATH)

def append_history_row(csv_path, week_id, count):
    file_exists = os.path.isfile(csv_path)
    import csv as _csv
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = _csv.writer(f)
        if not file_exists:
            writer.writerow(["week", "count"])
        writer.writerow([week_id, count])

def roll_weekly_state(now_utc: datetime):
    """Returns the weekly state to use for today's run. If a new ISO week has
    started since the last run, archives last week's final totals to the
    permanent history CSVs, then resets the running lists to empty."""
    wk_id = iso_week_id(now_utc)
    weekly_state = load_weekly_state()

    if weekly_state.get("week_id") is not None and weekly_state.get("week_id") != wk_id:
        append_history_row(WEEKLY_HISTORY_L2_OUT, weekly_state["week_id"], len(weekly_state.get("level2_ids", [])))
        append_history_row(WEEKLY_HISTORY_L3_OUT, weekly_state["week_id"], len(weekly_state.get("level3_ids", [])))
        log(f"New week detected ({weekly_state['week_id']} -> {wk_id}); archived last week's totals and reset counters.")
        weekly_state = {"week_id": wk_id, "level2_ids": [], "level3_ids": []}
    elif weekly_state.get("week_id") is None:
        weekly_state = {"week_id": wk_id, "level2_ids": [], "level3_ids": []}

    return weekly_state

# --------------- NEW: detail-report writers (weekly running totals) ---------------
def write_unreachable_new(merged_df: pd.DataFrame, net_series: pd.Series, weekly_state: dict):
    """Every Level 2 charger that has been UNREACHABLE at any point THIS WEEK so far
    (running total, resets each new week), with the date it first went down and
    whether it's still down right now."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    today_mask = (merged_df["Charger type"] == "Level 2") & net_series.eq("UNREACHABLE")
    today_ids = set(merged_df.loc[today_mask, "stationID"].dropna().astype(str))
    weekly_state["level2_ids"] = sorted(set(weekly_state.get("level2_ids", [])) | today_ids)

    cols = [
        "stationID", "stationName", "Address", "City", "State", "Charger type", "stationModel",
        "date_became_unreachable", "hours_unreachable",
    ]
    for c in cols:
        if c not in merged_df.columns:
            merged_df[c] = None

    week_ids = set(weekly_state["level2_ids"])
    detail = merged_df.loc[merged_df["stationID"].astype(str).isin(week_ids), cols].copy()
    detail = detail.drop_duplicates(subset=["stationID"], keep="last")
    detail["currently_unreachable"] = detail["stationID"].astype(str).isin(today_ids)
    detail.insert(0, "week", weekly_state["week_id"])
    detail.insert(0, "date", today)
    detail = detail.drop(columns=["stationID"])

    atomic_write_csv(detail, UNREACHABLE_NEW_OUT)
    log(f"Wrote {UNREACHABLE_NEW_OUT} — {len(detail):,} distinct Level 2 chargers unreachable so far this week ({weekly_state['week_id']})")

def write_pm_new(merged_df: pd.DataFrame, fr_series_upper: pd.Series, weekly_state: dict):
    """Every Level 3 charger that has shown a Power Module fault at any point THIS
    WEEK so far (running total, resets each new week)."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    keywords_upper = [kw.upper() for kw in POWER_MODULE_FAULT_KEYWORDS]
    kw_mask = fr_series_upper.apply(lambda x: any(kw in x for kw in keywords_upper))
    today_mask = (merged_df["Charger type"] == "Level 3") & kw_mask
    today_ids = set(merged_df.loc[today_mask, "stationID"].dropna().astype(str))
    weekly_state["level3_ids"] = sorted(set(weekly_state.get("level3_ids", [])) | today_ids)

    cols = ["stationID", "stationName", "Address", "City", "State", "Charger type", "stationModel", "faultReason"]
    for c in cols:
        if c not in merged_df.columns:
            merged_df[c] = None

    week_ids = set(weekly_state["level3_ids"])
    detail = merged_df.loc[merged_df["stationID"].astype(str).isin(week_ids), cols].copy()
    detail = detail.drop_duplicates(subset=["stationID"], keep="last")
    detail["currently_faulted"] = detail["stationID"].astype(str).isin(today_ids)
    detail.insert(0, "week", weekly_state["week_id"])
    detail.insert(0, "date", today)
    detail = detail.drop(columns=["stationID"])

    atomic_write_csv(detail, PM_NEW_OUT)
    log(f"Wrote {PM_NEW_OUT} — {len(detail):,} distinct Level 3 power module faults so far this week ({weekly_state['week_id']})")

# --------------- State + alert helpers ---------------
def load_state(path=STATE_PATH):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_state(state, path=STATE_PATH):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    os.replace(tmp, path)

def is_unreachable_row(row) -> bool:
    net = str(row.get("StationNetworkStatus") or "").strip().upper()
    return net == "UNREACHABLE"

def build_48h_alerts(df: pd.DataFrame, now_utc: datetime):
    state = load_state()
    alerts = []

    if df is None or df.empty or "stationID" not in df.columns:
        return state, alerts

    ts_series = pd.to_datetime(df.get("StatusTimestamp"), utc=True, errors="coerce")

    for idx, r in df.iterrows():
        sid = str(r.get("stationID") or "").strip()
        if not sid:
            continue

        was = state.get(sid, {
            "status": "UNKNOWN",
            "first_unreachable_iso": None,
            "last_seen_reachable_iso": None,
            "last_alert_iso": None,
            "unreachable_event_count": 0,
            "last_unreachable_start_iso": None
        })

        cur_unreach = is_unreachable_row(r)
        t_obs = ts_series.iloc[idx] if pd.notna(ts_series.iloc[idx]) else now_utc
        t_obs_iso = t_obs.astimezone(timezone.utc).isoformat()
        prev_status = (was.get("status") or "UNKNOWN").upper()

        if cur_unreach:
            if prev_status != "UNREACHABLE":
                was["unreachable_event_count"] = int(was.get("unreachable_event_count") or 0) + 1
                was["last_unreachable_start_iso"] = t_obs_iso

            if not was.get("first_unreachable_iso"):
                was["first_unreachable_iso"] = t_obs_iso

            was["status"] = "UNREACHABLE"

            since = datetime.fromisoformat(was["first_unreachable_iso"].replace("Z", "+00:00"))
            hours = (now_utc - since).total_seconds() / 3600.0
            if hours >= ALERT_THRESHOLD_HOURS:
                alerts.append({
                    "stationID": sid,
                    "stationName": r.get("stationName"),
                    "Address": r.get("Address"),
                    "City": r.get("City"),
                    "State": r.get("State"),
                    "charger_type": r.get("Charger type"),
                    "since_iso": since.isoformat(),
                    "hours_down": round(hours, 1)
                })
        else:
            was["status"] = "REACHABLE"
            was["last_seen_reachable_iso"] = t_obs_iso
            was["first_unreachable_iso"] = None

        state[sid] = was

    return state, alerts

# --------------- Main ---------------
def main():
    log(f"Working dir: {OUTPUT_DIR}")

    stations_df = fetch_stations_full_per_station()
    if stations_df is None or stations_df.empty:
        log("ERROR: No station metadata retrieved. Aborting.")
        return

    # Write stations CSV WITHOUT stationID
    stations_public = stations_df.drop(columns=["stationID"], errors="ignore")
    atomic_write_csv(stations_public, STATIONS_OUT)

    ids = stations_df["stationID"].dropna().unique().tolist()

    status_df = fetch_all_statuses(ids)
    load_df   = fetch_all_loads(ids)

    merged = stations_df
    if status_df is not None and not status_df.empty:
        merged = merged.merge(status_df, on=["stationID"], how="left")
    if load_df is not None and not load_df.empty:
        merged = merged.merge(load_df, on=["stationID"], how="left")

    for c in ["ports_total", "ports_charging", "ports_occupied", "ports_available"]:
        if c not in merged.columns:
            merged[c] = 0

    def compute_label(row):
        charger_type = (row.get("Charger type") or "").upper()
        model = (row.get("stationModel") or "").upper()
        avail = int(row.get("ports_available", 0) or 0)
        chg   = int(row.get("ports_charging", 0) or 0)
        occ   = int(row.get("ports_occupied", 0) or 0)

        is_dc  = ("LEVEL 3" in charger_type) or ("DC" in charger_type) or ("FAST" in charger_type)
        is_cpf = model.startswith("CPF")

        if is_dc or is_cpf:
            if chg > 0: return "Charging"
            if avail > 0: return "Available"
            if occ > 0: return "Occupied"
            return "Available"
        else:
            if avail > 0: return "Available"
            if chg > 0: return "Charging"
            if occ > 0: return "Occupied"
            return "Available"

    merged["station_label"] = merged.apply(compute_label, axis=1)

    def norm(x):
        if x is None: return ""
        try:
            if pd.isna(x): return ""
        except Exception:
            pass
        return str(x)

    fr  = merged.get("faultReason", pd.Series([""] * len(merged))).map(norm).str.strip().str.upper()
    lps = merged.get("LastPortStatus", pd.Series([""] * len(merged))).map(norm).str.strip().str.upper()
    net = merged.get("StationNetworkStatus", pd.Series([""] * len(merged))).map(norm).str.strip().str.upper()

    fault_mask = (
        lps.isin({"FAULTED","UNAVAILABLE","OUTOFORDER","MAINTENANCE"}) |
        net.eq("UNREACHABLE") |
        ((~fr.isin({"", "NONE", "N/A", "NULL"})) & lps.isin({"FAULTED","UNAVAILABLE","OUTOFORDER","MAINTENANCE"}))
    )
    merged.loc[fault_mask, "station_label"] = "Faulted"

    now_utc = datetime.now(timezone.utc)
    state, alerts = build_48h_alerts(merged, now_utc)
    save_state(state)

    with open(ALERTS_PATH, "w", encoding="utf-8") as f:
        json.dump(alerts, f, indent=2, ensure_ascii=False)
    log(f"Wrote {ALERTS_PATH} with {len(alerts)} alert(s)")

    counts_rows = []
    for sid, info in state.items():
        counts_rows.append({
            "stationID": sid,
            "unreachable_event_count": int(info.get("unreachable_event_count") or 0),
            "status": info.get("status"),
            "last_unreachable_start_iso": info.get("last_unreachable_start_iso"),
            "current_first_unreachable_iso": info.get("first_unreachable_iso"),
            "last_seen_reachable_iso": info.get("last_seen_reachable_iso"),
        })
    counts_df = pd.DataFrame(counts_rows)

    if stations_df is not None and not stations_df.empty and not counts_df.empty:
        add_cols = ["stationID", "stationName", "Address", "City", "State", "Charger type", "stationModel"]
        counts_df = counts_df.merge(stations_df[add_cols], on="stationID", how="left")

    preferred = [
        "stationName","Address","City","State","Charger type","stationModel",
        "unreachable_event_count","status",
        "last_unreachable_start_iso","current_first_unreachable_iso","last_seen_reachable_iso",
    ]
    for c in preferred:
        if c not in counts_df.columns:
            counts_df[c] = None
    counts_df = counts_df[preferred]
    atomic_write_csv(counts_df, UNREACH_COUNTS_OUT)
    log(f"Wrote {UNREACH_COUNTS_OUT} with {len(counts_df):,} rows")

    since_map = {sid: info.get("first_unreachable_iso") for sid, info in state.items() if info.get("first_unreachable_iso")}
    merged["hours_unreachable"] = 0.0
    since_series = merged["stationID"].map(since_map)
    since_dt = pd.to_datetime(since_series, utc=True, errors="coerce")

    unreachable_mask = net.eq("UNREACHABLE")
    hours = (now_utc - since_dt).dt.total_seconds() / 3600.0
    merged.loc[unreachable_mask & since_dt.notna(), "hours_unreachable"] = hours.round(1)

    # NEW: date each currently-unreachable station first went down, per persisted state
    merged["date_became_unreachable"] = None
    merged.loc[unreachable_mask & since_dt.notna(), "date_became_unreachable"] = (
        since_dt[unreachable_mask & since_dt.notna()].dt.strftime("%Y-%m-%d")
    )

    # NEW: roll over to a new week's counters if a new ISO week has started
    # (archives last week's final totals to the history CSVs first)
    weekly_state = roll_weekly_state(now_utc)

    # NEW: write the two requested detail reports (Level 2 unreachable / Level 3 power module)
    # — these accumulate distinct chargers hit THIS WEEK, resetting each new week
    write_unreachable_new(merged, net, weekly_state)
    write_pm_new(merged, fr, weekly_state)
    save_weekly_state(weekly_state)

    final_df = slim_output(merged)
    atomic_write_csv(final_df, STATUS_OUT)
    log(f"Wrote {STATUS_OUT} with {len(final_df):,} rows")

if __name__ == "__main__":
    main()
