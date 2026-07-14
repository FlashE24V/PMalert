#!/usr/bin/env python3
"""
discover_fault_reasons.py

One-off diagnostic: queries getStations + getStationStatus for your whole
fleet and prints every DISTINCT faultReason string it finds (with counts and
an example station), broken out by Level (2 / 3 / other).

Run this once to see the *actual* wording your ChargePoint account uses for
things like power module failures, so the keyword match in
fleet_map_updater.py (POWER_MODULE_FAULT_KEYWORDS) can be set correctly.

Usage:
    export CP_USERNAME="your_api_license_key"
    export CP_PASSWORD="your_api_password"
    pip install requests
    python3 discover_fault_reasons.py
"""

import os
import sys
from collections import defaultdict
import xml.etree.ElementTree as ET

import requests

USERNAME = os.getenv("CP_USERNAME")
PASSWORD = os.getenv("CP_PASSWORD")
if not USERNAME or not PASSWORD:
    print("ERROR: Set CP_USERNAME and CP_PASSWORD environment variables first.")
    sys.exit(1)

ENDPOINT = "https://webservices.chargepoint.com/webservices/chargepoint/services/5.1"
PAGE_SIZE = 500


def strip_tag(tag: str) -> str:
    return tag.split("}")[-1] if "}" in tag else tag


def soap_call(body_xml: str) -> bytes:
    envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:urn="urn:dictionary:com.chargepoint.webservices">
  <soapenv:Header xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
    <wsse:Security soapenv:mustUnderstand="1">
      <wsse:UsernameToken>
        <wsse:Username>{USERNAME}</wsse:Username>
        <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">{PASSWORD}</wsse:Password>
      </wsse:UsernameToken>
    </wsse:Security>
  </soapenv:Header>
  <soapenv:Body>{body_xml}</soapenv:Body>
</soapenv:Envelope>"""
    resp = requests.post(
        ENDPOINT,
        data=envelope.encode("utf-8"),
        headers={"Content-Type": "text/xml; charset=utf-8"},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.content


def get_all_stations():
    """Return list of dicts: {stationID, Level, stationName}"""
    stations = []
    start = 1
    while True:
        body = f"""<urn:getStations>
      <searchQuery>
        <startRecord>{start}</startRecord>
        <numStations>{PAGE_SIZE}</numStations>
      </searchQuery>
    </urn:getStations>"""
        root = ET.fromstring(soap_call(body))
        found_any = False
        for st in root.iter():
            if strip_tag(st.tag) == "stationData":
                found_any = True
                sid = st.findtext(".//stationID") or st.findtext("stationID")
                level = st.findtext(".//Level") or st.findtext("Level")
                name = st.findtext(".//stationName") or st.findtext("stationName")
                stations.append({"stationID": sid, "Level": level, "stationName": name})
        more = root.findtext(".//moreFlag") or "0"
        if not found_any or more != "1":
            break
        start += PAGE_SIZE
    return stations


def get_station_status(station_ids_batch):
    id_tags = "".join(f"<stationIDs>{sid}</stationIDs>" for sid in station_ids_batch)
    body = f"""<urn:getStationStatus>
      <searchQuery>
        {id_tags}
      </searchQuery>
    </urn:getStationStatus>"""
    root = ET.fromstring(soap_call(body))
    results = []
    for st in root.iter():
        if strip_tag(st.tag) == "stationData":
            sid = st.findtext(".//stationID") or st.findtext("stationID")
            for p in st.findall(".//Port") or st.findall("Port"):
                fault = (p.findtext("faultReason") or "NONE").strip()
                results.append({"stationID": sid, "faultReason": fault})
    return results


def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def main():
    print("Fetching all stations (this can take a bit for large fleets)...")
    stations = get_all_stations()
    print(f"Found {len(stations)} stations.")

    level_by_id = {s["stationID"]: (s["Level"] or "Unknown") for s in stations}
    name_by_id = {s["stationID"]: (s["stationName"] or "") for s in stations}
    ids = [s["stationID"] for s in stations if s["stationID"]]

    # fault_counts[level][faultReason] = {"count": n, "example": stationName}
    fault_counts = defaultdict(lambda: defaultdict(lambda: {"count": 0, "example": ""}))

    print("Fetching station status in batches of 100...")
    for batch in chunked(ids, 100):
        for row in get_station_status(batch):
            sid = row["stationID"]
            fault = row["faultReason"]
            if not fault or fault.upper() == "NONE":
                continue
            level = level_by_id.get(sid, "Unknown")
            bucket = fault_counts[level][fault]
            bucket["count"] += 1
            if not bucket["example"]:
                bucket["example"] = name_by_id.get(sid, sid)

    print("\n===== Distinct faultReason values by station Level =====\n")
    if not fault_counts:
        print("No non-NONE faultReason values found across your fleet right now.")
        return

    for level in sorted(fault_counts.keys(), key=str):
        print(f"--- Level {level} ---")
        for fault, info in sorted(fault_counts[level].items(), key=lambda kv: -kv[1]["count"]):
            print(f"  [{info['count']:>4}]  {fault!r}   (e.g. {info['example']})")
        print()


if __name__ == "__main__":
    main()
