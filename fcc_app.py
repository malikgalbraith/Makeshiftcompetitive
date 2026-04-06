import json
import re
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
import streamlit as st

FIND_URL = "https://publicfiles.fcc.gov/find/{prefix}/page-offset-0/order-call-sign-asc/filter-none/"
SEARCH_URL = "https://publicfiles.fcc.gov/api/manager/search/key/{keyword}.json?entityId={entity_id}"

st.set_page_config(page_title="Iowa FCC Political File Search", page_icon="📡", layout="wide")
st.title("📡 Iowa FCC Political File Search")
st.caption("Searches all Iowa TV, FM, and AM stations for matching documents in the FCC Public Inspection File.")

# ── Helpers ──────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False, ttl=3600)
def load_iowa_stations():
    """Fetch all Iowa stations (cached for 1 hour)."""
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; FCCSearch/1.0)"})

    stations = []
    seen_ids = set()

    for prefix in ["k", "w"]:
        url = FIND_URL.format(prefix=prefix)
        for attempt in range(3):
            try:
                resp = session.get(url, timeout=30)
                resp.raise_for_status()
                html = resp.text
                break
            except Exception:
                if attempt == 2:
                    html = ""
                time.sleep(2 * (attempt + 1))

        match = re.search(r"let results = (\{.*?\});", html, re.DOTALL)
        if not match:
            continue

        data = json.loads(match.group(1))
        entity = data.get("entity", {})

        for list_key in ["tvFacilityList", "fmFacilityList", "amFacilityList"]:
            for s in entity.get(list_key, []):
                if s.get("communityState") == "IA" and s["id"] not in seen_ids:
                    seen_ids.add(s["id"])
                    stations.append({
                        "id": s["id"],
                        "callSign": s.get("callSign", ""),
                        "service": s.get("service", ""),
                        "city": s.get("communityCity", ""),
                    })

    return stations


def search_station(session, station, keyword):
    """Search one station for keyword. Returns list of match dicts."""
    url = SEARCH_URL.format(
        keyword=urllib.parse.quote(keyword),
        entity_id=station["id"],
    )
    for attempt in range(3):
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            result = data.get("searchResult", {})
            matches = []
            for f in result.get("files", []):
                date_str = f.get("create_ts", "")[:10]
                if date_str and date_str < "2026-01-01":
                    continue
                matches.append({
                    "Station": station["callSign"],
                    "City": station["city"].title(),
                    "Service": station["service"],
                    "Type": "File",
                    "Document Name": f.get("file_name", ""),
                    "Folder Path": f.get("file_folder_path", ""),
                    "Extension": f.get("file_extension", "").upper(),
                    "Date Filed": date_str,
                    "Entity ID": station["id"],
                    "File ID": f.get("file_id", ""),
                    "PDF": f"https://publicfiles.fcc.gov/api/manager/download/{f.get('folder_id','')}/{f.get('file_manager_id','')}.pdf" if f.get("file_manager_id") else "",
                })
            for folder in result.get("folders", []):
                matches.append({
                    "Station": station["callSign"],
                    "City": station["city"].title(),
                    "Service": station["service"],
                    "Type": "Folder",
                    "Document Name": folder.get("folder_name", ""),
                    "Folder Path": "",
                    "Extension": "",
                    "Date Filed": "",
                    "Entity ID": station["id"],
                    "File ID": "",
                    "PDF": "",
                })
            return matches
        except Exception:
            if attempt == 2:
                return []
            time.sleep(2 * (attempt + 1))
    return []


def run_search(keyword, stations, progress_bar, status_text):
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; FCCSearch/1.0)"})

    all_matches = []
    completed = 0
    total = len(stations)

    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(search_station, session, s, keyword): s for s in stations}
        for future in as_completed(futures):
            completed += 1
            matches = future.result()
            all_matches.extend(matches)
            progress_bar.progress(completed / total)
            status_text.text(f"Checked {completed}/{total} stations — {len(all_matches)} match(es) found")

    return all_matches


# ── UI ────────────────────────────────────────────────────────────────────────

with st.form("search_form"):
    keyword = st.text_input(
        "Candidate name",
        placeholder="e.g. Lahn",
        help="Searches all Iowa station public files for documents containing this name.",
    )
    submitted = st.form_submit_button("Search Iowa Stations", type="primary", use_container_width=True)

if submitted and keyword.strip():
    keyword = keyword.strip()

    with st.spinner("Loading Iowa station list..."):
        stations = load_iowa_stations()

    if not stations:
        st.error("Could not load Iowa station list. Check your internet connection and try again.")
        st.stop()

    st.info(f"Searching **{len(stations)} Iowa stations** for **\"{keyword}\"**...")

    progress_bar = st.progress(0)
    status_text = st.empty()

    matches = run_search(keyword, stations, progress_bar, status_text)

    progress_bar.empty()
    status_text.empty()

    if not matches:
        st.warning(f"No documents found matching **\"{keyword}\"** in any Iowa station.")
    else:
        # Deduplicate (files can appear as both file and folder result)
        seen = set()
        deduped = []
        for m in matches:
            key = (m["Station"], m["File ID"] or m["Document Name"])
            if key not in seen:
                seen.add(key)
                deduped.append(m)

        files_only = [m for m in deduped if m["Type"] == "File"]
        st.success(f"Found **{len(files_only)} file(s)** across **{len(set(m['Station'] for m in files_only))} station(s)** matching \"{keyword}\"")

        df = pd.DataFrame(files_only)[["Station", "City", "Service", "Document Name", "Folder Path", "Extension", "Date Filed", "PDF"]]
        df = df.sort_values(["Station", "Document Name"]).reset_index(drop=True)

        st.dataframe(
            df,
            column_config={"PDF": st.column_config.LinkColumn("PDF", display_text="Open PDF")},
            use_container_width=True,
            hide_index=True,
        )

        csv = df.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=f"fcc_{keyword.replace(' ', '_')}_iowa.csv",
            mime="text/csv",
        )

elif submitted:
    st.warning("Please enter a candidate name.")
