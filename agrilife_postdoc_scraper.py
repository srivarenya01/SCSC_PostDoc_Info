"""
Scrape AgriLife People directory for postdoc contact details.

Workflow implemented from `Details.md`:
1. Fetch units index and parse unit rows from HTML.
2. Keep units whose "Supported Entities" includes "Agri" (case-insensitive).
3. Traverse each unit recursively to discover subunits and employee profile links.
4. Fetch employee profiles, keep profiles where title contains postdoc variants.
5. Fetch supervisor profile details and emit a CSV.

Usage:
    python agrilife_postdoc_scraper.py --output postdocs.csv

Dependencies:
    pip install requests beautifulsoup4
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import logging
import re
import shutil
import threading
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup


BASE_URL = "https://agrilifepeople.tamu.edu"
UNITS_URL = f"{BASE_URL}/units"
# Matches common postdoc title variants like:
# - Postdoc
# - Post Doc
# - Post-Doc
# - Postdoctoral
POSTDOC_PATTERN = re.compile(r"\bpost[\s-]*doc(?:toral)?\b", re.IGNORECASE)

# Words to strip from scraped names (social media links, credentials, etc.)
NAME_STRIP_WORDS = re.compile(
    r"\b(facebook|instagram|linkedin|twitter|youtube|website|cv|resume|profile|ph\.?d\.?|dr\.?)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class UnitRow:
    """Represents one row from the units index table."""

    name: str
    unit_url: str
    supported_entities: str


@dataclass(frozen=True)
class PersonRecord:
    """Normalized person record collected from a profile page."""

    first_name: str
    last_name: str
    person_url: str
    email: str
    phone: str
    title: str
    unit_name: str
    supervisor_name: str
    supervisor_url: str
    supervisor_phone: str


@dataclass(frozen=True)
class UnitPersonCandidate:
    """Person discovered on a unit page with unit-level role metadata."""

    person_url: str
    unit_position: str
    unit_name: str


class AgriLifeScraper:
    """
    Encapsulates crawling, parsing, filtering, and CSV writing.

    Design goals:
    - Minimize unnecessary person-page calls by using unit-page role/title first.
    - Traverse nested unit structures safely.
    - Use bounded concurrency for profile requests to improve runtime.
    - Keep output deterministic and easy to audit.
    """

    def __init__(
        self,
        request_timeout_seconds: int = 30,
        max_retries: int = 3,
        retry_backoff_seconds: float = 1.5,
        request_delay_seconds: float = 0.15,
        max_workers: int = 12,
    ) -> None:
        self.request_timeout_seconds = request_timeout_seconds
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self.request_delay_seconds = request_delay_seconds
        self.max_workers = max_workers
        self._thread_local = threading.local()
        self._field_cache_lock = threading.Lock()
        self._field_cache: Dict[str, Dict[str, str]] = {}

    def fetch_html(self, url: str) -> str:
        """
        Fetch page HTML with retry + linear backoff.

        Raises:
            RuntimeError: if all attempts fail.
        """
        last_error: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                session = self._get_session()
                response = session.get(url, timeout=self.request_timeout_seconds)
                response.raise_for_status()
                if self.request_delay_seconds > 0:
                    time.sleep(self.request_delay_seconds)
                return response.text
            except (requests.RequestException, requests.Timeout) as exc:
                last_error = exc
                logging.warning(
                    "Request failed (attempt %s/%s) for %s: %s",
                    attempt,
                    self.max_retries,
                    url,
                    exc,
                )
                if attempt < self.max_retries:
                    time.sleep(self.retry_backoff_seconds * attempt)
        raise RuntimeError(f"Failed to fetch URL after retries: {url}") from last_error

    def parse_units_index(self, html: str) -> List[UnitRow]:
        """
        Parse `/units` index table into normalized `UnitRow` records.

        Source table columns are expected to include:
        - Unit Name (with anchor to `/units/view/<id>`)
        - Supported Entities
        """
        soup = BeautifulSoup(html, "html.parser")
        rows: List[UnitRow] = []
        for tr in soup.select("#unitTable tbody tr"):
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue

            unit_anchor = tds[0].find("a", href=True)
            if not unit_anchor:
                continue

            unit_name = self._clean_text(unit_anchor.get_text(" ", strip=True))
            unit_url = self._to_absolute_url(unit_anchor["href"])
            supported_entities = self._clean_text(tds[1].get_text(" ", strip=True))

            if unit_name and unit_url:
                rows.append(UnitRow(unit_name, unit_url, supported_entities))
        return rows

    def filter_agri_units(self, units: Sequence[UnitRow]) -> List[UnitRow]:
        """Return only rows where Supported Entities includes 'agri'."""
        return [u for u in units if "agri" in u.supported_entities.lower()]

    def crawl_units_for_people(self, seed_units: Sequence[UnitRow]) -> Dict[str, UnitPersonCandidate]:
        """
        Traverse units/subunits and return people discovered from unit listing rows.

        For each unit page:
        - Subunits: `/units/view/<id>`
        - People rows in employee tables (capture person link + unit Position column)
        """
        # BFS traversal across unit/subunit pages.
        visited_units: Set[str] = set()
        discovered_people: Dict[str, UnitPersonCandidate] = {}
        queue: List[Tuple[str, str]] = [(unit.unit_url, unit.name) for unit in seed_units]

        while queue:
            current_unit_url, current_unit_name = queue.pop(0)
            if current_unit_url in visited_units:
                continue
            visited_units.add(current_unit_url)

            try:
                html = self.fetch_html(current_unit_url)
            except RuntimeError:
                logging.exception("Skipping unit due to repeated request failure: %s", current_unit_url)
                continue

            # Parse this unit page once: collect nested units and candidate people rows.
            subunits, candidates = self._extract_unit_links_and_candidates(
                current_unit_url,
                current_unit_name,
                html,
            )
            for candidate in candidates:
                # Keep first-seen mapping for each person URL (stable + deterministic).
                if candidate.person_url not in discovered_people:
                    discovered_people[candidate.person_url] = candidate

            for subunit_url, subunit_name in subunits:
                if subunit_url not in visited_units:
                    queue.append((subunit_url, subunit_name))

        logging.info(
            "Traversal complete. Visited %s unit pages and found %s people candidates.",
            len(visited_units),
            len(discovered_people),
        )
        return discovered_people

    def build_postdoc_records(self, people: Dict[str, UnitPersonCandidate]) -> List[PersonRecord]:
        """
        Build final postdoc records.

        Optimization:
        - Use unit-page position/title first.
        - Fetch profile page only for unit candidates matching postdoc title pattern.
        """
        records: List[PersonRecord] = []
        supervisor_phone_cache: Dict[str, str] = {}
        supervisor_cache_lock = threading.Lock()

        postdoc_candidates: List[Tuple[str, UnitPersonCandidate]] = []
        for person_url in sorted(people.keys()):
            candidate = people[person_url]
            unit_position = self._clean_text(candidate.unit_position)
            if POSTDOC_PATTERN.search(unit_position):
                postdoc_candidates.append((person_url, candidate))

        def process_candidate(item: Tuple[str, UnitPersonCandidate]) -> Optional[PersonRecord]:
            """Worker: fetch person/supervisor details and build one output record."""
            person_url, candidate = item
            unit_position = self._clean_text(candidate.unit_position)

            fields = self._get_person_fields(person_url)
            if not fields:
                return None

            raw_name = self._pick_first_non_empty(fields, ["name", "display name"])
            # Strip social media / credential words from the name
            clean_name = NAME_STRIP_WORDS.sub("", raw_name)
            clean_name = self._clean_text(clean_name)
            # Split into first_name (all but last token) and last_name (last token)
            name_parts = clean_name.split()
            if len(name_parts) >= 2:
                first_name = " ".join(name_parts[:-1])
                last_name = name_parts[-1]
            else:
                first_name = clean_name
                last_name = ""

            email = self._pick_first_non_empty(fields, ["email address", "email"])
            phone = self._pick_first_non_empty(fields, ["phone number", "office phone", "phone"])
            title = self._pick_first_non_empty(fields, ["title", "position", "job title"])
            if not title:
                title = unit_position
            supervisor_name = self._pick_first_non_empty(fields, ["immediate supervisor", "supervisor"])
            supervisor_url = fields.get("_supervisor_url", "")

            supervisor_phone = ""
            if supervisor_url:
                # Thread-safe read through shared supervisor cache.
                with supervisor_cache_lock:
                    supervisor_phone = supervisor_phone_cache.get(supervisor_url, "")
                if not supervisor_phone:
                    supervisor_fields = self._get_person_fields(supervisor_url)
                    supervisor_phone = self._pick_first_non_empty(
                        supervisor_fields or {},
                        ["phone number", "office phone", "phone"],
                    )
                    with supervisor_cache_lock:
                        supervisor_phone_cache[supervisor_url] = supervisor_phone

            record = PersonRecord(
                first_name=first_name,
                last_name=last_name,
                person_url=person_url,
                email=email,
                phone=phone,
                title=title,
                unit_name=candidate.unit_name,
                supervisor_name=supervisor_name,
                supervisor_url=supervisor_url,
                supervisor_phone=supervisor_phone,
            )
            logging.info(
                "Extracted postdoc: %s %s | %s",
                first_name or "(unknown)", last_name, person_url,
            )
            return record

        # Parallelize profile fetches to reduce wall-clock runtime.
        # `executor.map` keeps result ordering aligned with `postdoc_candidates`.
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for record in executor.map(process_candidate, postdoc_candidates):
                if record is not None:
                    records.append(record)

        return records

    def write_csv(self, records: Sequence[PersonRecord], output_csv_path: str) -> None:
        """
        Write normalized records to CSV.

        The caller is responsible for ordering records before writing.
        """
        fieldnames = [
            "first_name",
            "last_name",
            "email",
            "phone",
            "title",
            "unit_name",
            "person_url",
            "supervisor_name",
            "supervisor_url",
            "supervisor_phone",
        ]
        with open(output_csv_path, "w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for record in records:
                writer.writerow(
                    {
                        "first_name": record.first_name,
                        "last_name": record.last_name,
                        "email": record.email,
                        "phone": record.phone,
                        "title": record.title,
                        "unit_name": record.unit_name,
                        "person_url": record.person_url,
                        "supervisor_name": record.supervisor_name,
                        "supervisor_url": record.supervisor_url,
                        "supervisor_phone": record.supervisor_phone,
                    }
                )

    def _extract_unit_links_and_candidates(
        self, unit_url: str, unit_name: str, html: str
    ) -> Tuple[List[Tuple[str, str]], List[UnitPersonCandidate]]:
        """
        Extract from a single unit page:
        1) Subunit links for recursion.
        2) People rows from tables, including unit-level position text.

        Returns:
            (subunits, candidates)
            - subunits: list of (subunit_url, subunit_name)
            - candidates: person URLs + unit metadata
        """
        soup = BeautifulSoup(html, "html.parser")
        subunits: Dict[str, str] = {}
        candidates: List[UnitPersonCandidate] = []

        # First pass: gather subunit anchors.
        for anchor in soup.find_all("a", href=True):
            href = anchor["href"].strip()
            absolute = self._to_absolute_url(href)
            path = urlparse(absolute).path

            if re.fullmatch(r"/units/view/\d+", path) and absolute != unit_url:
                name = self._clean_text(anchor.get_text(" ", strip=True))
                if not name:
                    name = absolute
                subunits[absolute] = name

        # Second pass: gather people rows from employee listing tables.
        # Typical columns are: Name | Position | Contact | Location
        for tr in soup.find_all("tr"):
            person_anchor = tr.find("a", href=re.compile(r"^/people/view/\d+$"))
            if not person_anchor:
                continue
            person_url = self._to_absolute_url(person_anchor["href"])
            tds = tr.find_all("td")
            unit_position = ""
            if len(tds) >= 2:
                unit_position = self._clean_text(tds[1].get_text(" ", strip=True))
            if not unit_position:
                unit_position = self._clean_text(tr.get_text(" ", strip=True))
            candidates.append(
                UnitPersonCandidate(
                    person_url=person_url,
                    unit_position=unit_position,
                    unit_name=unit_name,
                )
            )

        return list(subunits.items()), candidates

    def _get_person_fields(self, person_url: str) -> Optional[Dict[str, str]]:
        """
        Parse a person profile page into a dictionary of normalized fields.

        Example extracted keys include:
        - name
        - email address
        - phone number
        - title
        - immediate supervisor
        - _supervisor_url (internal helper key)
        """
        with self._field_cache_lock:
            if person_url in self._field_cache:
                return self._field_cache[person_url]

        try:
            html = self.fetch_html(person_url)
        except RuntimeError:
            logging.exception("Skipping person due to repeated request failure: %s", person_url)
            return None

        soup = BeautifulSoup(html, "html.parser")
        fields: Dict[str, str] = {}

        heading = soup.select_one("h1")
        if heading:
            fields["name"] = self._clean_text(heading.get_text(" ", strip=True))
        elif soup.title and soup.title.text:
            fields["name"] = self._clean_text(soup.title.text.split("|")[0].strip())

        for label_div in soup.select("div.details-label"):
            raw_label = self._clean_text(label_div.get_text(" ", strip=True)).rstrip(":")
            key = raw_label.lower()
            row = label_div.find_parent("div", class_=lambda x: x and "row" in x)
            if not row:
                continue

            value_col = row.find("div", class_=lambda x: x and "col" in x)
            if not value_col:
                continue

            value_text = self._clean_text(value_col.get_text(" ", strip=True))
            if value_text:
                fields[key] = value_text

            if key in {"immediate supervisor", "supervisor"}:
                supervisor_anchor = value_col.find("a", href=True)
                if supervisor_anchor:
                    fields["_supervisor_url"] = self._to_absolute_url(supervisor_anchor["href"])

        with self._field_cache_lock:
            self._field_cache[person_url] = fields
        return fields

    def _get_session(self) -> requests.Session:
        """
        Return a per-thread `requests.Session`.

        A dedicated session per worker thread avoids cross-thread session sharing
        and still preserves HTTP connection pooling within that thread.
        """
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update(
                {
                    "User-Agent": (
                        "Mozilla/5.0 (compatible; AgriLifePostdocScraper/1.0; "
                        "+https://agrilifepeople.tamu.edu)"
                    )
                }
            )
            self._thread_local.session = session
        return session

    @staticmethod
    def _pick_first_non_empty(fields: Dict[str, str], keys: Sequence[str]) -> str:
        """Pick the first non-empty field value by preferred key order."""
        for key in keys:
            value = fields.get(key, "").strip()
            if value:
                return value
        return ""

    @staticmethod
    def _clean_text(value: str) -> str:
        """Normalize repeated whitespace and trim."""
        return re.sub(r"\s+", " ", value or "").strip()

    @staticmethod
    def _to_absolute_url(raw_url: str) -> str:
        """Convert relative path to absolute URL under the AgriLife domain."""
        return urljoin(BASE_URL, raw_url)


def _normalize_name(name: object) -> str:
    """Lower-case, collapse whitespace, handle NaN/None safely."""
    if not isinstance(name, str):
        return ""
    return re.sub(r"\s+", " ", name.strip()).lower()


def _url_last_id(url: str) -> int:
    """Extract the trailing numeric ID from a person URL for sorting."""
    try:
        return int(str(url).rstrip("/").rsplit("/", 1)[-1])
    except (ValueError, AttributeError):
        return -1


def build_scsc_csv(
    csv_path: str,
    faculty_xlsx: str,
    out_csv: str,
    past_dir: str,
) -> None:
    """
    Post-processing step:
    1. Load previous SCSC output to carry forward first_seen_date values.
    2. Archive the full postdoc CSV to PastResults/ with a date stamp.
    3. Load SCSC faculty names from *faculty_xlsx* (local path or Google Sheets URL).
    4. Filter postdocs to those supervised by a SCSC faculty member.
    5. Assign first_seen_date: carry forward existing dates, assign today for new entries.
    6. Write all scraper columns + first_seen_date to *out_csv*,
       sorted in reverse order by the numeric ID at the end of person_url.
    """
    past = Path(past_dir)
    past.mkdir(exist_ok=True)
    today_str = date.today().strftime("%Y-%m-%d")

    # -- Load previous SCSC output to carry forward first_seen_date -------------
    # We look for the previous scsc output CSV (same path) to read dates.
    prev_first_seen: Dict[str, str] = {}
    if Path(out_csv).exists():
        try:
            prev_scsc = pd.read_csv(out_csv, usecols=["person_url", "first_seen_date"])
            for _, row in prev_scsc.iterrows():
                url = str(row["person_url"])
                dt = str(row.get("first_seen_date", "")).strip()
                if url and dt and dt != "nan":
                    prev_first_seen[url] = dt
            logging.info(
                "Loaded %s first_seen_date values from previous SCSC output.",
                len(prev_first_seen),
            )
        except Exception:
            logging.warning("Could not read previous SCSC output %s; all rows will get today's date.", out_csv)

    # -- Archive current CSV ----------------------------------------------------
    stamp = date.today().strftime("%Y%m%d")
    archive = past / f"agrilife_postdocs_{stamp}.csv"
    shutil.copy2(csv_path, archive)
    logging.info("Archived full CSV -> %s", archive)

    # -- Load data --------------------------------------------------------------
    postdocs_df = pd.read_csv(csv_path)

    # Migrate old 'name' column to 'first_name'/'last_name' if needed
    if "name" in postdocs_df.columns and "first_name" not in postdocs_df.columns:
        def _split_name(full_name: object):
            raw = str(full_name).strip() if isinstance(full_name, str) else ""
            clean = re.sub(r"\s+", " ", NAME_STRIP_WORDS.sub("", raw)).strip()
            parts = clean.split()
            if len(parts) >= 2:
                return " ".join(parts[:-1]), parts[-1]
            return (clean, "")
        postdocs_df[["first_name", "last_name"]] = pd.DataFrame(
            postdocs_df["name"].apply(_split_name).tolist(),
            index=postdocs_df.index,
        )
        logging.info("Migrated legacy 'name' column to 'first_name'/'last_name'.")

    # Convert Google Sheets edit/view URL to direct export URL if applicable
    resolved_faculty_xlsx = faculty_xlsx
    if "docs.google.com/spreadsheets" in faculty_xlsx:
        match = re.match(r"(https://docs\.google\.com/spreadsheets/d/[^/]+)", faculty_xlsx)
        if match:
            resolved_faculty_xlsx = match.group(1) + "/export?format=xlsx"
            logging.info("Converting Google Sheets URL to export URL: %s", resolved_faculty_xlsx)

    faculty_df = pd.read_excel(resolved_faculty_xlsx)

    # Save a local backup if fetched from Google Sheets
    if resolved_faculty_xlsx != faculty_xlsx:
        local_backup = "TAMU_SCSC_Faculty.xlsx"
        try:
            faculty_df.to_excel(local_backup, index=False)
            logging.info("Saved local backup of faculty list to %s", local_backup)
        except Exception as e:
            logging.warning("Could not save local backup to %s: %s", local_backup, e)

    logging.info(
        "Loaded %s postdoc rows and %s faculty rows.",
        len(postdocs_df), len(faculty_df),
    )

    # -- Build normalised faculty name set --------------------------------------
    faculty_norm: set[str] = {
        _normalize_name(n) for n in faculty_df["Name"] if _normalize_name(n)
    }

    # -- Filter -----------------------------------------------------------------
    postdocs_df["_sup_norm"] = postdocs_df["supervisor_name"].apply(_normalize_name)
    filtered = postdocs_df[postdocs_df["_sup_norm"].isin(faculty_norm)].copy()
    logging.info("%s postdocs matched a SCSC faculty supervisor.", len(filtered))

    # -- Assign first_seen_date -------------------------------------------------
    filtered["first_seen_date"] = filtered["person_url"].apply(
        lambda u: prev_first_seen.get(str(u), today_str)
    )
    new_count = filtered["person_url"].apply(lambda u: str(u) not in prev_first_seen).sum()
    logging.info(
        "%s newly seen postdocs (assigned today's date), %s returning.",
        new_count, len(filtered) - new_count,
    )

    # -- All scraper cols + first_seen_date, sorted reverse by URL trailing ID --
    all_cols = [
        "first_name", "last_name", "email", "phone", "title", "unit_name",
        "person_url", "supervisor_name", "supervisor_url", "supervisor_phone",
        "first_seen_date",
    ]
    out = filtered[all_cols].copy()
    out["_sort_key"] = out["person_url"].apply(_url_last_id)
    out = out.sort_values("_sort_key", ascending=False).drop(columns=["_sort_key"])
    out = out.reset_index(drop=True)

    # -- Write CSV -------------------------------------------------------------
    out.to_csv(out_csv, index=False)
    logging.info("Wrote %s rows to %s", len(out), out_csv)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Scrape AgriLife postdoc contacts and supervisor details."
    )
    parser.add_argument(
        "--output",
        default="agrilife_postdocs.csv",
        help="Output CSV file path (default: agrilife_postdocs.csv).",
    )
    parser.add_argument(
        "--faculty-xlsx",
        default="https://docs.google.com/spreadsheets/d/1FB-gll0kXQnqY_miXPSSGMEoVQJH0WxM/edit?usp=sharing&ouid=102587337650408258618&rtpof=true&sd=true",
        help="SCSC faculty Excel file or Google Sheets URL (default: https://docs.google.com/spreadsheets/d/1FB-gll0kXQnqY_miXPSSGMEoVQJH0WxM/edit?usp=sharing&ouid=102587337650408258618&rtpof=true&sd=true).",
    )
    parser.add_argument(
        "--scsc-output",
        default="scsc_postdocs.csv",
        help="Output CSV for SCSC-filtered postdocs (default: scsc_postdocs.csv).",
    )
    parser.add_argument(
        "--past-dir",
        default="PastResults",
        help="Directory to archive full CSV results (default: PastResults).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO).",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=12,
        help="Thread workers for person/supervisor fetching (default: 12).",
    )
    return parser.parse_args()


def main() -> int:
    """
    Execute full scrape pipeline:
    units index -> Agri filtering -> recursive unit crawl -> postdoc extraction -> CSV
    -> archive to PastResults/ -> SCSC-filtered CSV.
    """
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    scraper = AgriLifeScraper(max_workers=args.max_workers)
    logging.info("Fetching units index: %s", UNITS_URL)
    units_html = scraper.fetch_html(UNITS_URL)

    all_units = scraper.parse_units_index(units_html)
    agri_units = scraper.filter_agri_units(all_units)
    logging.info(
        "Parsed %s units, %s matched Agri supported entities.",
        len(all_units), len(agri_units),
    )
    people = scraper.crawl_units_for_people(agri_units)

    records = scraper.build_postdoc_records(people)
    records = sorted(
        records,
        key=lambda r: ((r.last_name or r.first_name).lower(), r.person_url.lower()),
    )
    scraper.write_csv(records, args.output)
    logging.info("Wrote %s postdoc records to %s", len(records), args.output)

    # Post-processing: archive full CSV and produce SCSC-filtered CSV.
    build_scsc_csv(
        csv_path=args.output,
        faculty_xlsx=args.faculty_xlsx,
        out_csv=args.scsc_output,
        past_dir=args.past_dir,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
