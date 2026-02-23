from __future__ import annotations

import csv
import json
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from dotenv import load_dotenv

KEEPA_EPOCH = datetime(2011, 1, 1, 0, 0, 0)


def datetime_to_keepa_minutes(dt: datetime) -> int:
    """Convert datetime to Keepa minutes (minutes since Jan 1, 2011)."""
    return int((dt - KEEPA_EPOCH).total_seconds() / 60)


@dataclass(frozen=True)
class Config:
    api_key: str
    domain: int
    timeout_s: int

    criteria_file: str
    output_csv: str
    dedupe_db: str
    checkpoint_file: str

    reset: bool
    resume: bool

    min_request_interval_s: float
    per_page: int
    max_results_per_query: int
    max_http_retries: int

    start_year: int
    window_months: int

    @staticmethod
    def load() -> "Config":
        # Use absolute paths for cron compatibility
        base_dir = "/home/azureuser/projects/keepa-data/asin-finder"
        load_dotenv("/home/azureuser/projects/keepa-data/.env")

        api_key = os.getenv("KEEPA_API", "").strip()
        if not api_key:
            raise ValueError("KEEPA_API not found in .env")

        reset = os.getenv("RESET_RUN", "0").strip() in {"1", "true", "True"}
        resume = os.getenv("RESUME", "1").strip() not in {"0", "false", "False"}
        if reset:
            resume = False

        output_csv = os.getenv("OUTPUT_CSV", f"{base_dir}/asinFinderOutput.csv")

        return Config(
            api_key=api_key,
            domain=int(os.getenv("KEEPA_DOMAIN", "1")),
            timeout_s=int(os.getenv("KEEPA_TIMEOUT_S", "60")),
            criteria_file=os.getenv("CRITERIA_FILE", f"{base_dir}/criteria.txt"),
            output_csv=output_csv,
            dedupe_db=os.getenv("DEDUPE_DB", f"{output_csv}.sqlite"),
            checkpoint_file=os.getenv("CHECKPOINT_FILE", f"{output_csv}.checkpoint.json"),
            reset=reset,
            resume=resume,
            min_request_interval_s=float(os.getenv("MIN_REQUEST_INTERVAL_S", "0.15")),
            per_page=max(50, int(os.getenv("PER_PAGE", "100"))),
            max_results_per_query=10000,
            max_http_retries=int(os.getenv("MAX_HTTP_RETRIES", "10")),
            start_year=int(os.getenv("START_YEAR", "2000")),
            window_months=max(1, int(os.getenv("WINDOW_MONTHS", "3"))),
        )


class KeepaClient:
    """Keepa /query client with retries + token waiting."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.session = requests.Session()
        self._last_request_ts = 0.0

    def _rate_limit_sleep(self) -> None:
        if self.cfg.min_request_interval_s <= 0:
            return
        elapsed = time.time() - self._last_request_ts
        if elapsed < self.cfg.min_request_interval_s:
            time.sleep(self.cfg.min_request_interval_s - elapsed)
        self._last_request_ts = time.time()

    def _request_with_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        for attempt in range(self.cfg.max_http_retries + 1):
            self._rate_limit_sleep()
            try:
                r = self.session.request(method, url, **kwargs)

                if r.status_code == 200:
                    return r

                if 400 <= r.status_code < 500 and r.status_code != 429:
                    if r.status_code == 400:
                        try:
                            print(f"  ❌ Keepa 400: {r.json()}")
                        except Exception:
                            print(f"  ❌ Keepa 400: {r.text[:200]}")
                    r.raise_for_status()

                if r.status_code in {429, 500, 502, 503, 504} and attempt < self.cfg.max_http_retries:
                    wait_s = min(60, 2**attempt)
                    print(f"  ⚠️  HTTP {r.status_code}, retrying in {wait_s}s")
                    time.sleep(wait_s)
                    continue

                r.raise_for_status()

            except requests.RequestException as e:
                if attempt < self.cfg.max_http_retries:
                    wait_s = min(60, 2**attempt)
                    print(f"  ⚠️  Request error: {e}, retrying in {wait_s}s")
                    time.sleep(wait_s)
                    continue
                raise

        raise RuntimeError(f"Failed after {self.cfg.max_http_retries} retries")

    def token_status(self) -> Dict[str, Any]:
        r = self._request_with_retry(
            "GET",
            "https://api.keepa.com/token",
            params={"key": self.cfg.api_key},
            timeout=10,
        )
        return r.json()

    def wait_for_tokens(self, needed: int = 10) -> None:
        while True:
            st = self.token_status()
            left = int(st.get("tokensLeft", 0))
            if left >= needed:
                return
            refill_ms = int(st.get("refillIn", 60000))
            sleep_s = max(5, refill_ms / 1000)
            print(f"  ⏳ Waiting for tokens {left}/{needed}, sleeping {sleep_s:.0f}s")
            time.sleep(sleep_s)

    def query(self, selection: Dict[str, Any]) -> Dict[str, Any]:
        r = self._request_with_retry(
            "POST",
            "https://api.keepa.com/query",
            params={"domain": self.cfg.domain, "key": self.cfg.api_key},
            data={"selection": json.dumps(selection)},
            timeout=self.cfg.timeout_s,
        )
        data = r.json()
        if data.get("status") not in {None, 200}:
            raise RuntimeError(f"Keepa API error: {data.get('status')} - {data.get('message')}")
        return data


class DedupeTracker:
    """SQLite-based ASIN dedupe."""

    def __init__(self, db_path: str) -> None:
        self.conn = sqlite3.connect(db_path, timeout=30)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("CREATE TABLE IF NOT EXISTS asins (asin TEXT PRIMARY KEY)")
        self.conn.commit()

    def is_new(self, asin: str) -> bool:
        try:
            self.conn.execute("INSERT INTO asins(asin) VALUES (?)", (asin,))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def count(self) -> int:
        cur = self.conn.execute("SELECT COUNT(*) FROM asins")
        return int(cur.fetchone()[0])

    def close(self) -> None:
        self.conn.close()


class Checkpoint:
    """Checkpoint for resume."""

    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.data = self._load()

    def _load(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {"mode": None, "identifier_index": 0, "current_date": None}
        try:
            return json.loads(self.path.read_text())
        except Exception:
            return {"mode": None, "identifier_index": 0, "current_date": None}

    def save(
        self,
        *,
        mode: Optional[str] = None,
        identifier_index: Optional[int] = None,
        current_date: Optional[str] = None,
    ) -> None:
        if mode is not None:
            self.data["mode"] = mode
        if identifier_index is not None:
            self.data["identifier_index"] = int(identifier_index)
        if current_date is not None:
            self.data["current_date"] = current_date
        self.data["updated_at"] = time.time()

        tmp = Path(str(self.path) + ".tmp")
        tmp.write_text(json.dumps(self.data, indent=2))
        tmp.replace(self.path)

    def identifier_index(self) -> int:
        return int(self.data.get("identifier_index", 0))

    def current_date(self) -> Optional[str]:
        return self.data.get("current_date")


class CSVWriter:
    """Append rows to CSV."""

    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self._initialized = self.path.exists() and self.path.stat().st_size > 0
        self.fieldnames: Optional[List[str]] = None

    def write_rows(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return

        if self.fieldnames is None:
            keys = set()
            for r in rows:
                keys.update(r.keys())
            keys.discard("asin")
            self.fieldnames = ["asin"] + sorted(keys)

        mode = "a" if self._initialized else "w"
        with open(self.path, mode, newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=self.fieldnames, extrasaction="ignore")
            if not self._initialized:
                w.writeheader()
                self._initialized = True
            w.writerows(rows)


class DateWindowGenerator:
    """Generate rolling windows for trackingSince."""

    def __init__(self, start_year: int, window_months: int) -> None:
        self.start_year = start_year
        self.window_months = max(1, window_months)

    def generate(self, start_from: Optional[str] = None) -> Iterable[Tuple[datetime, datetime, str]]:
        cur = datetime(self.start_year, 1, 1)
        today = datetime.now()

        if start_from:
            try:
                cur = datetime.strptime(start_from, "%Y-%m-%d")
            except Exception:
                pass

        while cur < today:
            nm = cur.month + self.window_months
            ny = cur.year
            while nm > 12:
                nm -= 12
                ny += 1
            end = datetime(ny, nm, 1)
            if end > today:
                end = today
            label = f"{cur.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}"
            yield cur, end, label
            cur = end


KNOWN_KEYS = {
    "upc",
    "ean",
    "model",
    "partNumber",
    "title",
    "brand",
    "manufacturer",
    "category",
    "rootCategory",
    "trackingSince_gte",
    "trackingSince_lte",
    "perPage",
    "page",
    "sort",
}


def _add_value(criteria: Dict[str, Any], key: str, value: str) -> None:
    value = value.strip().strip('"').strip("'")
    if value == "":
        return

    existing = criteria.get(key)
    if existing is None:
        if key in {"upc", "ean", "model", "partNumber"}:
            criteria[key] = [value]
        else:
            criteria[key] = value
        return

    if isinstance(existing, list):
        existing.append(value)
        return

    criteria[key] = [str(existing), value]


def parse_criteria_txt(path: str) -> Dict[str, Any]:
    """
    Supports:
      - Section headers:
          upc
          123
          456
      - Inline:
          upc=123
          brand: Ford
      - Comma lists:
          upc=123,456
      - Bare values (no section): treated as UPC values
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Criteria file not found: {path}")

    criteria: Dict[str, Any] = {}
    current_key: Optional[str] = None

    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue

        m = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s*(=|:)\s*(.+)$", line)
        if m:
            k = m.group(1).strip()
            v = m.group(3).strip()
            current_key = k if k in KNOWN_KEYS else None

            if k == "sort":
                # Accept: sort=trackingSince,asc   OR   sort=[["trackingSince","asc"]]
                if v.startswith("["):
                    try:
                        criteria["sort"] = json.loads(v)
                    except Exception:
                        pass
                else:
                    parts = [x.strip() for x in v.split(",") if x.strip()]
                    if len(parts) == 2:
                        criteria["sort"] = [[parts[0], parts[1]]]
                continue

            if k in {"perPage", "page", "trackingSince_gte", "trackingSince_lte"}:
                try:
                    criteria[k] = int(v)
                except Exception:
                    criteria[k] = v
                continue

            if "," in v and k in {"upc", "ean", "model", "partNumber"}:
                for part in [x.strip() for x in v.split(",")]:
                    _add_value(criteria, k, part)
            else:
                _add_value(criteria, k, v)
            continue

        if line in KNOWN_KEYS:
            current_key = line
            if current_key in {"upc", "ean", "model", "partNumber"} and current_key not in criteria:
                criteria[current_key] = []
            continue

        if current_key:
            _add_value(criteria, current_key, line)
        else:
            _add_value(criteria, "upc", line)

    # Remove empty lists
    for k in {"upc", "ean", "model", "partNumber"}:
        if k in criteria and isinstance(criteria[k], list) and len(criteria[k]) == 0:
            criteria.pop(k)

    return criteria


def load_criteria_file(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Criteria file not found: {path}")

    if p.suffix.lower() in {".json"}:
        return json.loads(p.read_text(encoding="utf-8"))

    return parse_criteria_txt(str(p))


@dataclass(frozen=True)
class Strategy:
    mode: str  # bypass_bulk | bypass_individual | windowing | simple
    bypass_filter: Optional[str] = None
    bypass_values: Optional[List[str]] = None
    base_criteria: Optional[Dict[str, Any]] = None


class StrategyAnalyzer:
    """Select mode based on criteria content."""

    BYPASSABLE = {"model", "partNumber", "upc", "ean"}
    WINDOWING = {"title", "brand", "manufacturer"}
    MULTI_VALUE_FIELDS = BYPASSABLE | WINDOWING  # All fields that can have multiple values

    @staticmethod
    def _normalize_values(v: Any) -> List[str]:
        if v is None:
            return []
        if isinstance(v, list):
            out = [str(x).strip() for x in v if str(x).strip()]
        else:
            s = str(v).strip()
            out = [s] if s else []
        return out

    @staticmethod
    def analyze(criteria: Dict[str, Any]) -> Strategy:
        # Check ALL multi-value fields for lists with >50 values
        # These must use bypass mode due to Keepa's 50-entry limit per filter
        for f in StrategyAnalyzer.MULTI_VALUE_FIELDS:
            if f in criteria:
                values = StrategyAnalyzer._normalize_values(criteria.get(f))

                # If it's a list with multiple values, check if it exceeds 50
                if len(values) > 1:
                    base = {k: v for k, v in criteria.items() if k != f}

                    if len(values) > 50:
                        # Must iterate individually to avoid Keepa's 50-entry limit
                        return Strategy(mode="bypass_individual", bypass_filter=f, bypass_values=values, base_criteria=base)
                    else:
                        # Can send all values in bulk (<=50 entries)
                        return Strategy(mode="bypass_bulk", bypass_filter=f, bypass_values=values, base_criteria=base)

        # Only use windowing if there are NO multi-value fields
        # (single-value brand/title/manufacturer can still use windowing)
        for f in StrategyAnalyzer.WINDOWING:
            if f in criteria:
                return Strategy(mode="windowing", base_criteria=criteria.copy())

        return Strategy(mode="simple", base_criteria=criteria.copy())


class KeepaExporter:
    """Orchestrates export based on strategy."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.client = KeepaClient(cfg)
        self.dedupe = DedupeTracker(cfg.dedupe_db)
        self.ckpt = Checkpoint(cfg.checkpoint_file)
        self.csv = CSVWriter(cfg.output_csv)
        self.windows = DateWindowGenerator(cfg.start_year, cfg.window_months)

    def _reset_outputs_if_needed(self) -> None:
        if not self.cfg.reset:
            return
        for p in [self.cfg.output_csv, self.cfg.dedupe_db, self.cfg.checkpoint_file]:
            pp = Path(p)
            if pp.exists():
                pp.unlink()
                print(f"🗑️  Deleted: {p}")
        self.dedupe.close()
        self.dedupe = DedupeTracker(self.cfg.dedupe_db)
        self.ckpt = Checkpoint(self.cfg.checkpoint_file)
        self.csv = CSVWriter(self.cfg.output_csv)

    def load_criteria(self) -> Dict[str, Any]:
        criteria = load_criteria_file(self.cfg.criteria_file)

        criteria["perPage"] = self.cfg.per_page
        criteria.setdefault("sort", [["trackingSince", "asc"]])

        criteria.pop("page", None)
        if "trackingSince_gte" in criteria or "trackingSince_lte" in criteria:
            # keep if user explicitly set it in criteria; windowing mode will override anyway
            pass

        return criteria

    def _paginate_query(self, criteria: Dict[str, Any], label: str) -> int:
        total_new = 0
        page = 0
        max_page = self.cfg.max_results_per_query // self.cfg.per_page  # pages 0..max_page-1

        while True:
            if page >= max_page:
                print(f"  ⚠️  {label}: reached Keepa 10k cap (page={page}, perPage={self.cfg.per_page})")
                break

            self.client.wait_for_tokens(15)
            criteria["page"] = page

            resp = self.client.query(criteria)
            asins = resp.get("asinList", []) or []
            if not asins:
                break

            aligned: Dict[str, List[Any]] = {}
            for k, v in resp.items():
                if k != "asinList" and isinstance(v, list) and len(v) == len(asins):
                    aligned[k] = v

            rows: List[Dict[str, Any]] = []
            for i, asin in enumerate(asins):
                if self.dedupe.is_new(asin):
                    row: Dict[str, Any] = {"asin": asin}
                    for k, values in aligned.items():
                        if i < len(values):
                            row[k] = values[i]
                    rows.append(row)
                    total_new += 1

            if rows:
                self.csv.write_rows(rows)

            if len(asins) < self.cfg.per_page:
                break

            page += 1

        return total_new

    def _run_bypass_bulk(self, base: Dict[str, Any], filter_name: str, values: List[str]) -> int:
        print(f"\n{'='*80}")
        print(f"BYPASS BULK: {filter_name} ({len(values)} values)")
        print(f"{'='*80}")

        if filter_name in self._STRING_ONLY_FIELDS:
            # title/brand/manufacturer only accept a single string; iterate one by one
            total_new = 0
            for i, val in enumerate(values):
                print(f"  [{i+1}/{len(values)}] {filter_name}={val!r}...", end=" ", flush=True)
                criteria = {**base, filter_name: val}
                new_here = self._paginate_query(criteria, label=f"bypass_bulk:{filter_name}:{i}")
                total_new += new_here
                print(f"({new_here} new)")
            self.ckpt.save(mode="bypass_bulk", identifier_index=len(values))
            return total_new

        criteria = {**base, filter_name: values}
        self.ckpt.save(mode="bypass_bulk", identifier_index=len(values))
        return self._paginate_query(criteria, label="bypass_bulk")

    # Fields that only accept a single string value (not a list)
    _STRING_ONLY_FIELDS = {"title", "brand", "manufacturer"}

    def _run_bypass_individual(self, base: Dict[str, Any], filter_name: str, values: List[str]) -> int:
        """Process values in batches of 50 (Keepa's limit), or one-by-one for string-only fields."""
        is_string_only = filter_name in self._STRING_ONLY_FIELDS
        BATCH_SIZE = 1 if is_string_only else 50

        print(f"\n{'='*80}")
        if is_string_only:
            print(f"BYPASS INDIVIDUAL: {filter_name} ({len(values)} values, one at a time)")
        else:
            print(f"BYPASS BATCHED: {filter_name} ({len(values)} values in batches of {BATCH_SIZE})")
        print(f"{'='*80}")

        start_idx = self.ckpt.identifier_index() if self.cfg.resume else 0
        if start_idx:
            print(f"🔄 RESUMING from index {start_idx}")

        total_new = 0
        total_batches = (len(values) + BATCH_SIZE - 1) // BATCH_SIZE

        for batch_num in range(start_idx // BATCH_SIZE, total_batches):
            batch_start = batch_num * BATCH_SIZE
            batch_end = min(batch_start + BATCH_SIZE, len(values))
            batch_values = values[batch_start:batch_end]

            print(f"  [Batch {batch_num+1}/{total_batches}] {filter_name} idx {batch_start}-{batch_end-1} ({len(batch_values)} values)...", end=" ", flush=True)

            criteria = {**base}
            if is_string_only:
                # title/brand/manufacturer accept only a single string
                criteria[filter_name] = batch_values[0]
            else:
                # upc/ean/model/partNumber accept a list (up to 50)
                criteria[filter_name] = batch_values

            new_here = self._paginate_query(criteria, label=f"{filter_name}:batch{batch_num}")
            total_new += new_here

            print(f"({new_here} new)")
            self.ckpt.save(mode="bypass_individual", identifier_index=batch_end)

        return total_new

    def _run_windowing(self, criteria: Dict[str, Any]) -> int:
        print(f"\n{'='*80}")
        print("WINDOWING: trackingSince date windows")
        print(f"{'='*80}")

        start_from = self.ckpt.current_date() if self.cfg.resume else None
        if start_from:
            print(f"🔄 RESUMING from {start_from}")

        total_new = 0
        for start_dt, end_dt, label in self.windows.generate(start_from):
            print(f"\nWINDOW: {label}")

            wcrit = dict(criteria)
            wcrit["trackingSince_gte"] = datetime_to_keepa_minutes(start_dt)
            wcrit["trackingSince_lte"] = datetime_to_keepa_minutes(end_dt)

            total_new += self._paginate_query(wcrit, label=f"window:{label}")
            self.ckpt.save(mode="windowing", current_date=end_dt.strftime("%Y-%m-%d"))

        return total_new

    def _run_simple(self, criteria: Dict[str, Any]) -> int:
        print(f"\n{'='*80}")
        print("SIMPLE MODE: criteria pagination")
        print(f"{'='*80}")
        self.ckpt.save(mode="simple")
        return self._paginate_query(criteria, label="simple")

    def run(self) -> None:
        self._reset_outputs_if_needed()

        criteria = self.load_criteria()

        # Ensure we don't accidentally keep stale pages
        criteria.pop("page", None)

        strategy = StrategyAnalyzer.analyze(criteria)

        print(f"\n{'='*80}")
        print("KEEPA ASIN FINDER - SMART BYPASS (TXT SECTIONS)")
        print(f"{'='*80}")
        print(f"Criteria file: {self.cfg.criteria_file}")
        print(f"Output CSV:   {self.cfg.output_csv}")
        print(f"Mode:         {strategy.mode}")
        if strategy.bypass_filter:
            print(f"Filter:       {strategy.bypass_filter} ({len(strategy.bypass_values or [])} values)")
        print(f"Reset:        {self.cfg.reset}")
        print(f"Resume:       {self.cfg.resume}")
        print(f"{'='*80}\n")

        if strategy.mode == "bypass_bulk":
            total_new = self._run_bypass_bulk(
                strategy.base_criteria or {},
                strategy.bypass_filter or "",
                strategy.bypass_values or [],
            )
        elif strategy.mode == "bypass_individual":
            total_new = self._run_bypass_individual(
                strategy.base_criteria or {},
                strategy.bypass_filter or "",
                strategy.bypass_values or [],
            )
        elif strategy.mode == "windowing":
            total_new = self._run_windowing(strategy.base_criteria or {})
        else:
            total_new = self._run_simple(strategy.base_criteria or {})

        print(f"\n{'='*80}")
        print("EXPORT COMPLETE")
        print(f"{'='*80}")
        print(f"New ASINs exported: {total_new:,}")
        print(f"Total unique ASINs: {self.dedupe.count():,}")
        print(f"Output file:        {self.cfg.output_csv}")
        print(f"{'='*80}\n")

        self.dedupe.close()


def main() -> int:
    try:
        cfg = Config.load()
        KeepaExporter(cfg).run()
        return 0
    except Exception as e:
        print(f"\n❌ ERROR: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
