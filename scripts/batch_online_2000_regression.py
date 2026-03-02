#!/usr/bin/env python3
"""
Collect 2000 company names online (Eastmoney clist) and run repeated enrich checks.

Outputs:
  data/review_reports/online_2000_regression_<timestamp>.json
  data/review_reports/online_2000_regression_<timestamp>.csv
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import datetime as dt
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "review_reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)

EM_URL = (
    "https://push2.eastmoney.com/api/qt/clist/get"
    "?pn={pn}&pz={pz}&po=1&np=1"
    "&ut=bd1d9ddb04089700cf9c27f6f7426281"
    "&fltt=2&invt=2&fid=f3"
    "&fs=m:0+t:6,m:0+t:13,m:1+t:2,m:1+t:23"
    "&fields=f12,f14"
)
EM_SUGGEST_URL = (
    "https://searchapi.eastmoney.com/api/suggest/get"
    "?input={q}&type=14&token=D43BF722C8E33BDC906FB84D85E326E8&count={count}"
)

SUGGEST_TOKENS = list(
    "中科华新创联海深广上北南东西天长恒立国金银光安德宏微芯智数云电车"
    "半导体电子网络科技信息系统材料化工生物医药能源装备制造机电通信导航"
)


def http_get_json(url: str, timeout: float = 15.0) -> Dict:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json,text/plain,*/*",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", errors="ignore"))


def fetch_online_names(target: int = 2000, page_size: int = 200) -> List[str]:
    names: List[str] = []
    seen = set()
    pages = max(1, (target + page_size - 1) // page_size) + 3
    for pn in range(1, pages + 1):
        try:
            payload = http_get_json(EM_URL.format(pn=pn, pz=page_size), timeout=20)
        except Exception:
            continue
        diff = (((payload or {}).get("data") or {}).get("diff")) or []
        if not diff:
            continue
        for row in diff:
            name = str((row or {}).get("f14") or "").strip()
            if not name:
                continue
            if name in seen:
                continue
            seen.add(name)
            names.append(name)
            if len(names) >= target:
                return names
    if len(names) >= target:
        return names[:target]

    # Source-2: eastmoney suggest to expand universe
    for q in SUGGEST_TOKENS:
        try:
            payload = http_get_json(EM_SUGGEST_URL.format(q=urllib.parse.quote(q), count=50), timeout=15)
        except Exception:
            continue
        items = (
            (((payload or {}).get("QuotationCodeTable") or {}).get("Data")) or []
        )
        for row in items:
            name = str((row or {}).get("Name") or "").strip()
            if not name:
                continue
            if name in seen:
                continue
            seen.add(name)
            names.append(name)
            if len(names) >= target:
                return names[:target]
    return names[:target]


def validate_one(api_base: str, q: str, timeout: float = 20.0) -> Dict:
    url = f"{api_base.rstrip('/')}/api/enrich?q={urllib.parse.quote(q)}"
    t0 = time.time()
    try:
        obj = http_get_json(url, timeout=timeout)
        cost_ms = int((time.time() - t0) * 1000)
    except Exception as ex:
        return {
            "query": q,
            "ok": False,
            "error": str(ex),
            "costMs": int((time.time() - t0) * 1000),
            "companyName": "",
            "listed": None,
            "industryL1": "",
            "industryL2": "",
            "website": "",
            "annualFound": False,
            "competitors": 0,
            "top5": 0,
            "suppliers": 0,
            "customers": 0,
            "issueFlags": "request_error",
        }

    c = obj.get("company") or {}
    src = obj.get("source") or {}
    annual_meta = (src.get("annualReport") or {}) if isinstance(src, dict) else {}
    annual_found = bool(annual_meta.get("found"))
    comp_rows = obj.get("competitors") or []
    top5_rows = obj.get("top5") or []
    sup_rows = obj.get("suppliers") or []
    cus_rows = obj.get("customers") or []
    ie = src.get("industryEvidence") or {}
    listed = bool(c.get("isListed"))
    website = str(c.get("website") or "").strip()
    l1 = str(c.get("industryLevel1") or "").strip()
    l2 = str(c.get("industryLevel2") or "").strip()
    has_industry_evidence = listed or bool(ie.get("website")) or bool(ie.get("registry")) or bool(ie.get("annual"))

    flags = []
    if (not has_industry_evidence) and l2 and l2 != "未识别":
        flags.append("industry_gate_violation")
    if (not listed) and (not annual_found) and (len(comp_rows) > 0 or len(top5_rows) > 0):
        flags.append("peer_top5_gate_violation")
    if any("行业Top候选" in str((x or {}).get("reason") or "") for x in comp_rows):
        flags.append("template_competitor_reason")
    if any(str((x or {}).get("sourceType") or "") in {"industry_public_ranking", "industry_top_candidate"} for x in top5_rows):
        flags.append("template_top5_source")
    if any(str((x or {}).get("name") or "").strip() in {"-", ""} for x in top5_rows):
        flags.append("bad_top5_name")

    return {
        "query": q,
        "ok": True,
        "error": "",
        "costMs": cost_ms,
        "companyName": str(c.get("name") or ""),
        "listed": listed,
        "industryL1": l1,
        "industryL2": l2,
        "website": website,
        "annualFound": annual_found,
        "competitors": len(comp_rows),
        "top5": len(top5_rows),
        "suppliers": len(sup_rows),
        "customers": len(cus_rows),
        "issueFlags": "|".join(flags),
    }


def run_batch(api_base: str, names: List[str], rounds: int, workers: int) -> Tuple[List[Dict], Dict]:
    rows: List[Dict] = []
    all_queries = []
    for _ in range(max(1, rounds)):
        all_queries.extend(names)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        futs = [ex.submit(validate_one, api_base, q) for q in all_queries]
        for i, f in enumerate(concurrent.futures.as_completed(futs), 1):
            rows.append(f.result())
            if i % 200 == 0:
                print(f"[progress] {i}/{len(all_queries)}", flush=True)

    total = len(rows)
    ok_rows = [x for x in rows if x["ok"]]
    err_rows = [x for x in rows if not x["ok"]]
    bad_rows = [x for x in ok_rows if x["issueFlags"]]
    avg_ms = int(sum(x["costMs"] for x in rows) / total) if total else 0
    p95_ms = 0
    if rows:
        ms = sorted(x["costMs"] for x in rows)
        p95_ms = ms[min(len(ms) - 1, int(len(ms) * 0.95))]

    summary = {
        "generatedAt": dt.datetime.now().isoformat(timespec="seconds"),
        "apiBase": api_base,
        "nameCount": len(names),
        "rounds": rounds,
        "totalChecks": total,
        "okCount": len(ok_rows),
        "errorCount": len(err_rows),
        "issueCount": len(bad_rows),
        "avgMs": avg_ms,
        "p95Ms": int(p95_ms),
        "issuesTop10": sorted(
            (
                (k, sum(1 for x in bad_rows if k in x["issueFlags"].split("|")))
                for k in {
                    "industry_gate_violation",
                    "peer_top5_gate_violation",
                    "template_competitor_reason",
                    "template_top5_source",
                    "bad_top5_name",
                }
            ),
            key=lambda x: x[1],
            reverse=True,
        ),
    }
    return rows, summary


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api-base", default=os.environ.get("API_BASE", "https://api.gstpcx.online"))
    ap.add_argument("--target", type=int, default=2000)
    ap.add_argument("--rounds", type=int, default=2)
    ap.add_argument("--workers", type=int, default=12)
    args = ap.parse_args()

    print(f"[info] collecting online names target={args.target}", flush=True)
    names = fetch_online_names(target=args.target)
    if len(names) < 200:
        print(f"[error] online name collection too small: {len(names)}", file=sys.stderr)
        return 2
    print(f"[info] collected names={len(names)}", flush=True)

    rows, summary = run_batch(args.api_base, names, args.rounds, args.workers)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = OUT_DIR / f"online_2000_regression_{ts}.json"
    out_csv = OUT_DIR / f"online_2000_regression_{ts}.csv"
    out_json.write_text(
        json.dumps({"summary": summary, "sampleNames": names[:50], "rows": rows}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    fields = [
        "query",
        "ok",
        "error",
        "costMs",
        "companyName",
        "listed",
        "industryL1",
        "industryL2",
        "website",
        "annualFound",
        "competitors",
        "top5",
        "suppliers",
        "customers",
        "issueFlags",
    ]
    with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})

    print("[done] summary:")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"[done] json={out_json}")
    print(f"[done] csv={out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
