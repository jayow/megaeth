"""Hourly incremental update job.

Runs the full data pipeline in order:
  1. sync.py          — pulls only NEW transfers since last sync (resumable)
  2. classify.py      — re-detects distributors / DEX pairs
  3. balances.py      — refreshes balanceOf for changed addresses
  4. megaeth_claims.py — pulls new Claimed events on Distributor proxy
  5. POST /api/refresh — invalidates the server's per-address cache

Designed for an hourly cron. Safe to run more often (sync is idempotent,
each step is incremental). Logs to stdout with timestamps.
"""

import os
import subprocess
import sys
import time
import urllib.request
import urllib.error


HERE = os.path.dirname(os.path.abspath(__file__))
PYTHON = os.path.join(HERE, "venv", "bin", "python")
if not os.path.exists(PYTHON):
    PYTHON = sys.executable

REFRESH_URL = os.environ.get("DASHBOARD_REFRESH_URL", "http://127.0.0.1:5050/api/refresh")


def log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def run_step(name, args, optional=False):
    log(f"→ {name}")
    t0 = time.time()
    try:
        result = subprocess.run(
            [PYTHON] + args,
            cwd=HERE,
            capture_output=True,
            text=True,
            timeout=900,  # 15-min hard cap per step
        )
        elapsed = time.time() - t0
        if result.returncode != 0:
            log(f"  ✗ {name} failed in {elapsed:.1f}s (rc={result.returncode})")
            log(f"  stderr: {result.stderr[-500:]}")
            return optional  # if optional, treat failure as non-fatal
        # Print last 5 lines of output
        tail = (result.stdout or "").strip().splitlines()[-5:]
        for line in tail:
            log(f"    {line}")
        log(f"  ✓ {name} done in {elapsed:.1f}s")
        return True
    except subprocess.TimeoutExpired:
        log(f"  ✗ {name} timed out after 15 min")
        return optional


def hit_refresh():
    log(f"→ POST {REFRESH_URL}")
    req = urllib.request.Request(REFRESH_URL, method="POST", data=b"")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            body = r.read().decode()
            log(f"  ✓ {r.status} {body.strip()}")
            return True
    except urllib.error.URLError as e:
        log(f"  ⚠ refresh skipped — server unreachable ({e})")
        return False


def main():
    log("=== UPDATE START ===")
    overall_t0 = time.time()

    ok = True
    ok &= run_step("sync transfers",   ["sync.py"])
    ok &= run_step("classify",         ["classify.py"])
    ok &= run_step("balances refresh", ["balances.py"])
    ok &= run_step("Claimed events",   ["megaeth_claims.py"], optional=True)
    hit_refresh()

    elapsed = time.time() - overall_t0
    log(f"=== UPDATE DONE in {elapsed:.1f}s — overall {'OK' if ok else 'WITH ERRORS'} ===")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
