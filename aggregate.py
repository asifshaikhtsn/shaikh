import asyncio
import json
import re
import time
from collections import defaultdict
from pathlib import Path

import aiohttp

ROOT = Path(__file__).resolve().parent
COUNTRY_DIR = ROOT / "country"
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DEAD_FILE = DATA_DIR / "dead_proxies.json"

ADDRESS_RE = re.compile(r"(\d{1,3}(?:\.\d{1,3}){3}:\d{1,5})")
CONCURRENCY = 100
TIMEOUT = 10

try:
    from aiohttp_socks import ProxyConnector
    HAS_SOCKS = True
except ImportError:
    HAS_SOCKS = False
    ProxyConnector = None


def load_dead_set():
    if DEAD_FILE.exists():
        try:
            return set(json.loads(DEAD_FILE.read_text(encoding="utf-8")).get("dead", []))
        except Exception:
            return set()
    return set()


def save_dead_set(dead_set):
    DEAD_FILE.write_text(json.dumps({"dead": sorted(dead_set), "updated": time.time(), "count": len(dead_set)}, indent=2), encoding="utf-8")


async def test_proxy(address, protocol, semaphore):
    async with semaphore:
        try:
            timeout = aiohttp.ClientTimeout(total=TIMEOUT)
            proto = protocol.upper()
            if proto in ("SOCKS4", "SOCKS5") and HAS_SOCKS:
                connector = ProxyConnector.from_url(f"{proto.lower()}://{address}")
                async with aiohttp.ClientSession(connector=connector, timeout=timeout) as s:
                    async with s.get("http://httpbin.org/ip", timeout=timeout) as resp:
                        if resp.status == 200:
                            return True
            else:
                scheme = "http" if proto in ("HTTP", "HTTPS") else proto.lower()
                proxy_url = f"{scheme}://{address}"
                async with aiohttp.ClientSession(timeout=timeout) as s:
                    async with s.get("http://httpbin.org/ip", proxy=proxy_url, timeout=timeout) as resp:
                        if resp.status == 200:
                            return True
        except Exception:
            pass
    return False


def load_existing_working():
    """Load previous working from data/all_proxies.json or country files"""
    existing = []
    all_json = DATA_DIR / "all_proxies.json"
    if all_json.exists():
        try:
            data = json.loads(all_json.read_text(encoding="utf-8"))
            for e in data.get("proxies", []):
                proxy = e.get("proxy", "").strip()
                country = e.get("country", "XX").strip().upper() or "XX"
                proto = e.get("protocol", "HTTP").strip().upper()
                if proxy and ADDRESS_RE.search(proxy):
                    existing.append({"proxy": proxy, "country": country, "protocol": proto, "source": e.get("source", "existing")})
            if existing:
                return existing
        except Exception:
            pass
    if COUNTRY_DIR.exists():
        for cc_dir in COUNTRY_DIR.iterdir():
            if not cc_dir.is_dir():
                continue
            cc = cc_dir.name.upper()
            if cc.startswith("ALL"):
                continue
            for proto_file in cc_dir.iterdir():
                if not proto_file.is_file():
                    continue
                proto = proto_file.stem.upper()
                if proto not in ("HTTP", "HTTPS", "SOCKS4", "SOCKS5"):
                    continue
                try:
                    for line in proto_file.read_text(encoding="utf-8").splitlines():
                        m = ADDRESS_RE.search(line)
                        if m:
                            existing.append({"proxy": m.group(1), "protocol": proto, "country": cc, "source": "existing"})
                except Exception:
                    continue
    return existing


# 9 boost repos to aggregate (no Walla, no habibi)
SOURCES = [
    ("habibi-boost", "http"),
    ("habibi-boost-1", "http"),
    ("habibi-boost-2", "http"),
    ("habibi-boost-3", "https"),
    ("habibi-boost-socks4-1", "socks4"),
    ("habibi-boost-socks4-2", "socks4"),
    ("habibi-boost-socks4-3", "socks4"),
    ("habibi-boost-socks5-1", "socks5"),
    ("habibi-boost-socks5-2", "socks5"),
    ("habibi-boost-socks5-3", "socks5"),
]

LIVE_JSON_URL = "https://raw.githubusercontent.com/asifshaikhtsn/{repo}/master/data/live_proxies.json"


async def fetch_live(session, repo):
    url = LIVE_JSON_URL.format(repo=repo)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status == 200:
                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    text = await resp.text()
                    data = json.loads(text)
                proxies = data.get("proxies", [])
                return proxies
            else:
                print(f"[{repo}] HTTP {resp.status} for {url}")
    except Exception as e:
        print(f"[{repo}] fetch error: {e}")
    return []


async def main():
    print("[Shaikh] Aggregating working proxies from 9 boost repos - persistent working re-validate + new as-is...")
    # --- Persistent: re-validate existing working before new fetch ---
    dead_set = load_dead_set()
    existing = load_existing_working()
    print(f"[Persistent] Loaded existing working: {len(existing)}, dead: {len(dead_set)}")
    still_working = []
    if existing:
        existing_filtered = [e for e in existing if e["proxy"] not in dead_set]
        print(f"[Persistent] Existing after dead filter: {len(existing)} -> {len(existing_filtered)}")
        if existing_filtered:
            semaphore0 = asyncio.Semaphore(CONCURRENCY)
            tasks0 = [test_proxy(e["proxy"], e["protocol"], semaphore0) for e in existing_filtered]
            results0 = await asyncio.gather(*tasks0)
            still_working = [e for e, ok in zip(existing_filtered, results0) if ok]
            dead_existing = [e["proxy"] for e, ok in zip(existing_filtered, results0) if not ok]
            print(f"[Persistent] Still working: {len(still_working)}, Dead from old: {len(dead_existing)}")
            if dead_existing:
                dead_set.update(dead_existing)
                save_dead_set(dead_set)
        else:
            print("[Persistent] All existing already in dead list")
    else:
        print("[Persistent] No existing working to re-validate")

    async with aiohttp.ClientSession() as session:
        all_entries = []
        per_repo = {}
        for repo, proto in SOURCES:
            proxies = await fetch_live(session, repo)
            per_repo[repo] = len(proxies)
            print(f"[Shaikh] {repo} ({proto}): {len(proxies)} proxies")
            for entry in proxies:
                proxy = entry.get("proxy", "").strip()
                country = entry.get("country", "XX").strip().upper() or "XX"
                if proxy and proxy not in dead_set:
                    all_entries.append((proxy, country, proto, repo))

        print(f"[Shaikh] Total fetched new (with dup, dead filtered): {len(all_entries)}")

        # Dedup new fetched by proxy, also against still_working
        seen_new = {}
        deduped_new = []
        still_addrs = {e["proxy"] for e in still_working}
        for proxy, country, proto, repo in all_entries:
            if proxy in still_addrs:
                continue
            if proxy not in seen_new:
                seen_new[proxy] = (country, proto)
                deduped_new.append({"proxy": proxy, "country": country, "protocol": proto, "source": repo})

        print(f"[Shaikh] New deduped: {len(deduped_new)} unique new proxies")

        # Merge still working + new (new already as-is, no re-validate, no geo)
        deduped = still_working + deduped_new
        print(f"[Shaikh] After merge still+new: {len(deduped)} unique proxies (still {len(still_working)} + new {len(deduped_new)})")

        # Group by country and protocol
        by_country_proto = defaultdict(set)
        by_country = defaultdict(set)
        protocol_counts = defaultdict(int)
        for e in deduped:
            by_country_proto[(e["country"], e["protocol"])].add(e["proxy"])
            by_country[e["country"]].add(e["proxy"])
            protocol_counts[e["protocol"]] += 1

        # Clean and write country files
        import shutil
        if COUNTRY_DIR.exists():
            shutil.rmtree(COUNTRY_DIR)
        COUNTRY_DIR.mkdir(parents=True, exist_ok=True)

        for (cc, proto), proxies in by_country_proto.items():
            cc_dir = COUNTRY_DIR / cc
            cc_dir.mkdir(parents=True, exist_ok=True)
            (cc_dir / f"{proto}.txt").write_text("\n".join(sorted(proxies)) + "\n", encoding="utf-8")

        for proto in ["http", "https", "socks4", "socks5"]:
            all_proto = [e["proxy"] for e in deduped if e["protocol"] == proto]
            if all_proto:
                (COUNTRY_DIR / f"all_{proto}.txt").write_text("\n".join(sorted(all_proto)) + "\n", encoding="utf-8")

        (COUNTRY_DIR / "all.txt").write_text("\n".join(sorted([e["proxy"] for e in deduped])) + "\n" if deduped else "", encoding="utf-8")

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        (DATA_DIR / "all_proxies.json").write_text(json.dumps({"proxies": deduped, "count": len(deduped), "updated": time.time()}, indent=2), encoding="utf-8")
        (DATA_DIR / "live_proxies.json").write_text(json.dumps({"proxies": deduped, "count": len(deduped), "updated": time.time()}, indent=2), encoding="utf-8")

        summary = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "sources": [r[0] for r in SOURCES],
            "per_repo": per_repo,
            "still_working": len(still_working),
            "new_fetched": len(all_entries),
            "new_deduped": len(deduped_new),
            "unique": len(deduped),
            "dead_total": len(dead_set),
            "protocol_counts": dict(protocol_counts),
            "country_count": len(by_country),
            "country_counts": dict(sorted({cc: len(v) for cc, v in by_country.items()}.items(), key=lambda x: -x[1])[:20]),
        }
        (ROOT / "last_run.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(json.dumps(summary, indent=2))
        print(f"\n[Shaikh] DONE! Unique: {len(deduped)} (still {len(still_working)} + new {len(deduped_new)}), Countries: {len(by_country)}, Protocols: {dict(protocol_counts)}")


if __name__ == "__main__":
    asyncio.run(main())
