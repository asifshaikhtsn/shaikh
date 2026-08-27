import asyncio
import json
import time
from collections import defaultdict
from pathlib import Path

import aiohttp

ROOT = Path(__file__).resolve().parent
COUNTRY_DIR = ROOT / "country"
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

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
# fallback raw country files if json missing - not used now but kept for reference


async def fetch_live(session, repo):
    url = LIVE_JSON_URL.format(repo=repo)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status == 200:
                data = await resp.json()
                proxies = data.get("proxies", [])
                # proxies is list of {"proxy": "ip:port", "country": "XX"}
                return proxies
            else:
                print(f"[{repo}] HTTP {resp.status} for {url}")
    except Exception as e:
        print(f"[{repo}] fetch error: {e}")
    return []


async def main():
    print("[Shaikh] Aggregating working proxies from 9 boost repos (no validate, no geo - as-is)...")
    async with aiohttp.ClientSession() as session:
        all_entries = []  # list of (proxy, country, proto, repo)
        per_repo = {}
        for repo, proto in SOURCES:
            proxies = await fetch_live(session, repo)
            per_repo[repo] = len(proxies)
            print(f"[Shaikh] {repo} ({proto}): {len(proxies)} proxies")
            for entry in proxies:
                proxy = entry.get("proxy", "").strip()
                country = entry.get("country", "XX").strip().upper() or "XX"
                if proxy:
                    all_entries.append((proxy, country, proto, repo))

        print(f"[Shaikh] Total fetched (with dup): {len(all_entries)}")

        # Dedup by proxy (keep first country/proto)
        seen = {}
        deduped = []
        for proxy, country, proto, repo in all_entries:
            if proxy not in seen:
                seen[proxy] = (country, proto)
                deduped.append({"proxy": proxy, "country": country, "protocol": proto, "source": repo})
            # if duplicate with different country/proto, keep first

        print(f"[Shaikh] After dedup: {len(deduped)} unique proxies")

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

        # Also write per-protocol all files
        for proto in ["http", "https", "socks4", "socks5"]:
            all_proto = [e["proxy"] for e in deduped if e["protocol"] == proto]
            if all_proto:
                (COUNTRY_DIR / f"all_{proto}.txt").write_text("\n".join(sorted(all_proto)) + "\n", encoding="utf-8")

        # Write combined all
        (COUNTRY_DIR / "all.txt").write_text("\n".join(sorted([e["proxy"] for e in deduped])) + "\n" if deduped else "", encoding="utf-8")

        # Write data files
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        (DATA_DIR / "all_proxies.json").write_text(json.dumps({"proxies": deduped, "count": len(deduped), "updated": time.time()}, indent=2), encoding="utf-8")
        # Per-protocol counts
        (DATA_DIR / "live_proxies.json").write_text(json.dumps({"proxies": deduped, "count": len(deduped), "updated": time.time()}, indent=2), encoding="utf-8")

        summary = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "sources": [r[0] for r in SOURCES],
            "per_repo": per_repo,
            "total_fetched": len(all_entries),
            "unique": len(deduped),
            "protocol_counts": dict(protocol_counts),
            "country_count": len(by_country),
            "country_counts": dict(sorted({cc: len(v) for cc, v in by_country.items()}.items(), key=lambda x: -x[1])[:20]),
        }
        (ROOT / "last_run.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(json.dumps(summary, indent=2))
        print(f"\n[Shaikh] DONE! Unique: {len(deduped)}, Countries: {len(by_country)}, Protocols: {dict(protocol_counts)}")


if __name__ == "__main__":
    asyncio.run(main())
