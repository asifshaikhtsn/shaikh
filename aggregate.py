import asyncio
import importlib.util
import json
import os
import sys
import urllib.request
from pathlib import Path

import aiohttp

# Existing protocol-specific boost repos plus validated direct-source snapshots.
# Protocols are normalized uppercase by the shared engine.
SOURCES = [
    ("habibi-boost", "HTTP"),
    ("habibi-boost-1", "HTTP"),
    ("habibi-boost-2", "HTTP"),
    ("habibi-boost-3", "HTTPS"),
    ("habibi-boost-socks4-1", "SOCKS4"),
    ("habibi-boost-socks4-2", "SOCKS4"),
    ("habibi-boost-socks4-3", "SOCKS4"),
    ("habibi-boost-socks5-1", "SOCKS5"),
    ("habibi-boost-socks5-2", "SOCKS5"),
    ("habibi-boost-socks5-3", "SOCKS5"),
    ("sevenworks-http", "HTTP"),
    ("sevenworks-https", "HTTPS"),
    ("sevenworks-socks4", "SOCKS4"),
    ("sevenworks-socks5", "SOCKS5"),
    ("vann-dev-socks5", "SOCKS5"),
]

LIVE_JSON_URL = "https://raw.githubusercontent.com/asifshaikhtsn/{repo}/master/data/live_proxies.json"
CONCURRENCY = 200

DIRECT_SOURCE_IDS = {
    "sevenworks-http",
    "sevenworks-https",
    "sevenworks-socks4",
    "sevenworks-socks5",
    "vann-dev-socks5",
}


def _direct_live_path(source_id):
    runner_temp = Path(os.environ.get("RUNNER_TEMP", "/tmp"))
    return runner_temp / "shaikh-direct" / source_id / "data" / "live_proxies.json"


async def fetch_live(session, repo):
    if repo in DIRECT_SOURCE_IDS:
        path = _direct_live_path(repo)
        if not path.exists():
            print(f"[{repo}] validated local snapshot missing: {path}")
            return []
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            rows = data.get("proxies", [])
            return rows if isinstance(rows, list) else []
        except Exception as exc:
            print(f"[{repo}] local snapshot error: {exc}")
            return []

    url = LIVE_JSON_URL.format(repo=repo)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status != 200:
                print(f"[{repo}] HTTP {resp.status}")
                return []
            try:
                data = await resp.json(content_type=None)
            except Exception:
                data = json.loads(await resp.text())
            rows = data.get("proxies", [])
            return rows if isinstance(rows, list) else []
    except Exception as exc:
        print(f"[{repo}] fetch error: {exc}")
        return []


def _load_shared_engine():
    root = Path(__file__).resolve().parent
    files = {
        "geo_country.py": "https://raw.githubusercontent.com/asifshaikhtsn/Walla/main/geo_country.py",
        "geo_runner.py": "https://raw.githubusercontent.com/asifshaikhtsn/Walla/main/geo_runner.py",
    }
    for name, url in files.items():
        path = root / name
        if not path.exists():
            urllib.request.urlretrieve(url, path)
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    spec = importlib.util.spec_from_file_location("geo_runner", root / "geo_runner.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def main():
    engine = _load_shared_engine()
    await engine.run_shaikh(sys.modules[__name__])


if __name__ == "__main__":
    asyncio.run(main())
