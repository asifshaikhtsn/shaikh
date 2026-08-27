# shaikh - Aggregator of 9 habibi-boost repos

Collects **only working proxies** (already validated + geolocated) from 9 boost repos **as-is, without re-validate or re-geo**.

**Sources (9):**
- `habibi-boost` (http 0-50k)
- `habibi-boost-1` (http 50k-100k)
- `habibi-boost-2` (http 100k-150k)
- `habibi-boost-3` (https ALL 56k)
- `habibi-boost-socks4-1` (socks4 0-50k)
- `habibi-boost-socks4-2` (socks4 50k-100k)
- `habibi-boost-socks4-3` (socks4 100k-135k)
- `habibi-boost-socks5-1` (socks5 0-50k)
- `habibi-boost-socks5-2` (socks5 50k-100k)
- `habibi-boost-socks5-3` (socks5 100k-148k)

**Pipeline:** Fetch `data/live_proxies.json` from each (contains `proxy` + `country` already) -> dedup by proxy -> group by `country/<CC>/<proto>.txt` + `country/all_<proto>.txt` + `country/all.txt` -> `data/all_proxies.json`

**No validate, no geolocate** - just aggregate.

**Schedule:** Every 1 hour + manual dispatch
