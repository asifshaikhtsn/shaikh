import os

# Generic adapter used by the Shaikh workflow to validate raw proxy feeds with
# the shared Walla geo_runner boost engine before they are merged into Shaikh.
PROTO = os.environ.get("DIRECT_PROTO", "HTTP")
PROXRIPPER_URL = os.environ.get("DIRECT_URL", "")
SKIP_FIRST = int(os.environ.get("DIRECT_SKIP", "0") or 0)
MAX_PROXIES = int(os.environ.get("DIRECT_MAX", "5000") or 5000)
CONCURRENCY = int(os.environ.get("DIRECT_CONCURRENCY", "180") or 180)
