"""
Push attendance-based tags to Momence.

Usage:
  python3 push_segment_tags.py active
  python3 push_segment_tags.py recently_lapsed
  python3 push_segment_tags.py lapsed
  python3 push_segment_tags.py inactive

Running a segment push:
  - ADDs the tag to every qualifying member
  - REMOVEs the tag from every non-qualifying member
"""
import sys
import requests
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

CLIENT_ID     = "api-37946-FzZYzj12IkUd4ZFS"
CLIENT_SECRET = "TbTOG6vQNt3q1xeWMUrdPpuKWttHytxV"
USERNAME      = "cindyblair@hotmail.com"
PASSWORD      = "Grayhawk1047!!"
API           = "https://api.momence.com"

SUPABASE_URL      = "https://ustbzgdbjlitayhyxqqp.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVzdGJ6Z2RiamxpdGF5aHl4cXFwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njk0ODE2MzUsImV4cCI6MjA4NTA1NzYzNX0.9ecIlYBxraKpQzuoWRl7gt-DUuTkvD7RTljJqD6wypE"

SEGMENT_TAGS = {
    "active":          310935,  # Active Last 30 Days
    "recently_lapsed": 311068,  # Active Last 60 Days
    "lapsed":          311035,  # Active Last 12 Months
    "inactive":        321828,  # Not Active Last 12m
}

SEGMENT_LABELS = {
    "active":          "Active Last 30 Days",
    "recently_lapsed": "Active Last 60 Days",
    "lapsed":          "Active Last 12 Months",
    "inactive":        "Not Active Last 12m",
}

WORKERS = 10


def attendance_level(last_attended, total_visits: int) -> str:
    if not last_attended or total_visits == 0:
        return "inactive"
    dt = datetime.fromisoformat(last_attended.replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    days = (now - dt).days
    if days <= 30:
        return "active"
    if days <= 60:
        return "recently_lapsed"
    if days <= 365:
        return "lapsed"
    return "inactive"


def get_token() -> str:
    basic = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    r = requests.post(
        f"{API}/api/v2/auth/token",
        headers={"Content-Type": "application/x-www-form-urlencoded",
                 "Accept": "application/json",
                 "Authorization": f"Basic {basic}"},
        data={"grant_type": "password", "username": USERNAME, "password": PASSWORD},
    )
    r.raise_for_status()
    d = r.json()
    return d.get("accessToken") or d.get("access_token")


def get_all_members() -> list:
    hdrs = {"apikey": SUPABASE_ANON_KEY, "Authorization": f"Bearer {SUPABASE_ANON_KEY}"}
    members = []
    offset = 0
    while True:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/contact_segments"
            f"?select=momence_id,last_attended,total_visits"
            f"&momence_id=not.is.null&limit=1000&offset={offset}",
            headers=hdrs,
        )
        rows = r.json()
        if not rows:
            break
        members.extend(rows)
        if len(rows) < 1000:
            break
        offset += 1000
    return members


def add_tag(token: str, member_id: int, tag_id: int) -> str:
    r = requests.post(
        f"{API}/api/v2/host/members/{member_id}/tags/{tag_id}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=20,
    )
    if r.ok or r.status_code == 409:
        return "added"
    return f"error_{r.status_code}"


def remove_tag(token: str, member_id: int, tag_id: int) -> str:
    r = requests.delete(
        f"{API}/api/v2/host/members/{member_id}/tags/{tag_id}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=20,
    )
    if r.ok or r.status_code == 404:
        return "ok"
    return f"error_{r.status_code}"


def run_batch(token: str, items: list, tag_id: int, action: str, label: str):
    total = len(items)
    if total == 0:
        print(f"  No members to {action}.", flush=True)
        return 0, 0

    success = errors = done = 0
    fn = add_tag if action == "add" else remove_tag

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(fn, token, m["momence_id"], tag_id): m for m in items}
        for future in as_completed(futures):
            done += 1
            try:
                result = future.result()
                if result in ("added", "ok"):
                    success += 1
                else:
                    errors += 1
                    print(f"  [{done}/{total}] ✗ member {futures[future]['momence_id']} → {result}", flush=True)
            except Exception as e:
                errors += 1
                print(f"  [{done}/{total}] ERROR: {e}", flush=True)
            if done % 200 == 0:
                print(f"  [{done}/{total}] {label}…", flush=True)

    return success, errors


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in SEGMENT_TAGS:
        print("Usage: python3 push_segment_tags.py <segment>")
        print("Segments:", ", ".join(SEGMENT_TAGS))
        sys.exit(1)

    segment = sys.argv[1]
    tag_id = SEGMENT_TAGS[segment]
    label = SEGMENT_LABELS[segment]

    print("Authenticating...", flush=True)
    token = get_token()
    print("Token OK", flush=True)

    print("Fetching members from Supabase...", flush=True)
    all_members = get_all_members()
    print(f"  {len(all_members)} total members", flush=True)

    in_segment = []
    out_of_segment = []

    for m in all_members:
        level = attendance_level(m.get("last_attended"), m.get("total_visits") or 0)
        if level == segment:
            in_segment.append(m)
        else:
            out_of_segment.append(m)

    print(f"\n  {len(in_segment)} members qualify ({segment}) → '{label}'", flush=True)
    print(f"  {len(out_of_segment)} members do not qualify → remove tag\n", flush=True)

    print(f"── '{label}' (id={tag_id}) ──", flush=True)
    print(f"   Adding to {len(in_segment)} members...", flush=True)
    added, add_err = run_batch(token, in_segment, tag_id, "add", f"adding {label}")

    print(f"   Removing from {len(out_of_segment)} other members...", flush=True)
    removed, rem_err = run_batch(token, out_of_segment, tag_id, "remove", f"removing {label}")

    print("\n=== COMPLETE ===")
    print(f"Segment:      {segment}")
    print(f"Tags added:   {added}  (errors: {add_err})")
    print(f"Tags removed: {removed}  (errors: {rem_err})")


if __name__ == "__main__":
    main()
