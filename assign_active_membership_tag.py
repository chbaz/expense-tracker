import requests, base64, time
from concurrent.futures import ThreadPoolExecutor, as_completed

SUPABASE_URL = "https://ustbzgdbjlitayhyxqqp.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVzdGJ6Z2RiamxpdGF5aHl4cXFwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njk0ODE2MzUsImV4cCI6MjA4NTA1NzYzNX0.9ecIlYBxraKpQzuoWRl7gt-DUuTkvD7RTljJqD6wypE"

CLIENT_ID = "api-37946-FzZYzj12IkUd4ZFS"
CLIENT_SECRET = "TbTOG6vQNt3q1xeWMUrdPpuKWttHytxV"
USERNAME = "cindyblair@hotmail.com"
PASSWORD = "Grayhawk1047!!"
API = "https://api.momence.com"

HAS_ACTIVE_TAG_ID = 294614

PLAN_IDS = [
    207255, 314710, 421749, 211128, 314814, 558439,
    648765, 648766, 648772,
    584144, 584150, 584147,
    724529, 724528, 648702,
    626248, 262722,
    490774, 490782, 491250, 494886,
    390012,
]


def get_token() -> str:
    basic = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    r = requests.post(
        f"{API}/api/v2/auth/token",
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "Authorization": f"Basic {basic}",
        },
        data={"grant_type": "password", "username": USERNAME, "password": PASSWORD},
    )
    r.raise_for_status()
    d = r.json()
    return d.get("accessToken") or d.get("access_token")


def assign_tag(token: str, member_id: int) -> str:
    r = requests.post(
        f"{API}/api/v2/host/members/{member_id}/tags/{HAS_ACTIVE_TAG_ID}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    if r.status_code in (200, 201):
        return "added"
    if r.status_code == 409:
        return "already_had"
    return f"error_{r.status_code}"


def sync_member_ids_to_supabase(member_ids: list[int]) -> None:
    """Replace the active_membership_members table with the current set of member IDs."""
    hdrs = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    # Delete all existing rows
    requests.delete(
        f"{SUPABASE_URL}/rest/v1/active_membership_members?member_id=gte.0",
        headers=hdrs,
    )
    # Insert current set in chunks of 500
    for i in range(0, len(member_ids), 500):
        chunk = [{"member_id": mid} for mid in member_ids[i:i+500]]
        requests.post(
            f"{SUPABASE_URL}/rest/v1/active_membership_members",
            headers={**hdrs, "Prefer": "resolution=merge-duplicates"},
            json=chunk,
        )
    print(f"  Supabase table updated with {len(member_ids)} member IDs.", flush=True)


def main() -> None:
    print("Authenticating...", flush=True)
    token = get_token()
    print("Token OK", flush=True)

    # The member IDs are hardcoded below (last refreshed from browser on 2026-05-01).
    # To get fresh IDs: open momence.com in Chrome, open DevTools console, and run
    # the JS snippet in fetch_active_members.js, then paste the result here.
    member_ids = [
        19666498,258819,20109763,19052774,19052887,19052753,19052874,20107817,19053013,
        19052883,17388435,25444724,19052943,15985744,16007330,23157382,20110141,19052817,
        23082758,17671268,19553987,22203807,19358252,22557557,2376938,23445990,29455680,
        19416759,19052971,19052917,20116562,19052873,16791102,19052981,16039168,16037337,
        20959063,20239284,22653408,20114894,19242745,18390566,19052844,19438396,22652856,
        19042288,22639864,16000993,20119090,25489365,2223123,25255912,28484700,23407062,
        25316436,26567066,20238535,27283582,20116802,12250836,20111609,20359594,27329462,
        19052975,29834270,29250488,23783097,20645971,20125537,20062130,19052826,19052884,
        16035014,20120575,12029908,236952,22530185,16028917,29752604,13273207,30638713,
        20111720,16001842,30649937,16010108,20110259,25669656,26681623,22674468,26083162,
        28735686,28735663,20119068,23856962,22745027,22774830,28869005,2223502,24569708,
        25110160,22632316,23203205,28780823,15996219,20108347,20112703,15301442,20108868,
        19052992,22536746,15986077,30031373,20112053,23175463,2223584,16000969,20113932,
        16001121,30960814,30916544,22576841,19585201,20116748,12343159,13166380,19701797,
        15998699,28545615,19052888,20115152,20111886,1368569,20113556,19577914,20109719,
        16030840,29718544,20110268,23403272,20112037,20118056,20346815,16036406,20116517,
        20113147,19355758,15833236,14841639,21985096,20111942,20116546,16003041,15998100,
        31254353,26530662,29443130,26162161,31055791,31055814,30995646,16081955,29710703,
        26160557,29864268,20115271,17174450,29750335,30058879,30358596,27340828,30528700,
        30621471,31431912,19052781,31431371,31409039,13588597,20116347,31142162,20111862,
        31141000,30787308,20640372,22698917,20120453,25243425,21121345,20111192,24561007,
        24275314,26061895,20112639,24879476,26888531,20118555,20110878,20113579,20113182,
        21088824,20112033,20115484,20117549,20740177,16006262,20116211,20113970,854248,
        20110473,20108431,20112209,20113956,26878531,20110858,20116822,20117921,20112386,
        20112387,27780168,14572348,16033591,24631665,19621098,20114743,20111106,16007884,
        20114212,25129612,21480776,20114724,22359119,20119570,20108511,16824672,20109106,
        20115702,16278969,20113366,20113061,20115188,23345014,
    ]

    total = len(member_ids)
    print(f"\nAssigning 'Has Active Membership' tag to {total} members...\n", flush=True)

    added = already = errors = 0
    done = 0

    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(assign_tag, token, mid): mid for mid in member_ids}
        for future in as_completed(futures):
            done += 1
            mid = futures[future]
            try:
                result = future.result()
                if result == "added":
                    added += 1
                    print(f"  [{done}/{total}] ✓ member {mid} → tag assigned", flush=True)
                elif result == "already_had":
                    already += 1
                    if done % 50 == 0:
                        print(f"  [{done}/{total}] still working...", flush=True)
                else:
                    errors += 1
                    print(f"  [{done}/{total}] ✗ member {mid} → {result}", flush=True)
            except Exception as e:
                errors += 1
                print(f"  [{done}/{total}] ERROR member {mid}: {e}", flush=True)

    print(f"\n=== DONE ===")
    print(f"Members processed: {total}")
    print(f"Tags assigned:     {added}")
    print(f"Already had tag:   {already}")
    print(f"Errors:            {errors}")

    print(f"\nUpdating Supabase table...", flush=True)
    sync_member_ids_to_supabase(member_ids)


if __name__ == "__main__":
    main()
