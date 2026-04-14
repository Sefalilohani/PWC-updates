import os
import time
import requests
from collections import defaultdict

REDASH_URL        = "https://redash.springworks.in"
QUERY_ID          = 2067
PER_QUERY_API_KEY = "aXlGyp4bEWhtlH2RC40R84W6ud9kOYe5G7UifSUh"
REDASH_REPORT_URL = f"{REDASH_URL}/queries/{QUERY_ID}#2888"

_raw_token    = os.environ["SLACK_BOT_TOKEN"]
SLACK_TOKEN   = "xoxb" + _raw_token[4:31] + "bFqMGfkmHBzvLRtU1It2ptnt"
SLACK_CHANNEL = "C096Y8SDH88"

SEV_ORDER = ["0-1", "2 - 3", "4 - 5", "6 - 7", "8 - 14", "15 - 30", "31 - 90", "90+"]

TAT_7_PLUS  = 7
TAT_10_PLUS = 10


def fetch_results():
    url = f"{REDASH_URL}/api/queries/{QUERY_ID}/results.json"
    resp = requests.get(url, params={"api_key": PER_QUERY_API_KEY, "max_age": 0}, timeout=60)
    print(f"GET {url} → {resp.status_code}")
    resp.raise_for_status()
    data = resp.json()

    if "job" in data:
        job_id = data["job"]["id"]
        print(f"Query running (job {job_id}), polling…")
        for _ in range(60):
            time.sleep(5)
            poll = requests.get(
                url,
                params={"api_key": PER_QUERY_API_KEY, "max_age": 30},
                timeout=60,
            )
            poll_data = poll.json()
            if "query_result" in poll_data:
                rows = poll_data["query_result"]["data"]["rows"]
                print(f"Got {len(rows)} rows after polling")
                return rows
        raise RuntimeError("Timed out waiting for Redash result")

    rows = data["query_result"]["data"]["rows"]
    print(f"Retrieved {len(rows)} rows from Redash")
    return rows


def build_pivot(rows):
    pivot = defaultdict(lambda: defaultdict(int))
    for row in rows:
        sev   = row.get("New Severity") or "0-1"
        combo = f"{row.get('Check Name','?')} | {row.get('Verification Type','N/A')}"
        pivot[sev][combo] += 1
    return pivot


def format_pivot_table(pivot):
    combos = sorted(set(c for sev_data in pivot.values() for c in sev_data))
    sevs   = [s for s in SEV_ORDER if s in pivot]

    sev_short = {
        "0-1": "0-1", "2 - 3": "2-3", "4 - 5": "4-5", "6 - 7": "6-7",
        "8 - 14": "8-14", "15 - 30": "15-30", "31 - 90": "31-90", "90+": "90+",
    }

    def abbrev(combo):
        check, _, vtype = combo.partition(" | ")
        short_check = {
            "Universal Account Number Check":              "UAN Check",
            "Moonlighting Check":                          "Moonlighting",
            "University Recognition check":                "Univ Recognition",
            "Social Media Lite":                           "Social Media",
            "Police Clearance Certificate Acknowledgement":"PCC Acknowledgement",
            "Police Clearance Certificate":                "PCC",
        }.get(check, check)
        short_vtype = {
            "DIGITAL":                        "Digital",
            "PHYSICAL":                       "Physical",
            "OFFICIAL":                       "Official",
            "REGIONAL_PARTNER":               "Regional",
            "UNIVERSAL_ACCOUNT_NUMBER_CHECK": "UAN",
        }.get(vtype, "")
        return f"{short_check} ({short_vtype})" if short_vtype else short_check

    labels = [abbrev(c) for c in combos]
    lw = max(25, max(len(l) for l in labels) + 2)
    sw = 7
    tw = 7

    sev_hdrs = [sev_short.get(s, s) for s in sevs]
    header = f"{'Check':<{lw}}" + "".join(f"{h:>{sw}}" for h in sev_hdrs) + f"{'Total':>{tw}}"
    sep    = "-" * len(header)

    lines = ["```", header, sep]
    grand_total = 0

    for label, combo in zip(labels, combos):
        row_total = sum(pivot[s].get(combo, 0) for s in sevs)
        grand_total += row_total
        cells = "".join(
            f"{pivot[s].get(combo, 0) or '-':>{sw}}" for s in sevs
        )
        lines.append(f"{label:<{lw}}{cells}{row_total:>{tw}}")

    lines.append(sep)
    col_tots = "".join(
        f"{sum(pivot[s].get(c, 0) for c in combos):>{sw}}" for s in sevs
    )
    lines.append(f"{'Total':<{lw}}{col_tots}{grand_total:>{tw}}")
    lines.append("```")

    return "\n".join(lines), grand_total


def compute_crossed_days(rows):
    counts_7  = defaultdict(int)
    counts_10 = defaultdict(int)
    for row in rows:
        check   = row.get("Check Name", "Unknown")
        net_tat = row.get("NET TAT")
        if net_tat is None:
            continue
        net_tat = float(net_tat)
        if net_tat >= TAT_7_PLUS:
            counts_7[check] += 1
        if net_tat >= TAT_10_PLUS:
            counts_10[check] += 1
    return counts_7, counts_10


def build_message(rows):
    pivot             = build_pivot(rows)
    table, total      = format_pivot_table(pivot)
    counts_7, counts_12 = compute_crossed_days(rows)

    bullet_lines = []
    for check in sorted(set(list(counts_7.keys()) + list(counts_12.keys()))):
        c7  = counts_7.get(check, 0)
        c12 = counts_12.get(check, 0)
        if c7 > 0:
            bullet_lines.append(f"• {c7} checks has crossed 7+ days in {check}")
        if c12 > 0:
            bullet_lines.append(f"• {c12} checks has crossed 12+ days in {check}")

    bullets = "\n".join(bullet_lines) if bullet_lines else "• No checks have crossed 7+ days"

    message = (
        f"*Update on PwC client In Progress checks*\n\n"
        f"{table}\n\n"
        f"{bullets}\n\n"
        f"*Total In-Progress checks: {total}*\n"
        f"<{REDASH_REPORT_URL}|View full report on Redash>"
    )
    return message


def send_slack(message):
    resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {SLACK_TOKEN}"},
        json={"channel": SLACK_CHANNEL, "text": message, "mrkdwn": True},
        timeout=15,
    )
    resp.raise_for_status()
    result = resp.json()
    if not result.get("ok"):
        raise RuntimeError(f"Slack error: {result.get('error')}")
    print("Message sent to Slack successfully")


if __name__ == "__main__":
    rows    = fetch_results()
    message = build_message(rows)
    print("--- Message preview ---")
    print(message)
    print("-----------------------")
    send_slack(message)
