"""
EIE Broker Feed Generator
Generates a JSON feed of all EIE brokers with their next available booking slot.
Run every hour via GitHub Actions or crontab.
"""

import requests
import json
import time
import re
from datetime import datetime, timedelta

# ── CONFIG ────────────────────────────────────────────────────────────────────

SUPABASE_URL = "https://avtpaqeyepgnsirkyjco.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF2dHBhcWV5ZXBnbnNpcmt5amNvIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjAwMjQxMTEsImV4cCI6MjA3NTYwMDExMX0.WwlUvuRBRdLXx86CbK4-lTV8faFLex12CjOylFGGD7Y"

SUPABASE_HEADERS = {
    "apikey": SUPABASE_ANON_KEY,
    "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    "Content-Type": "application/json",
}

PHOTO_BASE_URL = f"{SUPABASE_URL}/storage/v1/object/public/office-product-photos"
EIE_OFFICES_BASE = "https://eie.no/eiendom/kontorer"
BOOKING_BASE_URL = "https://booking.eie.no"
CALENDAR_ENDPOINT = f"{SUPABASE_URL}/functions/v1/get-calendar"

LOOKAHEAD_DAYS = 14
REQUEST_DELAY = 0.3
OUTPUT_FILE = "feed.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "no,en;q=0.9",
}


# ── OFFICE NAME → URL SLUG ────────────────────────────────────────────────────

def office_name_to_slug(name):
    """
    Converts office name to URL slug matching eie.no/eiendom/kontorer/{slug}
    "EIE Majorstuen & St. Hanshaugen" -> "eie-majorstuen-st-hanshaugen"
    """
    s = name.lower()
    # Norwegian characters
    s = s.replace("æ", "ae").replace("ø", "o").replace("å", "a")
    # Remove dots, ampersands, and other punctuation
    s = re.sub(r"[&.,/\\']", " ", s)
    # Replace any non-alphanumeric (except spaces) with space
    s = re.sub(r"[^a-z0-9\s-]", "", s)
    # Collapse whitespace to single hyphen
    s = re.sub(r"\s+", "-", s.strip())
    # Collapse multiple hyphens
    s = re.sub(r"-+", "-", s)
    return s


def build_office_url(office_name):
    if not office_name:
        return None
    slug = office_name_to_slug(office_name)
    return f"{EIE_OFFICES_BASE}/{slug}"


# ── FETCH ALL BROKERS ─────────────────────────────────────────────────────────

def fetch_all_brokers():
    print("\nFetching all brokers from Supabase...")

    params = {
        "select": "id,name,email,phone,slug,external_id,active,office_id,office:office_id(id,name,external_id),photo:photo_id(path)",
        "active": "eq.true",
        "order": "name.asc",
    }

    try:
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/broker",
            headers=SUPABASE_HEADERS,
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        brokers = resp.json()
        print(f"  Fetched {len(brokers)} active brokers.")
        return brokers
    except requests.RequestException as e:
        print(f"  ERROR fetching brokers: {e}")
        return []


# ── FETCH NEXT AVAILABLE SLOT ─────────────────────────────────────────────────

def fetch_next_slot(broker_email):
    today = datetime.now().date()
    to_date = today + timedelta(days=LOOKAHEAD_DAYS)

    payload = {
        "email": broker_email,
        "from": str(today),
        "to": str(to_date),
    }

    try:
        resp = requests.post(
            CALENDAR_ENDPOINT,
            headers=SUPABASE_HEADERS,
            json=payload,
            timeout=15,
        )
        resp.raise_for_status()
        slots = resp.json()

        if not slots or not isinstance(slots, list):
            return None

        now = datetime.now()
        valid_slots = []
        for s in slots:
            start_str = s.get("start", "")
            if not start_str:
                continue
            try:
                dt = datetime.strptime(start_str, "%Y-%m-%d %H:%M")
                if dt <= now:
                    continue
                if dt.hour == 0 and dt.minute == 0:
                    continue
                valid_slots.append((dt, s))
            except ValueError:
                continue

        if not valid_slots:
            return None

        valid_slots.sort(key=lambda x: x[0])
        dt, first = valid_slots[0]

        return {
            "date": dt.strftime("%Y-%m-%d"),
            "time": dt.strftime("%H:%M"),
            "datetime_iso": dt.isoformat(),
            "slot_end": first.get("end", ""),
        }

    except requests.RequestException as e:
        print(f"    Calendar ERROR for {broker_email}: {e}")
        return None


# ── BUILD URLs ────────────────────────────────────────────────────────────────

def build_direct_booking_url(broker, slot, product_id=147):
    if not slot:
        return f"{BOOKING_BASE_URL}/department/{broker.get('_department_id', '')}"

    params = (
        f"brokerId={broker['id']}"
        f"&brokerName={requests.utils.quote(broker['name'])}"
        f"&brokerEmail={requests.utils.quote(broker['email'])}"
        f"&postalCode=0000"
        f"&products={product_id}"
        f"&date={slot['date']}"
        f"&time={slot['time']}"
        f"&duration=60"
    )
    return f"{BOOKING_BASE_URL}/booking/form?{params}"


def build_photo_url(photo_path):
    if not photo_path:
        return None
    return f"{PHOTO_BASE_URL}/{photo_path}"


# ── ASSEMBLE FEED ─────────────────────────────────────────────────────────────

def generate_feed():
    print("=" * 60)
    print("EIE Broker Feed Generator")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 60)

    brokers = fetch_all_brokers()
    if not brokers:
        print("No brokers found. Aborting.")
        return

    feed_brokers = []
    print(f"\nFetching calendar slots for {len(brokers)} brokers...")

    for i, broker in enumerate(brokers, 1):
        name = broker.get("name", "")
        email = broker.get("email", "")
        phone = broker.get("phone", "")
        broker_id = broker.get("id")
        external_id = broker.get("external_id")
        slug = broker.get("slug", "")

        office = broker.get("office") or {}
        office_name = office.get("name", "")
        department_id = office.get("external_id")

        photo = broker.get("photo") or {}
        photo_url = build_photo_url(photo.get("path", ""))

        office_url = build_office_url(office_name)

        print(f"  [{i}/{len(brokers)}] {name} ({email})", end=" ", flush=True)

        time.sleep(REQUEST_DELAY)
        slot = fetch_next_slot(email)

        if slot:
            print(f"-> {slot['date']} {slot['time']}")
        else:
            print("-> no slot found")

        broker["_department_id"] = department_id
        direct_booking_url = build_direct_booking_url(broker, slot)

        entry = {
            "id": broker_id,
            "external_id": external_id,
            "name": name,
            "email": email,
            "phone": phone,
            "slug": slug,
            "photo_url": photo_url,
            "office_name": office_name,
            "office_department_id": department_id,
            "office_url": office_url,
            "direct_booking_url": direct_booking_url,
            "next_slot": slot,
            "has_availability": slot is not None,
        }

        feed_brokers.append(entry)

    feed_brokers.sort(
        key=lambda b: (
            not b["has_availability"],
            b["next_slot"]["datetime_iso"] if b["next_slot"] else "9999",
        )
    )

    feed = {
        "generated_at": datetime.now().isoformat(),
        "lookahead_days": LOOKAHEAD_DAYS,
        "total_brokers": len(feed_brokers),
        "brokers_with_availability": sum(1 for b in feed_brokers if b["has_availability"]),
        "brokers": feed_brokers,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(feed, f, ensure_ascii=False, indent=2)

    print(f"\n✓ Feed written to {OUTPUT_FILE}")
    print(f"  Total brokers: {feed['total_brokers']}")
    print(f"  With availability: {feed['brokers_with_availability']}")
    print(f"  Generated at: {feed['generated_at']}")


if __name__ == "__main__":
    generate_feed()
