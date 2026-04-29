"""
EIE Broker Feed Generator
Generates a JSON feed of all EIE brokers with their next available booking slot.
Run every hour via GitHub Actions or crontab.
"""

import requests
import json
import time
import re
import math
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
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

LOOKAHEAD_DAYS = 14
REQUEST_DELAY = 0.3
GEOCODE_DELAY = 1.1
OUTPUT_FILE = "feed.json"

HEADERS = {
    "User-Agent": "EIE-Feed-Generator/1.0 (marops.no)",
    "Accept-Language": "no,en;q=0.9",
}

MAX_RADIUS_KM = 80
MIN_RADIUS_KM = 10

# ── ADDRESS OVERRIDES ─────────────────────────────────────────────────────────
# Complete address list from eie.no/eiendom/kontorer
# Key = exact office name in Supabase, Value = address with zip from known data

ADDRESS_OVERRIDES = {
    "EIE Asker":                        "Kirkeveien 212, 1383 Asker",
    "EIE Bergen":                       "Edvard Griegs vei 3 E, 5059 Bergen",
    "EIE bolig- og prosjektmegling":    "Karenslyst allé 49, 0279 Oslo",
    "EIE Drammen":                      "Nedre Storgate 13, 3015 Drammen",
    "EIE Ensjø":                        "Gladengveien 24, 0661 Oslo",
    "EIE Fjellmegleren Beitostølen":    "Skifervegen 4, 2900 Fagernes",
    "EIE Fjellmegleren Fagernes":       "Skifervegen 4, 2900 Fagernes",
    "EIE Fjellmegleren Geilo":          "Geilovegen 34, 3580 Geilo",
    "EIE Fjellmegleren Gol":            "Sentrumsvegen 79, 3550 Gol",
    "EIE Fjellmegleren Hemsedal":       "Høvlerivegen 4, 3560 Hemsedal",
    "Fjellmegleren Hemsedal":           "Høvlerivegen 4, 3560 Hemsedal",
    "EIE Fjellmegleren Nesbyen & Flå":  "Alfarvegen 115 A, 3540 Nesbyen",
    "Fjellmegleren Nesbyen og Flå":     "Alfarvegen 115 A, 3540 Nesbyen",
    "EIE Follo":                        "Haugenveien 5, 1423 Ski",
    "EIE Fredensborg":                  "Slemdalsveien 70 B, 0370 Oslo",
    "EIE Hamar":                        "Seminargata 3, 2317 Hamar",
    "EIE Harstad":                      "Hans Egedes gate 2B, 9405 Harstad",
    "EIE Heimdal":                      "Industriveien 21, 7080 Heimdal",
    "EIE Hitra & Frøya":               "Sentrumsgata 8, 7240 Hitra",
    "EIE Jessheim":                     "Saggata 7, 2053 Jessheim",
    "EIE Larvik & Lågendalen":         "Kongegata 20 B, 3256 Larvik",
    "EIE Lillestrøm":                   "Storgata 29, 2000 Lillestrøm",
    "EIE Linderud":                     "Erich Mogensøns vei 38, 0594 Oslo",
    "EIE Løren & Økern":               "Lørenveien 41 D, 0585 Oslo",
    "EIE Lørenskog":                    "Snøkrystallen 9, 1470 Lørenskog",
    "EIE Majorstuen & St. Hanshaugen": "Hegdehaugsveien 24, 0352 Oslo",
    "EIE Mo i Rana":                    "Postboks 1074, 8602 Mo i Rana",
    "EIE Molde":                        "Storgata 25, 6413 Molde",
    "EIE nybygg Stavanger Sandnes":     "Niels Juels gate 50, 4008 Stavanger",
    "EIE Ringsaker":                    "Brugata 3, 2380 Brumunddal",
    "EIE Røa, Skøyen & Ullern":        "Griniveien 10, 0756 Oslo",
    "EIE Råholt":                       "Trondheimsvegen 266, 2070 Råholt",
    "EIE Sandefjord":                   "Thor Dahls gate 1-5, 3210 Sandefjord",
    "EIE Sandnes":                      "Eidsvollgata 47, 4307 Sandnes",
    "EIE Sandvika":                     "Kinoveien 9 A, 1337 Sandvika",
    "EIE Sinsen & Carl Berner":         "Trondheimsveien 153-155, 0570 Oslo",
    "EIE Solli & Frogner":             "Inkognitogata 24, 0256 Oslo",
    "EIE Stabekk":                      "Gamle Drammensvei 45, 1369 Stabekk",
    "EIE Stavanger":                    "Niels Juels gate 50, 4008 Stavanger",
    "EIE Stokke":                       "Frederik Stangs gate 1, 3160 Stokke",
    "EIE Stovner":                      "Stovner senter 3, 0913 Oslo",
    "EIE Torshov & Nydalen":           "Sandakerveien 56 H, 0477 Oslo",
    "EIE Tromsø":                       "Grønnegata 30, 9008 Tromsø",
    "EIE Trondheim sentrum":            "Beddingen 14, 7042 Trondheim",
    "EIE Tønsberg":                     "Nedre Langgate 19, 3126 Tønsberg",
    "EIE Ullevål & Sagene":            "Sognsveien 77 C, 0855 Oslo",
    "EIE Valkyrien":                    "Bogstadveien 66 B, 0366 Oslo",
    "EIE Vesterålen":                   "Gårdsalléen 8 B, 8400 Sortland",
    "EIE Vinderen":                     "Slemdalsveien 70 B, 0370 Oslo",
    "EIE Vinderen & Fredensborg":       "Slemdalsveien 70 B, 0370 Oslo",
    "EIE Ålesund":                      "Lorkenesgata 3, 6002 Ålesund",
    "EIE Fornebu":                      "Solgangsbrisen 5, 1364 Fornebu",
    "EIE Kongsvinger":                  "Røde Korsvegen 4, 2208 Kongsvinger",
    "Aukra":                            "Nærøyvegen 2, 6480 Aukra",
    "Hustadvika":                       "Storgata 17, 6440 Elnesvågen",
}

# ── NORWEGIAN DATE HELPERS ────────────────────────────────────────────────────

WEEKDAYS_NO = {
    0: "mandag", 1: "tirsdag", 2: "onsdag",
    3: "torsdag", 4: "fredag", 5: "lørdag", 6: "søndag"
}
MONTHS_NO = {
    1: "januar", 2: "februar", 3: "mars", 4: "april",
    5: "mai", 6: "juni", 7: "juli", 8: "august",
    9: "september", 10: "oktober", 11: "november", 12: "desember"
}

def format_date_no(dt):
    return f"{WEEKDAYS_NO[dt.weekday()]} {dt.day}. {MONTHS_NO[dt.month]}"

def format_date_short_no(dt):
    return f"{dt.day}. {MONTHS_NO[dt.month][:3]}"


# ── REGION MAPPING ────────────────────────────────────────────────────────────

ZIP_REGION_MAP = {
    **{str(i).zfill(2): "Østlandet" for i in list(range(0, 20)) + list(range(30, 37))},
    **{str(i).zfill(2): "Innlandet" for i in range(20, 30)},
    **{str(i).zfill(2): "Sørlandet" for i in range(37, 46)},
    **{str(i).zfill(2): "Vestlandet" for i in range(46, 63)},
    **{str(i).zfill(2): "Midt-Norge" for i in range(63, 70)},
    **{str(i).zfill(2): "Trøndelag" for i in range(70, 75)},
    **{str(i).zfill(2): "Nord-Norge" for i in range(75, 100)},
}

def get_region_from_zip(zip_code):
    if not zip_code or len(zip_code) < 2:
        return None
    return ZIP_REGION_MAP.get(zip_code[:2])

def extract_zip_from_address(address):
    if not address:
        return None
    match = re.search(r'\b(\d{4})\b', address)
    return match.group(1) if match else None

def extract_city_from_address(address):
    if not address:
        return None
    match = re.search(r'\d{4}\s+([A-ZÆØÅ][A-ZÆØÅa-zæøå\s\-]+?)(?:\s*,|$)', address)
    if match:
        return match.group(1).strip().title()
    return None

def is_valid_address(address):
    if not address:
        return False
    stripped = address.strip()
    if stripped in ("-", "", "–", "—"):
        return False
    if not re.search(r'\d{4}', stripped):
        return False
    return True


# ── GEOCODING ─────────────────────────────────────────────────────────────────

def nominatim_query(query):
    try:
        time.sleep(GEOCODE_DELAY)
        params = {"q": query, "format": "json", "limit": 1, "countrycodes": "no"}
        resp = requests.get(NOMINATIM_URL, params=params, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        results = resp.json()
        if results:
            return float(results[0]["lat"]), float(results[0]["lon"])
    except Exception as e:
        print(f"    Geocode error for '{query}': {e}")
    return None, None

def geocode_address(address):
    if not is_valid_address(address):
        return None, None

    zip_code = extract_zip_from_address(address)
    city = extract_city_from_address(address)
    queries = []

    if address.lower().startswith("postboks"):
        if zip_code and city:
            queries.append(f"{zip_code} {city}, Norway")
    else:
        queries.append(f"{address}, Norway")
        street = re.split(r',', address)[0].strip()
        if city:
            queries.append(f"{street}, {city}, Norway")

    if zip_code and city:
        queries.append(f"{zip_code} {city}, Norway")
    elif zip_code:
        queries.append(f"{zip_code}, Norway")

    for query in queries:
        lat, lon = nominatim_query(query)
        if lat:
            return lat, lon

    return None, None


# ── GEO MATH ──────────────────────────────────────────────────────────────────

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat/2)**2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2)
    return R * 2 * math.asin(math.sqrt(a))

def compute_radii(offices_geo):
    ids = list(offices_geo.keys())
    radii = {}
    for oid in ids:
        lat1, lon1 = offices_geo[oid]
        if lat1 is None:
            radii[oid] = MAX_RADIUS_KM
            continue
        min_dist = float("inf")
        for other_id in ids:
            if other_id == oid:
                continue
            lat2, lon2 = offices_geo[other_id]
            if lat2 is None:
                continue
            d = haversine_km(lat1, lon1, lat2, lon2)
            if d < min_dist:
                min_dist = d
        radius = MAX_RADIUS_KM if min_dist == float("inf") else round(min(max(min_dist / 2, MIN_RADIUS_KM), MAX_RADIUS_KM), 1)
        radii[oid] = radius
    return radii


# ── FETCH ALL OFFICES ─────────────────────────────────────────────────────────

def fetch_all_offices():
    print("Fetching all offices from Supabase...")
    params = {"select": "id,name,address,external_id,zip_codes", "active": "eq.true"}
    try:
        resp = requests.get(f"{SUPABASE_URL}/rest/v1/office", headers=SUPABASE_HEADERS, params=params, timeout=30)
        resp.raise_for_status()
        offices = resp.json()
        print(f"  Fetched {len(offices)} offices.")
        return offices
    except requests.RequestException as e:
        print(f"  ERROR: {e}")
        return []


# ── FETCH ALL BROKERS ─────────────────────────────────────────────────────────

def fetch_all_brokers():
    print("\nFetching all brokers from Supabase...")
    params = {
        "select": "id,name,email,phone,slug,external_id,active,office_id,office:office_id(id,name,external_id),photo:photo_id(path)",
        "active": "eq.true",
        "order": "name.asc",
    }
    try:
        resp = requests.get(f"{SUPABASE_URL}/rest/v1/broker", headers=SUPABASE_HEADERS, params=params, timeout=30)
        resp.raise_for_status()
        brokers = resp.json()
        print(f"  Fetched {len(brokers)} active brokers.")
        return brokers
    except requests.RequestException as e:
        print(f"  ERROR: {e}")
        return []


# ── FETCH NEXT SLOT ───────────────────────────────────────────────────────────

def fetch_next_slot(broker_email):
    today = datetime.now().date()
    payload = {
        "email": broker_email,
        "from": str(today),
        "to": str(today + timedelta(days=LOOKAHEAD_DAYS)),
    }
    try:
        resp = requests.post(CALENDAR_ENDPOINT, headers=SUPABASE_HEADERS, json=payload, timeout=15)
        resp.raise_for_status()
        slots = resp.json()
        if not slots or not isinstance(slots, list):
            return None
        now = datetime.now()
        valid = []
        for s in slots:
            start_str = s.get("start", "")
            if not start_str:
                continue
            try:
                dt = datetime.strptime(start_str, "%Y-%m-%d %H:%M")
                if dt <= now or (dt.hour == 0 and dt.minute == 0):
                    continue
                valid.append((dt, s))
            except ValueError:
                continue
        if not valid:
            return None
        valid.sort(key=lambda x: x[0])
        dt, first = valid[0]
        return {
            "date": dt.strftime("%Y-%m-%d"),
            "time": dt.strftime("%H:%M"),
            "datetime_iso": dt.isoformat(),
            "slot_end": first.get("end", ""),
        }
    except requests.RequestException as e:
        print(f"    Calendar ERROR for {broker_email}: {e}")
        return None


# ── HELPERS ───────────────────────────────────────────────────────────────────

def office_name_to_slug(name):
    s = name.lower()
    s = s.replace("æ", "ae").replace("ø", "o").replace("å", "a")
    s = re.sub(r"[&.,/\\']", " ", s)
    s = re.sub(r"[^a-z0-9\s-]", "", s)
    s = re.sub(r"\s+", "-", s.strip())
    return re.sub(r"-+", "-", s)

def build_office_url(office_name):
    return f"{EIE_OFFICES_BASE}/{office_name_to_slug(office_name)}" if office_name else None

def build_direct_booking_url(broker, slot, product_id=147):
    if not slot:
        return f"{BOOKING_BASE_URL}/department/{broker.get('_department_id', '')}"
    params = (
        f"brokerId={broker['id']}"
        f"&brokerName={requests.utils.quote(broker['name'])}"
        f"&brokerEmail={requests.utils.quote(broker['email'])}"
        f"&postalCode=0000&products={product_id}"
        f"&date={slot['date']}&time={slot['time']}&duration=60"
    )
    return f"{BOOKING_BASE_URL}/booking/form?{params}"

def build_photo_url(path):
    return f"{PHOTO_BASE_URL}/{path}" if path else None

def build_title(office_name, slot):
    if not slot:
        return f"Book megler – {office_name}"
    dt = datetime.fromisoformat(slot["datetime_iso"])
    return f"Neste ledige tid – {office_name} – {format_date_short_no(dt)} kl. {slot['time']}"

def build_description(office_name, broker_name, slot):
    if not slot:
        return f"Book tid med din lokale megler hos {office_name}. Se ledige tider og finn en tid som passer deg."
    dt = datetime.fromisoformat(slot["datetime_iso"])
    return (
        f"Book tid med din lokale megler! "
        f"Neste ledige tid hos {office_name} er {format_date_no(dt)} kl. {slot['time']} "
        f"med {broker_name}. Velkommen til en hyggelig prat."
    )


# ── MAIN ──────────────────────────────────────────────────────────────────────

def generate_feed():
    print("=" * 60)
    print("EIE Broker Feed Generator")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 60)

    offices = fetch_all_offices()
    office_lookup = {}
    offices_geo = {}

    print("\nGeocoding office addresses...")
    for office in offices:
        oid = office["id"]
        office_name = office["name"]

        # Override takes priority over Supabase address
        address = ADDRESS_OVERRIDES.get(office_name) or office.get("address", "")

        zip_codes = office.get("zip_codes") or []
        zip_code = extract_zip_from_address(address)
        city = extract_city_from_address(address)

        region = None
        if zip_codes:
            region = get_region_from_zip(zip_codes[0])
        if not region and zip_code:
            region = get_region_from_zip(zip_code)

        lat, lon = geocode_address(address)
        status = f"{lat:.4f}, {lon:.4f}" if lat else "skipped"
        print(f"  {office_name} -> {status}")

        offices_geo[oid] = (lat, lon)
        office_lookup[oid] = {
            "name": office_name,
            "address": address,
            "lat": lat, "lon": lon,
            "city": city, "region": region,
        }

    print("\nComputing coverage radii...")
    radii = compute_radii(offices_geo)

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
        office_supabase_id = office.get("id")
        department_id = office.get("external_id")

        photo = broker.get("photo") or {}
        photo_url = build_photo_url(photo.get("path", ""))

        od = office_lookup.get(office_supabase_id, {})
        lat = od.get("lat")
        lon = od.get("lon")
        city = od.get("city")
        region = od.get("region")
        radius_km = radii.get(office_supabase_id, MAX_RADIUS_KM)

        print(f"  [{i}/{len(brokers)}] {name} ({email})", end=" ", flush=True)

        time.sleep(REQUEST_DELAY)
        slot = fetch_next_slot(email)
        print(f"-> {slot['date']} {slot['time']}" if slot else "-> no slot")

        broker["_department_id"] = department_id

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
            "office_url": build_office_url(office_name),
            "latitude": lat,
            "longitude": lon,
            "geo": f"{lat},{lon}" if lat and lon else None,
            "radius_km": radius_km,
            "city": city,
            "region": region,
            "title": build_title(office_name, slot),
            "description": build_description(office_name, name, slot),
            "direct_booking_url": build_direct_booking_url(broker, slot),
            "next_slot": slot,
            "has_availability": slot is not None,
        }
        feed_brokers.append(entry)

    feed_brokers.sort(key=lambda b: (
        not b["has_availability"],
        b["next_slot"]["datetime_iso"] if b["next_slot"] else "9999",
    ))

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
