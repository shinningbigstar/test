import io
import time
import requests
import pandas as pd
from requests.adapters import HTTPAdapter, Retry

# Socrata dataset CSV endpoint
BASE_URL = "https://data.sfgov.org/resource/m8hk-2ipk.csv"

# Pagination settings
LIMIT = 50000  # Socrata supports up to 50000 per request for many datasets

# Select columns (kept from original query)
SELECT = (
    "vehicle_placard_number,driver_id,start_time_local,end_time_local,"
    "pickup_location_latitude,pickup_location_longitude,pickup_location,"
    "dropoff_location_latitude,dropoff_location_longitude,dropoff_location,"
    "hail_type,paratransit,sfo_pickup,qa_flags,fare_type,meter_fare_amount,"
    "upfront_pricing,promo_rate,tolls,sf_exit_fee,other_fees,tip,extra_amount,"
    "total_fare_amount,fare_time_milliseconds,trip_distance_meters,data_as_of,data_loaded_at"
)

# WHERE clause (same filters as original script)
WHERE = (
    "(start_time_local > '2024-01-01' AND start_time_local IS NOT NULL) "
    "AND (end_time_local < '2024-12-31' AND end_time_local IS NOT NULL)"
)


def make_session(retries=3, backoff_factor=0.3):
    s = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(['GET', 'POST'])
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount('https://', adapter)
    s.mount('http://', adapter)
    return s


def fetch_chunk(session, offset, limit=LIMIT):
    params = {
        "$select": SELECT,
        "$where": WHERE,
        "$limit": str(limit),
        "$offset": str(offset),
    }
    resp = session.get(BASE_URL, params=params, timeout=60)
    resp.raise_for_status()
    # read CSV from response content
    # specify dtypes for columns that may have mixed types to avoid DtypeWarning
    dtype_map = {
        "vehicle_placard_number": str,
        "driver_id": str,
    }
    try:
        return pd.read_csv(io.StringIO(resp.text), dtype=dtype_map, low_memory=False)
    except Exception:
        # Fall back to a more permissive read to avoid crashing; caller will handle empty frames
        return pd.read_csv(io.StringIO(resp.text), low_memory=False)


def download_all(out_path="full_dataset.csv"):
    sess = make_session()
    offset = 0
    chunks = []
    while True:
        print(f"Fetching rows offset={offset} limit={LIMIT}...")
        try:
            df = fetch_chunk(sess, offset, LIMIT)
        except requests.HTTPError as e:
            print(f"HTTP error while fetching offset {offset}: {e}")
            break
        except requests.RequestException as e:
            print(f"Network error while fetching offset {offset}: {e}")
            break

        if df.empty:
            print("No more rows returned by the API. Stopping.")
            break

        chunks.append(df)
        print(f" - received {len(df)} rows")
        # If fewer than limit rows returned, we're done
        if len(df) < LIMIT:
            break

        offset += LIMIT
        # be polite to the API
        time.sleep(0.5)

    if not chunks:
        print("No data downloaded.")
        return

    full_df = pd.concat(chunks, ignore_index=True)
    full_df.to_csv(out_path, index=False)
    print(f"Downloaded {len(full_df)} rows to {out_path}")


if __name__ == "__main__":
    download_all("sfgov_taxi_data_2024.csv")
