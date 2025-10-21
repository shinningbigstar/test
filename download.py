import io
import os
import time
from datetime import datetime
import requests
import pandas as pd
from requests.adapters import HTTPAdapter, Retry

# Socrata dataset CSV endpoint
BASE_URL = "https://data.cityofchicago.org/resource/ajtu-isnz.csv"

# Pagination settings
LIMIT = 50000  # Socrata supports up to 200000 per request for many datasets

# Select columns from the Chicago taxi dataset (from the provided URL)
SELECT = (
    "trip_id,taxi_id,trip_start_timestamp,trip_end_timestamp,trip_seconds,trip_miles,"
    "pickup_census_tract,dropoff_census_tract,pickup_community_area,dropoff_community_area,"
    "fare,tips,tolls,extras,trip_total,payment_type,company,"
    "pickup_centroid_latitude,pickup_centroid_longitude,pickup_centroid_location,"
    "dropoff_centroid_latitude,dropoff_centroid_longitude,dropoff_centroid_location"
)

# WHERE clause (from the provided URL)
WHERE = (
    "(trip_start_timestamp > '2024-01-01' AND trip_start_timestamp IS NOT NULL) "
    "AND (trip_end_timestamp < '2024-12-31' AND trip_end_timestamp IS NOT NULL)"
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
    return pd.read_csv(io.StringIO(resp.text))


def download_all(out_path="full_dataset.csv"):
    sess = make_session()
    offset = 2900000
    first_chunk = True
    while True:
        print(datetime.now())
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
        print(f" - received {len(df)} rows")

        # Persist this chunk immediately to disk to save progress and memory.
        if first_chunk:
            # write header on first chunk (overwrite any existing file)
            df.to_csv(out_path, index=False, mode='w')
            first_chunk = False
        else:
            # append without header
            df.to_csv(out_path, index=False, header=False, mode='a')

        # If fewer than limit rows returned, we're done
        if len(df) < LIMIT:
            print("Last chunk received (< LIMIT). Download complete.")
            break

        offset += LIMIT
        # be polite to the API
        time.sleep(0.5)
    if first_chunk:
        # No data was written
        print("No data downloaded.")
        return

    # At this point all chunks have been saved to out_path incrementally
    size = os.path.getsize(out_path) if os.path.exists(out_path) else 0
    print(f"Download finished. Data saved to {out_path} ({size} bytes)")


if __name__ == "__main__":
    download_all("cityofchicago_taxi_data_2024.csv")