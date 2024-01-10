import pandas as pd
import requests
import json
from typing import List, Union, Dict, Optional
from time import sleep
import os
import argparse
import data_utils
import argparse

HEADERS = {
    "accept": "application/json",
    # TODO: remove hardcoded environment variable
    "Authorization": "Bearer i9tXrN9wOtM9pRRl9xO9eiUjgN01mfSKIelJ9NhFrmFJ-lc8cDZuFZrQO_QF5d8jvZ5EYTri4ZrTelZ0sOZHJ7zLIpB8Bb1KE8iKtKTaCB3aYlulmqVIwqSva-iOZXYx",
}


# %%
# fns
def craft_review_url(row: pd.Series) -> str:
    """Foramts URL to hit the yelp reviews API given a row from the
        exploded yelp businesses data.
    Args:
        row (pd.Series):

    Returns:
        str:
    """
    # url = f'https://api.yelp.com/v3/businesses/{row.id}/review_highlights?count=3'
    url = f"https://api.yelp.com/v3/businesses/{row.id}/reviews?offset={row.offset}&limit=20&sort_by=newest"
    return url


def multiples_of_k(n, k=20):
    result = [i for i in range(0, n, k)]
    return result


def explode_df(
    yelp_df,
):
    yelp_df["offset"] = (
        yelp_df["review_count"].fillna(0).astype(int).apply(multiples_of_k)
    )
    exploded = yelp_df.explode(column="offset")
    return exploded


def get_yelp_review_repsonses(row: pd.Series, sleeptime: int = 0) -> List:
    """
    sleeptime: seconds to sleep between requests. Default: 0
    NOTE: this API only returns places with existing reviews
    This should include closed places.
    """
    response = requests.get(row.review_url, headers=HEADERS)
    sleep(sleeptime)
    return response


def main(start_index: int) -> None:
    df = pd.read_csv(f"{data_utils.YELP_BIZ_RESPONSES_AGG_DIR}/biz_agg1.csv")
    df = df.set_index("School Code")
    yelp_df = df[df.yelp_entry_found]

    exploded = explode_df(yelp_df)
    exploded = exploded.reset_index()
    exploded["review_url"] = exploded.apply(craft_review_url, axis=1)

    # currently only 1800 rows... so hoping we can keep doing the hack of hard coding offset.
    offset_per_day = 400
    end_index = start_index + offset_per_day

    head = exploded.iloc[start_index:end_index]
    # fetch yelp responses and parse
    head["responses"] = head.apply(get_yelp_review_repsonses, axis=1)
    head["response_dct"] = head["responses"].apply(lambda x: json.loads(x.text))
    head["reviews"] = head["response_dct"].apply(lambda x: x.get("reviews", []))
    exploded_reviews = head.explode("reviews").reset_index()
    rev_raw = pd.json_normalize(exploded_reviews["reviews"])
    keep_cols = [
        "School Code",
        "filename",
        "yelp_entry_found",
        "review_url",
        "offset",
    ]

    merged_df = exploded_reviews[keep_cols].merge(
        rev_raw, left_index=True, right_index=True
    )
    os.makedirs(data_utils.YELP_REV_RESPONSES_RAW_DIR, exist_ok=True)
    merged_df.to_csv(
        f"{data_utils.YELP_REV_RESPONSES_RAW_DIR}/{start_index}_{end_index}.csv"
    )


# %%
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--start_index",
        type=int,
        help="index to start at for fetching yelp reviews",
    )
    args = parser.parse_args()
    main(args.start_index)
