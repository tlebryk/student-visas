# %%
import pandas as pd
import requests
import json
from typing import List, Union, Dict, Optional
from time import sleep
from thefuzz import fuzz
import os
import argparse
import data_utils

# %%
# globals
# TODO: token rotation?
YELP_API_KEY = os.getenv("YELP_API_KEY")
HEADERS = {
    "accept": "application/json",
    # TODO: remove hardcoded environment variable
    "Authorization": f"Bearer {YELP_API_KEY}",
}


# %%
# fns
def craft_url(row: pd.Series) -> str:
    """Foramts URL to hit the yelp API given a row of the original school CSV

    Args:
        row (pd.Series):

    Returns:
        str:
    """
    url = f"https://api.yelp.com/v3/businesses/search?latitude={row.lat}&longitude={row.lng}&term={row.school_name}&sort_by=best_match&limit=5"
    return url


def get_yelp_biz_repsonses(row: pd.Series, sleeptime: int = 0) -> List:
    """
    sleeptime: seconds to sleep between requests. Default: 0
    NOTE: this API only returns places with existing reviews
    This should include closed places.
    """
    response = requests.get(row.url, headers=HEADERS)
    sleep(sleeptime)
    return response


def get_matches(df: pd.DataFrame, ratio_thres: int = 50) -> pd.Series:
    """Checks match columns are all true
    (boolean columns for exact match between original data and yelp entry found)
    and ratio columns (fuzzy match columns between OG data and yelp entry)
    are over some ratio_threshold.
    """
    match_check = df.filter(regex="match").all(axis=1)
    ratio_check = df.filter(regex="ratio").gt(ratio_thres).all(axis=1)
    return match_check & ratio_check


# %%
def main(start_index: int) -> None:
    df = pd.read_csv(data_utils.LOCAL_PATH)
    # hit_yelp_once simply means we tried one time, not that we are successful yet.
    offset_per_day = 400
    end_index = start_index + offset_per_day
    df["url"] = df.apply(craft_url, axis=1)
    head = df.iloc[start_index:end_index]
    # fetch yelp responses and parse
    head["responses"] = head.apply(get_yelp_biz_repsonses, axis=1)
    head["response_dct"] = head["responses"].apply(lambda x: json.loads(x.text))
    # TODO: we can drop intermediate columns later
    # n_biz_found doesn't really matter because it seems somewhat business agnostic?
    head["n_biz_found"] = head["response_dct"].apply(lambda x: x.get("total", 0))
    head["businesses"] = head["response_dct"].apply(lambda x: x.get("businesses", []))
    # NOTE: we're taking the first element for now. Later we can figure out how to deal with multi businesses
    # ideas: make the distance smaller
    # add additional information like phone number?
    head.businesses.str.len() - head.n_biz_found
    head["first_business"] = head.businesses.str[0]
    biz1 = pd.json_normalize(head["first_business"])
    biz1.index = head.index
    merged_df = head.join(biz1, lsuffix="og", rsuffix="api", how="left")
    # %%
    # check if first entry found was a match.
    merged_df["zip_match"] = merged_df.zipcode.astype(str).eq(
        merged_df["location.zip_code"].astype(str)
    )
    merged_df["lat_match"] = merged_df.lat.round(0).eq(
        merged_df["coordinates.latitude"].round(0)
    )
    merged_df["lng_match"] = merged_df.lng.round(0).eq(
        merged_df["coordinates.longitude"].round(0)
    )
    merged_df["name_match"] = merged_df.lng.round(0).eq(
        merged_df["coordinates.longitude"].round(0)
    )
    merged_df["name_lower"] = merged_df["name"].str.lower()
    merged_df["school_name_lower"] = merged_df["school_name"].str.lower()
    merged_df["name_ratio"] = merged_df.apply(
        lambda row: fuzz.ratio(row.name_lower, row["school_name_lower"]), axis=1
    )

    merged_df["yelp_entry_found"] = get_matches(merged_df)
    merged_df["yelp_entry_found"].value_counts(dropna=False, normalize=True)
    # %%
    # because of API rate limit, intelligently save the current batch.
    #

    os.makedirs(data_utils.YELP_BIZ_RESPONSES_RAW_DIR, exist_ok=True)
    merged_df.to_csv(
        f"{data_utils.YELP_BIZ_RESPONSES_RAW_DIR}/{start_index}_{end_index}.csv"
    )
    # update original dataframe
    df.loc[start_index:end_index, "hit_yelp_once"] = True
    df.to_csv(data_utils.LOCAL_PATH)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_index", type=int)
    args = parser.parse_args()
    # main(args.start_index)

# next steps: run for as long as we have tokens (let's do 300 today and then 500 every day over the weekend)
# we'll iteratively save and then have code to read the batches. Maybe have a note for where things start and stop
# Then write a function to evaluate "exact matches"
# initial idea: use coordinate and then name fuzzy matching.
# then get list of non exact matches
# we care about percentage of non-exact matches
# followed by steps to catch the non exact matches
# we should also sanity check on how often the non-exact matches are googlable
# Future research could be to get all reviews and do work on the text
# most of the columns aren't helpful. But the website itself could be helpful?
