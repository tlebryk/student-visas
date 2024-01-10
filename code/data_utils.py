import os
import pandas as pd

DATA_PATH = "../data"
LOCAL_PATH = f"{DATA_PATH}/language-schools-df - Copy.csv"
YELP_BIZ_RESPONSES_RAW_DIR = f"{DATA_PATH}/yelp_biz_responses_raw"
YELP_BIZ_RESPONSES_AGG_DIR = f"{DATA_PATH}/yelp_biz_responses_agg"
YELP_REV_RESPONSES_RAW_DIR = f"{DATA_PATH}/yelp_rev_responses_raw"
YELP_REV_RESPONSES_AGG_DIR = f"{DATA_PATH}/yelp_rev_responses_agg"

REVIEWS_DIR = f"{DATA_PATH}/yelp_reviews"


def read_yelp_responses(yelp_dir: str = YELP_BIZ_RESPONSES_RAW_DIR) -> pd.DataFrame:
    """Reads all the the csvs from a directory into a single pandas DataFrame

    Args:
        yelp_dir (str, optional): Defaults to yelp_dir.

    Returns:
        pd.DataFrame
    """
    files = [f for f in os.listdir(yelp_dir) if f.endswith(".csv")]
    dfs = []
    for f in files:
        df = pd.read_csv(f"{yelp_dir}/{f}")
        df["filename"] = f
        dfs.append(df)
    df = pd.concat(dfs)
    df = df.drop(columns=df.filter(regex="Unnamed:").columns)
    return df


def aggregate_and_save(
    raw_dir=YELP_BIZ_RESPONSES_RAW_DIR,
    agg_dir=YELP_BIZ_RESPONSES_AGG_DIR,
    filename="biz_agg1.csv",
):
    df = read_yelp_responses(raw_dir)
    df.to_csv(f"{agg_dir}/{filename}")
