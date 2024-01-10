

def yelp_reviews_fetch():
    = exploded_reviews.join(rev_raw)

    row['response_dct'].keys()
    ['reviews'][0]['text']

    # TODO: check if we can return more than 20 review chunks?

    # iterate through rows, and split up URL into 20 review chunks
    # save that to a cs.
    # we read that csv, figure out where we left off, and then continue.
    yelp_df["offset"] = 0
    num_chunks = 20
    # df['offset'] =
    pd.cut(df["review_count"], bins=num_chunks, labels=False)


    for i, row in yelp_df.iterrows():
        pass
        url = craft_url(row)
        response = requests.get(url, headers=HEADERS)
        print(response.json())
        break


    # %%
    data = {"value": range(100)}
    df2 = pd.DataFrame(data)

    # Specify the number of chunks
    num_chunks = 5
    df2["offset"] = pd.cut(df2["value"], bins=num_chunks, labels=False)
    df2["offset"] *= 20  # Adjust the offset value based on your specific requirements

    # Duplicate rows for each chunk and update the value column
    df2_expanded = df2.loc[df2.index.repeat(num_chunks)].reset_index(drop=True)
    df2_expanded["value"] += df2_expanded["offset"]

    # Display the result
    print(df2_expanded)