# Remove " ratings" text and convert to int
def clean_ratings_count(value):
    if pd.isna(value):
        return 0
    value = value.lower().replace("ratings", "").strip()
    multipliers = {"k": 1_000, "m": 1_000_000}
    for suffix, multiplier in multipliers.items():
        if value.endswith(suffix):
            return int(float(value[:-1]) * multiplier)
    try:
        return int(value.replace(",", ""))
    except:
        return 0

df_all["ratings_count"] = df_all["ratings_count"].apply(clean_ratings_count)

# Ratings to float
df_all["rating"] = pd.to_numeric(df_all["rating"], errors="coerce")

# Author name cleanup + first name
df_all["author"] = df_all["author"].fillna("").str.strip()
df_all["author_first"] = df_all["author"].apply(lambda x: x.split()[0].lower() if x else "")
df_all["source"] = "Goodreads"

#Remove duplicates
df_all.drop_duplicates(inplace=True)