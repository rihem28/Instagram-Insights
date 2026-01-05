# transform.py
import pandas as pd
import os

# ----------------- Setup -----------------
os.chdir(r"C:\Users\malek\OneDrive\Desktop\BI Project")
print("Current working directory:", os.getcwd())

processed_dir = "data/processed"
os.makedirs(processed_dir, exist_ok=True)

# ----------------- Cleaning -----------------
def clean_data():
    df = pd.read_csv("data/staging/Instagram_Analytics.csv")
    print("Initial dataset shape:", df.shape)

    # ---- Missing Values ----
    print("\nMissing values before cleaning:")
    print(df.isnull().sum())

    # Categorical columns → mode
    categorical_cols = ["media_type", "traffic_source", "content_category"]
    for col in categorical_cols:
        mode_val = df[col].mode()[0]
        df[col] = df[col].fillna(mode_val)
        print(f"Filled missing categorical '{col}' with mode: {mode_val}")

    # Numerical columns → median
    numerical_cols = [
        "likes", "comments", "shares", "saves",
        "reach", "impressions", "followers_gained",
        "caption_length", "hashtags_count", "engagement_rate"
    ]
    for col in numerical_cols:
        median_val = df[col].median()
        df[col] = df[col].fillna(median_val)
        print(f"Filled missing numerical '{col}' with median: {median_val}")

    # ---- Data Type Fixing ----
    df["upload_date"] = pd.to_datetime(df["upload_date"], errors="coerce")
    before_drop = df.shape[0]
    df = df.dropna(subset=["upload_date"])
    after_drop = df.shape[0]
    print(f"Dropped {before_drop - after_drop} rows with invalid upload_date")

    # ---- Standardization ----
    df["media_type"] = df["media_type"].str.title().str.strip()
    df["traffic_source"] = df["traffic_source"].str.title().str.strip()
    df["content_category"] = df["content_category"].str.title().str.strip()
    print("Standardized categorical columns")

    # ---- Remove Duplicates ----
    before_dup = df.shape[0]
    df = df.drop_duplicates(subset=["post_id"])
    after_dup = df.shape[0]
    print(f"Removed {before_dup - after_dup} duplicate rows based on post_id")

    print("\nMissing values after cleaning:")
    print(df.isnull().sum())

    # Save cleaned dataset without KPIs
    df.to_csv(f"{processed_dir}/Instagram_Analytics_clean.csv", index=False)
    print("Cleaned data saved")

    return df

# ----------------- Transformation -----------------
def transform_data(df):
    print("\n--- Adding KPI columns ---")

    # ---- Total Engagement ----
    df["total_engagement"] = df["likes"] + df["comments"] + df["shares"] + df["saves"]
    print("Added 'total_engagement' column")

    # ---- Time Features ----
    df["upload_date"] = pd.to_datetime(df["upload_date"])
    df["year_month"] = df["upload_date"].dt.to_period("M")

    # ---- Engagement Growth Rate (MoM) ----
    monthly_engagement = df.groupby("year_month")["total_engagement"].sum().reset_index()
    monthly_engagement["engagement_growth_rate"] = monthly_engagement["total_engagement"].pct_change()
    df = df.merge(monthly_engagement[["year_month", "engagement_growth_rate"]],
                  on="year_month", how="left")
    print("Added 'engagement_growth_rate' column")

    # ---- High Engagement Flag ----
    avg_engagement = df["total_engagement"].mean()
    df["high_engagement_flag"] = (df["total_engagement"] > avg_engagement).astype(int)
    print("Added 'high_engagement_flag' column")

    # ---- Average Engagement by Media Type ----
    avg_engagement_media = (
        df.groupby("media_type")["total_engagement"]
        .mean()
        .reset_index()
        .rename(columns={"total_engagement": "avg_engagement_by_media"})
    )
    df = df.merge(avg_engagement_media, on="media_type", how="left")
    print("Added 'avg_engagement_by_media' column")

    # ---- Save cleaned dataset WITH KPI columns ----
    df.to_csv(f"{processed_dir}/Instagram_Analytics_clean_with_KPIs.csv", index=False)
    print("Cleaned dataset with KPIs saved")

    # ----------------- Dimensions -----------------

    # Time Dimension
    time_dim = df[["upload_date"]].drop_duplicates()
    time_dim["year"] = time_dim["upload_date"].dt.year
    time_dim["month"] = time_dim["upload_date"].dt.month
    time_dim["day"] = time_dim["upload_date"].dt.day
    time_dim.to_csv(f"{processed_dir}/time_dim.csv", index=False)

    # Content Dimension
    content_dim = df[["media_type", "content_category", "caption_length", "hashtags_count"]].drop_duplicates().reset_index(drop=True)
    content_dim["content_id"] = content_dim.index + 1
    content_dim.to_csv(f"{processed_dir}/content_dim.csv", index=False)

    # Media Dimension
    media_dim = df[["media_type"]].drop_duplicates().reset_index(drop=True)
    media_dim["media_id"] = media_dim.index + 1
    media_dim.to_csv(f"{processed_dir}/media_dim.csv", index=False)

    # Traffic Dimension
    traffic_dim = df[["traffic_source"]].drop_duplicates().reset_index(drop=True)
    traffic_dim["traffic_id"] = traffic_dim.index + 1
    traffic_dim.to_csv(f"{processed_dir}/traffic_dim.csv", index=False)

    # ----------------- Fact Table -----------------
    instagram_fact = df.merge(
        content_dim,
        on=["media_type", "content_category", "caption_length", "hashtags_count"],
        how="left"
    ).merge(
        media_dim[['media_type', 'media_id']],
        on='media_type',
        how='left'
    ).merge(
        traffic_dim[['traffic_source', 'traffic_id']],
        on='traffic_source',
        how='left'
    )

    # Optional: fill NaN IDs
    instagram_fact['traffic_id'] = instagram_fact['traffic_id'].fillna(0).astype(int)
    instagram_fact['media_id'] = instagram_fact['media_id'].fillna(0).astype(int)

    instagram_fact = instagram_fact[[
        "post_id", "upload_date", "content_id", "media_id", "traffic_id",
        "likes", "comments", "shares", "saves",
        "reach", "impressions", "followers_gained", "engagement_rate",
        "total_engagement", "engagement_growth_rate",
        "high_engagement_flag", "avg_engagement_by_media",
        "traffic_source"
    ]]
    instagram_fact.to_csv(f"{processed_dir}/instagram_fact.csv", index=False)
    print("Fact table with KPIs, media_id, and traffic_id saved")

    return df, instagram_fact

# ----------------- Validation -----------------
def validate_data(df, fact):
    print("\n--- Validating Data ---")
    allowed_nulls = ["engagement_growth_rate"]  # traffic_id and media_id are filled
    null_cols = fact.columns[fact.isnull().any()].tolist()
    unexpected = [c for c in null_cols if c not in allowed_nulls]
    assert len(unexpected) == 0, f"Unexpected NULLs: {unexpected}"
    print("Data validation completed successfully.")

# ----------------- Main -----------------
def main():
    df_clean = clean_data()
    df_kpi, fact = transform_data(df_clean)
    validate_data(df_kpi, fact)

if __name__ == "__main__":
    main()

