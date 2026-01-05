import pandas as pd
import os

# ----------------- Get Project Root and Staging Directory -----------------
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
staging_dir = os.path.join(project_root, "data", "staging")
os.makedirs(staging_dir, exist_ok=True)

# ----------------- Extracting Data -----------------
# Load Instagram data from staging folder
instagram_csv_path = os.path.join(staging_dir, "Instagram_Analytics.csv")
instagram_data = pd.read_csv(instagram_csv_path)

# ----------------- Save Raw Data to Staging Area -----------------
instagram_data.to_csv(os.path.join(staging_dir, "instagram_raw.csv"), index=False)

print("Instagram data extraction completed and saved to staging area.")


