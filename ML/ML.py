# ===============================
# ML PIPELINE - Instagram Engagement
# ===============================

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor
from sklearn.cluster import KMeans
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error

# ===============================
# 1. LOAD DATA
# ===============================
df = pd.read_csv(r"C:\Users\malek\OneDrive\Desktop\BI Project\data\processed\Instagram_Analytics_clean_with_KPIs.csv")

# Ensure total_engagement exists
if "total_engagement" not in df.columns:
    df["total_engagement"] = df["likes"] + df["comments"] + df["shares"] + df["saves"]

# Ensure engagement_rate exists
if "engagement_rate" not in df.columns:
    if "followers_gained" in df.columns:
        df["engagement_rate"] = df["total_engagement"] / df["followers_gained"]
    elif "impressions" in df.columns:
        df["engagement_rate"] = df["total_engagement"] / df["impressions"]
    else:
        df["engagement_rate"] = df["total_engagement"]

# Drop rows with missing target
df = df.dropna(subset=["engagement_rate"])

# ===============================
# 2. FEATURE SELECTION FOR REGRESSION
# ===============================

numerical_features = [
    "caption_length",
    "hashtags_count",
    "reach",
    "impressions"
]

categorical_features = [
    "media_type",
    "content_category",
    "traffic_source"
]

X = df[numerical_features + categorical_features]
y = df["engagement_rate"]

# ===============================
# 3. REGRESSION PIPELINE
# ===============================

# Preprocessing
numeric_transformer = StandardScaler()
categorical_transformer = OneHotEncoder(drop="first", handle_unknown="ignore")

preprocessor = ColumnTransformer(
    transformers=[
        ("num", numeric_transformer, numerical_features),
        ("cat", categorical_transformer, categorical_features),
    ]
)

# Model
regressor = RandomForestRegressor(n_estimators=200, random_state=42)

model = Pipeline(steps=[
    ("preprocessor", preprocessor),
    ("regressor", regressor)
])

# Train-test split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# Train model
model.fit(X_train, y_train)

# Predictions
y_pred = model.predict(X_test)

# Evaluation
print("Regression Performance:")
print("R²:", r2_score(y_test, y_pred))
print("RMSE:", np.sqrt(mean_squared_error(y_test, y_pred)))
print("MAE:", mean_absolute_error(y_test, y_pred))

# Predict engagement for all posts
df["predicted_engagement_rate"] = model.predict(X)

# ===============================
# 4. CLUSTERING TASK
# ===============================

clustering_features = [
    "likes",
    "comments",
    "shares",
    "saves",
    "engagement_rate"
]

cluster_data = df[clustering_features].fillna(0)

# Scale data
scaler = StandardScaler()
cluster_data_scaled = scaler.fit_transform(cluster_data)

# K-Means clustering
kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
df["cluster"] = kmeans.fit_predict(cluster_data_scaled)

# ===============================
# 5. LABEL CLUSTERS
# ===============================

cluster_means = df.groupby("cluster")["engagement_rate"].mean().sort_values()

cluster_mapping = {
    cluster_means.index[0]: "Low Engagement",
    cluster_means.index[1]: "Medium Engagement",
    cluster_means.index[2]: "High Engagement",
}

df["performance_group"] = df["cluster"].map(cluster_mapping)

# ===============================
# 6. EXPORT RESULTS FOR POWER BI
# ===============================

df.to_csv(r"C:\Users\malek\OneDrive\Desktop\BI Project\data\processed\final_ml_results.csv", index=False)

print("ML pipeline completed successfully.")
print("File exported: final_ml_results.csv")
