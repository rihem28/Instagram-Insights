# 📊 Instagram Insights: From BI to Predictive Intelligence

## Project Overview

**Instagram Insights** is an end-to-end Business Intelligence and Predictive Analytics project that transforms raw Instagram performance data into actionable insights through a full data pipeline — from extraction and cleaning to dimensional modeling, interactive dashboarding, and machine learning-based engagement prediction.

The project simulates a real-world social media analytics workflow: ingesting raw post-level metrics, engineering a relational data warehouse, building executive-ready Power BI dashboards, and layering a predictive model on top to classify and forecast post performance.

---

## 🎯 Objectives

- Analyze content performance across media types
- Evaluate traffic sources and discoverability
- Identify factors driving follower growth
- Consolidate scattered Instagram post metrics into a clean, query-ready data warehouse
- Study and surface engagement, reach, and traffic-source trends through interactive dashboards
- Predict post engagement rate and segment content by performance tier using machine learning to support data-driven content strategy

---

## Dataset
Source: Kaggle – Instagram Analytics Dataset 
The dataset contains 30,000 Instagram posts collected over 12 months.
(Data used for educational purposes)

---

## 🛠️ Tech Stack

| Layer | Tools |
|---|---|
| **ETL** | Python (pandas), CSV staging |
| **Database** | MySQL (mysql-connector-python) |
| **Visualization** | Power BI Desktop |
| **Machine Learning** | Python (scikit-learn — regression + clustering) |
| **Environment** | dotenv for config management |

---

## 🏗️ Architecture & Pipeline

The project follows a classic **Extract → Transform → Load (ETL)** architecture feeding a **star schema** data warehouse:

```
Raw CSV (staging) → Clean & Transform → Star Schema (processed) → MySQL → Power BI + ML
```

### 1. Extract (`Extract.py`)
Loads the raw `Instagram_Analytics.csv` from the staging directory and persists a raw snapshot for traceability before any transformation.

### 2. Transform (`transform.py`)
- **Data cleaning**: imputes missing categorical values (mode) and numerical values (median), coerces and validates `upload_date`, standardizes text casing across categorical fields, and removes duplicate posts by `post_id`.
- **KPI engineering**: computes `total_engagement`, month-over-month `engagement_growth_rate`, a `high_engagement_flag` (posts above average engagement), and `avg_engagement_by_media` per media type.
- **Dimensional modeling**: decomposes the cleaned dataset into a **star schema** — `Time_Dim`, `Content_Dim`, `Media_Dim`, `Traffic_Dim`, and a central `Instagram_Fact` table — with surrogate keys and referential joins.
- **Validation**: asserts no unexpected NULLs remain in the fact table before export.

### 3. Load (`Load.py`)
Connects to MySQL, programmatically creates all dimension and fact tables (with primary/foreign key constraints), and bulk-loads the processed CSVs using `INSERT IGNORE` for idempotent reloading.

### 4. Data Warehouse Schema

```
Time_Dim ──┐
Content_Dim ─┼──< Instagram_Fact >── Media_Dim
Traffic_Dim ─┘
```

- **Instagram_Fact**: post_id, likes, comments, shares, saves, reach, impressions, followers_gained, engagement_rate, total_engagement, engagement_growth_rate, high_engagement_flag, avg_engagement_by_media, traffic_source
- **Content_Dim**: media type, content category, caption length, hashtag count
- **Time_Dim**: upload date, year, month, day
- **Media_Dim** / **Traffic_Dim**: lookup tables for media type and traffic source

### 5. Machine Learning Layer
A supervised regression model predicts `predicted_engagement_rate` per post, while a clustering model segments posts into three performance tiers — **High**, **Medium**, and **Low Engagement** — stored in a `final_ml_results` table linked back to the fact table via `post_id`.

---

## 📈 Dashboards (Power BI)

Four interconnected report pages:

1. **Overview** — Global KPIs (total engagement, reach, impressions), engagement rate by media type, monthly engagement trend, top-performing posts.
2. **Content & Traffic Analysis** — Total engagement by content category, engagement growth rate over time, traffic source contribution breakdown.
3. **Advanced Analytics & Drill-Through** — Interactive filtering by media type, content category, and traffic source, with a detailed post-level table for granular exploration.
4. **Predictive Insights** — Actual vs. predicted engagement rate scatter plot, post distribution by performance group, and a full prediction detail table with cluster assignments.

---

## 🔍 Key Insights

-Traffic Source Distribution:
The 6 traffic sources (Reels Feed, Home Feed, Profile, External, Hashtags, Explore) are almost perfectly balanced, each contributing roughly 16–17% of total engagement indicating no single aquisition channel dominates, growth isn't overly dependent on one acquisition source — a diversified but non-optimized funnel without deeper testing.

-Media Type Performance:
Average engagement rate varies noticeably across media types (Video, Reel, Carousel, Photo), with Reels and carousels showing the strongest average engagement relative to Photo posts — consistent with the platform-wide trend of algorithmic favoring of video content.

-Engagement Trend:
Monthly total engagement fluctuates in a relatively narrow band across the year, without one runaway seasonal spike — suggesting fairly stable, non-seasonal posting performance.

-Engagement Growth Rate:
Shows high month-to-month volatility (swinging between roughly 0 and 2), indicating engagement momentum is inconsistent rather than steadily compounding.

-Predictive Model Results:
The predictive model successfully separates posts into performance tiers, enabling **proactive** identification of likely low-performing content before publication.
The actual-vs-predicted scatter plot shows most posts tightly clustered along the diagonal (strong correlation), with a small number of outlier posts where predicted engagement diverges sharply from actual — useful to mention as a model limitation (a few high-variance posts pull residuals up). The prediction detail table also shows the model tends to predict slightly higher than actual for lower-performing posts — worth noting as a "conservative-risk" bias if asked about model evaluation.

---

## 💡 Business Recommendations

- Prioritize content formats and categories shown to drive higher average engagement (e.g., Reels, Carousel).
- Recommend A/B testing incremental budget/content shifts toward one channel at a time to identify which one has untapped growth potential and concentrate distribution efforts on the highest-converting traffic sources rather than treating all 6 as equally worth future investment, while testing improvements on underperforming channels.
- Integrate the engagement prediction model into the content workflow to flag draft posts likely to underperform, allowing pre-publication adjustments.

---

## 📁 Repository Structure

```
├── data/
│   ├── staging/          # Raw ingested data
│   └── processed/        # Cleaned data + star schema CSVs
├── etl/
│   ├── Extract.py
│   ├── transform.py
│   └── Load.py
├── models/                # ML training scripts / notebooks
├── dashboards/             # Power BI .pbix files
└── README.md
```

---

## 🚀 How to Run

1. Place raw data in `data/staging/Instagram_Analytics.csv`
2. Run the pipeline in order: `Extract.py` → `transform.py` → `Load.py`
3. Configure MySQL credentials via `.env` (`DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`)
4. Open the Power BI `.pbix` file and refresh the data connection to your MySQL instance

---

