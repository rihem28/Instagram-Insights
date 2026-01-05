import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Column mappings for CSV → MySQL
column_mappings = {
    "Time_Dim": {
        "upload_date": "UploadDate",
        "year": "Year",
        "month": "Month",
        "day": "Day"
    },
    "Content_Dim": {
        "content_id": "ContentID",
        "media_type": "MediaType",
        "content_category": "ContentCategory",
        "caption_length": "CaptionLength",
        "hashtags_count": "HashtagsCount"
    },
    "Media_Dim": {
        "media_id": "MediaID",
        "media_type": "MediaType"
    },
    "Traffic_Dim": {
        "traffic_id": "TrafficID",
        "traffic_source": "TrafficSource"
    },
    "Instagram_Fact": {
        "post_id": "PostID",
        "upload_date": "UploadDate",
        "content_id": "ContentID",
        "media_id": "MediaID",
        "traffic_id": "TrafficID",
        "likes": "Likes",
        "comments": "Comments",
        "shares": "Shares",
        "saves": "Saves",
        "reach": "Reach",
        "impressions": "Impressions",
        "followers_gained": "FollowersGained",
        "engagement_rate": "EngagementRate",
        "total_engagement": "TotalEngagement",
        "engagement_growth_rate": "EngagementGrowthRate",
        "high_engagement_flag": "HighEngagementFlag",
        "avg_engagement_by_media": "AvgEngagementByMedia",
        "traffic_source": "TrafficSource"
    }
}

# ----------------- MySQL Connection -----------------
def create_connection():
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        if connection.is_connected():
            print("Connected to MySQL database")
        return connection
    except Error as e:
        print(f"Error creating connection: {e}")
        return None

# ----------------- Create Tables -----------------
def create_tables(connection):
    sql = """
    CREATE TABLE IF NOT EXISTS Time_Dim (
        UploadDate DATE PRIMARY KEY,
        Year INT,
        Month INT,
        Day INT
    );

    CREATE TABLE IF NOT EXISTS Content_Dim (
        ContentID INT PRIMARY KEY,
        MediaType VARCHAR(50),
        ContentCategory VARCHAR(50),
        CaptionLength INT,
        HashtagsCount INT
    );

    CREATE TABLE IF NOT EXISTS Media_Dim (
        MediaID INT PRIMARY KEY,
        MediaType VARCHAR(50)
    );

    CREATE TABLE IF NOT EXISTS Traffic_Dim (
        TrafficID INT PRIMARY KEY,
        TrafficSource VARCHAR(100)
    );

    CREATE TABLE IF NOT EXISTS Instagram_Fact (
        PostID VARCHAR(50) PRIMARY KEY,
        UploadDate DATE,
        ContentID INT,
        MediaID INT,
        TrafficID INT,
        Likes INT,
        Comments INT,
        Shares INT,
        Saves INT,
        Reach INT,
        Impressions INT,
        FollowersGained INT,
        EngagementRate FLOAT,
        TotalEngagement INT,
        EngagementGrowthRate FLOAT,
        HighEngagementFlag TINYINT,
        AvgEngagementByMedia FLOAT,
        TrafficSource VARCHAR(100),
        FOREIGN KEY (ContentID) REFERENCES Content_Dim(ContentID),
        FOREIGN KEY (UploadDate) REFERENCES Time_Dim(UploadDate),
        FOREIGN KEY (MediaID) REFERENCES Media_Dim(MediaID),
        FOREIGN KEY (TrafficID) REFERENCES Traffic_Dim(TrafficID)
    );
    """
    cursor = connection.cursor()
    for stmt in sql.split(";"):
        if stmt.strip():
            cursor.execute(stmt)
    connection.commit()
    cursor.close()
    print("Tables created successfully")

# ----------------- Load CSV Data -----------------
def load_data(connection, table_name, csv_file):
    df = pd.read_csv(csv_file)
    if table_name in column_mappings:
        df.rename(columns=column_mappings[table_name], inplace=True)

    cursor = connection.cursor()
    cols = ", ".join(df.columns)
    vals = ", ".join(["%s"] * len(df.columns))
    query = f"INSERT IGNORE INTO {table_name} ({cols}) VALUES ({vals})"

    for row in df.itertuples(index=False):
        cursor.execute(query, tuple(row))

    connection.commit()
    cursor.close()
    print(f"Loaded {table_name}")

# ----------------- Main -----------------
def main():
    connection = create_connection()
    if connection:
        try:
            create_tables(connection)

            base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

            # Load CSV files with full path
            load_data(connection, "Time_Dim", os.path.join(base_dir, "data", "processed", "time_dim.csv"))
            load_data(connection, "Content_Dim", os.path.join(base_dir, "data", "processed", "content_dim.csv"))
            load_data(connection, "Media_Dim", os.path.join(base_dir, "data", "processed", "media_dim.csv"))
            load_data(connection, "Traffic_Dim", os.path.join(base_dir, "data", "processed", "traffic_dim.csv"))
            load_data(connection, "Instagram_Fact", os.path.join(base_dir, "data", "processed", "instagram_fact.csv"))

        finally:
            if connection.is_connected():
                connection.close()
                print("Database connection closed")

if __name__ == "__main__":
    main()
