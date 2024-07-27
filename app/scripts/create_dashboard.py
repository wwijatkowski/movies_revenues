from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from app.config.config import DIM_MOVE_LOCATION, FACT_DAILY_REVENUE_LOCATION, LIMIT

## 
TOP_REVENUE_TITLES_NUMBER = 30

def create_dashboard():
    spark = SparkSession.builder.appName("MovieRevenueAnalysis").getOrCreate()
    
    # Load data from parquet files
    dim_movie_df = spark.read.parquet(DIM_MOVE_LOCATION)
    fact_daily_revenue_df = spark.read.parquet(FACT_DAILY_REVENUE_LOCATION)
    
    # Join the fact and dimension tables
    merged_df = fact_daily_revenue_df.join(dim_movie_df, fact_daily_revenue_df.id == dim_movie_df.id)
    
    # Group by title and sum the revenue
    ranking_df = merged_df.groupBy("title").sum("revenue").orderBy("sum(revenue)", ascending=False).limit(TOP_REVENUE_TITLES_NUMBER)
    
    # Convert to Pandas DataFrame for visualization
    ranking_pd_df = ranking_df.toPandas()
    
    # Plot the ranking
    plt.figure(figsize=(12, 8))
    colors = cm.viridis([i / len(ranking_pd_df) for i in range(len(ranking_pd_df))])
    bars = plt.barh(ranking_pd_df["title"], ranking_pd_df["sum(revenue)"], color=colors)
    
    plt.xlabel("Total Daily Revenue (USD)")
    plt.ylabel("Movie Title")
    plt.title("Movie Revenue Ranking")
    plt.grid(axis='x', linestyle='--', alpha=0.7)
    plt.gca().invert_yaxis() 

    plt.subplots_adjust(left=0.30) # adjust left margin to accomodate longer titles
    
    # Add annotations
    for bar in bars:
        width = bar.get_width()
        plt.text(width + (100), bar.get_y() + bar.get_height() / 2, f'{width:.2f}', ha='left', va='center', color='black', fontsize=10, fontweight='bold')
    
    plt.show()

if __name__ == "__main__":
    create_dashboard()
