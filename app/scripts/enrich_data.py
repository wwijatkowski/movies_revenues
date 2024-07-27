from app.config.config import OMDB_API_KEY
from pyspark.sql import Row
from pyspark.sql import SparkSession
import requests
from tqdm import tqdm


def fetch_movie_data(title, year):
    url = f"http://www.omdbapi.com/?t={title}&y={year}&apikey={OMDB_API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    return None

def enrich_data(revenue_df):
    spark = SparkSession.builder.appName("MovieRevenueAnalysis").getOrCreate()
    enriched_data = []
    for row in tqdm(revenue_df.collect(), desc="Fetching movie data"):
        movie_data = fetch_movie_data(row['title'], row['date'])
        if movie_data and movie_data['Response'] == 'True':
            enriched_row = row.asDict()
            enriched_row.update({
                'genre': movie_data.get('Genre'),
                'director': movie_data.get('Director'),
                'actors': movie_data.get('Actors'),
                'imdb_rating': movie_data.get('imdbRating')
            })
            enriched_data.append(Row(**enriched_row))        
    return spark.createDataFrame(enriched_data)

if __name__ == "__main__":
    from ingest_data import ingest_data
    from app.config.config import DATA_PATH, LIMIT
    revenue_df = ingest_data(DATA_PATH, LIMIT)
    enriched_df = enrich_data(revenue_df)
    enriched_df.show()