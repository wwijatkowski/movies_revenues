import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

OMDB_API_KEY = os.getenv('OMDB_API_KEY')
LIMIT = 100 ## because of limit on api
DATA_PATH = "../movies_revenues/app/data/revenues_per_day.csv"
DATA_STORAGE_PREFIX = "../movies_revenues"

DIM_MOVE_LOCATION = f"{DATA_STORAGE_PREFIX}/output/dim_movie"
DIM_DATE_LOCATION = f"{DATA_STORAGE_PREFIX}/output/dim_date"
FACT_DAILY_REVENUE_LOCATION = f"{DATA_STORAGE_PREFIX}/output/fact_daily_revenue"
