import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# The API key for accessing the OMDb API, loaded from the .env file.
OMDB_API_KEY = os.getenv('OMDB_API_KEY')

# The limit for API requests, useful for handling API rate limits.
LIMIT = 100

# The path to the input data file.
DATA_PATH = "../movies_revenues/app/data/revenues_per_day.csv"

# The prefix for data storage paths.
DATA_STORAGE_PREFIX = "../movies_revenues"

# The path to store the dimension movie table.
DIM_MOVE_LOCATION = f"{DATA_STORAGE_PREFIX}/output/dim_movie"

# The path to store the dimension date table.
DIM_DATE_LOCATION = f"{DATA_STORAGE_PREFIX}/output/dim_date"

# The path to store the fact daily revenue table.
FACT_DAILY_REVENUE_LOCATION = f"{DATA_STORAGE_PREFIX}/output/fact_daily_revenue"
