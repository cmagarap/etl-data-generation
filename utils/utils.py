import json
import random
from datetime import timedelta
from pyspark.sql import SparkSession


def create_spark_session(app_name):
    """Create and return a Spark session."""
    spark = SparkSession.builder \
        .master('local') \
        .appName(app_name) \
        .config('spark.executor.memory', '5g') \
        .config('spark.cores.max', '6') \
        .getOrCreate()
    return spark


def generate_random_datetime_values(start, end):
    # Initialize an empty list to store random datetime values
    random_date_times = []

    # Generate random datetime values for each day within the one-month range
    current_date = start
    while current_date <= end:
        # Determine the number of datetime values to generate for the current date (20 or more)
        # num_datetime_values = random.randint(1, 10)
        num_datetime_values = random.randint(31, 200)

        for _ in range(num_datetime_values):
            # Generate a random time within the day (0 to 23 hours, 0 to 59 minutes, 0 to 59 seconds)
            random_hours = random.randint(0, 23)
            random_minutes = random.randint(0, 59)
            random_seconds = random.randint(0, 59)

            # Create a datetime object with the current date and random time
            random_date_time = current_date.replace(hour=random_hours, minute=random_minutes, second=random_seconds)

            # Append the random datetime to the list
            random_date_times.append(random_date_time)

        # Move to the next day
        current_date += timedelta(days=1)

    return random_date_times


def load_csv_data(spark, file_path, schema):
    """Load CSV file into a Spark DataFrame with specified schema."""
    sales_df = spark.read.csv(file_path, header=True, schema=schema)
    return sales_df


def load_json_file(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data


def write_to_sqlite(dataframe, table_name, conn):
    """Write DataFrame to SQLite database."""
    dataframe_pd = dataframe.toPandas()
    dataframe_pd.to_sql(table_name, conn, if_exists="replace", index=False)
