import logging
import sqlite3
import utils.utils as helper
from enums.sales_schema import SalesSchema
from pyspark.sql.functions import col, dense_rank, round, sum
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)


def calculate_total_sales_per_category(sales_df):
    """Calculate the total sales per product category from the provided sales DataFrame.

    Parameters:
    - sales_df (DataFrame): DataFrame containing sales data with columns including:
      - 'CATEGORY': Category of the product.
      - 'TOTAL_SALE': Total sales amount for each product sale.

    Returns:
    DataFrame:
        DataFrame with aggregated total sales per category.
        Columns:
        - 'CATEGORY': Category of the product.
        - 'TOTAL_SALES': Total sales amount for each category rounded to two decimal places.

    This function calculates the total sales for each product category by grouping the provided
    sales DataFrame ('sales_df') based on the 'CATEGORY' column and aggregating the 'TOTAL_SALE'
    column using the sum function. The resulting DataFrame is ordered by category."""

    total_sales_per_category = sales_df.groupBy(SalesSchema.CATEGORY.value) \
        .agg(round(sum(SalesSchema.TOTAL_SALE.value), 2).alias(f'{SalesSchema.TOTAL_SALE.value}s')) \
        .orderBy(SalesSchema.CATEGORY.value)
    return total_sales_per_category


def calculate_top_n_products_per_location(sales_df, n=3):
    """Calculate the top N best-selling products per StoreLocation from the provided sales DataFrame.

    Parameters:
    - sales_df (DataFrame): DataFrame containing sales data with columns including:
      - 'STORE_LOCATION': Location of the store where the product was sold.
      - 'TOTAL_SALE': Total sales amount for each product sale.
      - 'PRICE': Unit price of the product.

    - n (int, optional): Number of top-selling products to calculate per StoreLocation (default is 3).

    Returns:
    DataFrame:
        DataFrame containing the top N best-selling products per StoreLocation.
        Columns:
        - 'STORE_LOCATION': Location of the store.
        - Other original columns from sales_df (e.g., 'PRODUCT_ID', 'PRODUCT_NAME', 'TOTAL_SALE', 'PRICE').

    This function calculates the top N best-selling products for each StoreLocation based on total sales
    ('TOTAL_SALE'). It ranks products within each StoreLocation by total sales in descending order using
    window functions. The resulting DataFrame includes only the top N products per StoreLocation and
    retains the original columns from the input sales DataFrame ('sales_df'). The 'PRICE' and 'TOTAL_SALE'
    columns are rounded to two decimal places for clarity."""

    # Define the window specification for ranking by TotalSale within each StoreLocation
    window_spec = Window.partitionBy(SalesSchema.STORE_LOCATION.value) \
        .orderBy(col(SalesSchema.TOTAL_SALE.value).desc())

    rank_str = 'rank'
    # Rank the products based on TotalSale within each StoreLocation
    ranked_products = sales_df.withColumn(rank_str, dense_rank().over(window_spec))

    # Filter to keep only the top N products per StoreLocation and drop the rank column
    top_n_products_per_location = ranked_products.filter(col(rank_str) <= n) \
        .drop(rank_str)

    # Round off the Price column to 2 decimal places
    top_n_products_per_location = top_n_products_per_location.withColumn(SalesSchema.PRICE.value,
                                                                         round(col(SalesSchema.PRICE.value), 2))

    # Round off the TotalSale column to 2 decimal places
    top_n_products_per_location = top_n_products_per_location.withColumn(SalesSchema.TOTAL_SALE.value,
                                                                         round(col(SalesSchema.TOTAL_SALE.value), 2))

    return top_n_products_per_location


def main():
    # Create a Spark session
    spark = helper.create_spark_session('EtlSalesPipeline')

    # Define CSV file path and schema
    file_path = 'data/sales_data.csv'
    sales_schema = SalesSchema.get_sales_schema()

    # Load sales data into DataFrame
    logging.info(f'Extracting data from {file_path}...')
    sales_df = helper.load_csv_data(spark, file_path, sales_schema)

    logging.info('Calculating TotalSale per product category...')
    sales_df = sales_df.withColumn(SalesSchema.TOTAL_SALE.value,
                                   round(col(SalesSchema.PRICE.value) * col(SalesSchema.QUANTITY_SOLD.value), 2))
    sales_df = sales_df.withColumn(SalesSchema.PRICE.value, round(col(SalesSchema.PRICE.value), 2))
    total_sales_per_category = calculate_total_sales_per_category(sales_df)

    logging.info('Calculating Top 3 best-selling products per StoreLocation...')
    top3_products_per_location = calculate_top_n_products_per_location(sales_df, n=3)

    # Establish SQLite connection and write results to database
    db_str = 'data/sales_data.db'
    conn = sqlite3.connect(db_str)
    logging.info(f'Writing data to {db_str}...')
    helper.write_to_sqlite(sales_df, 'Sales', conn)
    helper.write_to_sqlite(total_sales_per_category, 'TotalSalesPerCategory', conn)
    helper.write_to_sqlite(top3_products_per_location, 'TopProducts', conn)
    logging.info('Data successfully written to SQLite db.')

    conn.close()
    spark.stop()
    logging.info('ETL process completed successfully.')


if __name__ == '__main__':
    main()
