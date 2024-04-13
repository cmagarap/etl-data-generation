import logging
import pandas as pd
import numpy as np
import random
import utils.utils as helper
from datetime import datetime
from enums.sales_schema import SalesSchema

logging.basicConfig(level=logging.INFO)


def generate_random_sales_data(start_date, end_date, products_data, stores_data):
    """Generate synthetic sales data for a specified date range.

    Parameters:
    - start_date (datetime): Start date for the sales data generation.
    - end_date (datetime): End date for the sales data generation.
    - products_data (dict): Dictionary mapping product categories to lists of products.
    - stores_data (list): List of store locations.

    Returns:
    - DataFrame: DataFrame containing synthetic sales data with columns:
      - 'Date': Date of sale.
      - 'Product_ID': Unique identifier of the product.
      - 'Product_Name': Name of the product.
      - 'Category': Category of the product.
      - 'Price': Unit price of the product.
      - 'Quantity_Sold': Quantity of the product sold.
      - 'Store_Location': Location of the store where the product was sold.

    The function generates sales data within the specified date range. Each sale record
    includes a randomly chosen product from the provided product categories with a price
    adjustment (up to ±20%) and quantity variation based on the category. Seasonal effects
    are simulated, with higher sales during December (20% increase). Noise and outliers
    are introduced, with a 5% chance of a sale being an outlier (quantity sold increased
    by 2x to 5x). The generated data is returned as a pandas DataFrame."""

    data = SalesSchema.generate_sales_dict()
    date_range_with_time = helper.generate_random_datetime_values(start_date, end_date)

    for date in date_range_with_time:
        product_category = random.choice(list(products_data.keys()))
        product = random.choice(products_data[product_category])

        # Introduce variability in price (e.g., random price adjustment up to 20%)
        price_adjustment = np.random.uniform(low=-0.2, high=0.2)
        price = product['price'] * (1 + price_adjustment)

        # Introduce variability in quantity sold based on product category
        if product_category == 'Electronics':
            quantity_sold = int(np.random.normal(loc=100, scale=80))
        elif product_category == 'Sports':
            quantity_sold = int(np.random.normal(loc=100, scale=10))
        elif product_category == 'Clothing':
            quantity_sold = int(np.random.normal(loc=2000, scale=1000))
        elif product_category == 'Home & Kitchen':
            quantity_sold = int(np.random.normal(loc=200, scale=50))
        elif product_category == 'Books':
            quantity_sold = int(np.random.normal(loc=600, scale=300))
        elif product_category == 'Toys & Games':
            quantity_sold = int(np.random.normal(loc=300, scale=200))
        elif product_category == 'Beauty & Personal Care':
            quantity_sold = int(np.random.normal(loc=300, scale=200))

        # Simulate seasonality (e.g., higher sales during holidays)
        if date.month == 12:  # December (holiday season)
            quantity_sold *= 1.2  # 20% increase in quantity sold

        store_location = random.choice(stores_data)

        # Generate noise and outliers
        if random.random() < 0.05:  # 5% chance of an outlier
            quantity_sold *= np.random.uniform(2, 5)  # Increase quantity sold by 2x to 5x

        data[SalesSchema.DATE_COL.value].append(date)
        data[SalesSchema.PRODUCT_ID.value].append(product['product_id'])
        data[SalesSchema.PRODUCT_NAME.value].append(product['product_name'])
        data[SalesSchema.CATEGORY.value].append(product_category)
        data[SalesSchema.PRICE.value].append(round(price, 2))
        data[SalesSchema.QUANTITY_SOLD.value].append(max(0, int(quantity_sold)))
        data[SalesSchema.STORE_LOCATION.value].append(store_location)

    return pd.DataFrame(data)


def save_sales_data_to_csv(df, output_file):
    """Save the provided sales data DataFrame to a CSV file.

    Parameters:
    - df (DataFrame): DataFrame containing sales data to be saved.
    - output_file (str): Path to the output CSV file where the data will be saved.

    Returns:
    None

    This function saves the given DataFrame containing synthetic sales data to a CSV file
    specified by 'output_file'. The 'index' parameter is set to False to exclude DataFrame
    indices from the output file. Upon completion, a message indicating the generation of
    synthetic sales data is printed to the logs."""

    logging.info(f'Writing data to {output_file}...')
    df.to_csv(output_file, index=False)
    logging.info(f'Data successfully saved in {output_file}.')


def main():
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2023, 12, 31)
    products_file = 'data/products.json'
    stores_file = 'data/stores.json'
    output_file = 'data/sales_data_NEW.csv'

    # Load JSON files
    products_data = helper.load_json_file(products_file)
    stores_data = helper.load_json_file(stores_file)['stores']

    # Generate sales data
    logging.info(f'Generating sales data from {products_file} and {stores_file}...')
    sales_df = generate_random_sales_data(start_date, end_date, products_data, stores_data)
    logging.info('Data successfully generated.')

    # Save sales data to CSV
    save_sales_data_to_csv(sales_df, output_file)


if __name__ == '__main__':
    main()
