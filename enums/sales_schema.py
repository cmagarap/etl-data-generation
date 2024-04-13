from enum import Enum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType


class SalesSchema(Enum):
    DATE_COL = 'Date'
    PRODUCT_ID = 'ProductID'
    PRODUCT_NAME = 'ProductName'
    CATEGORY = 'Category'
    PRICE = 'Price'
    QUANTITY_SOLD = 'QuantitySold'
    STORE_LOCATION = 'StoreLocation'
    TOTAL_SALE = 'TotalSale'

    @staticmethod
    def generate_sales_dict():
        return {
            SalesSchema.DATE_COL.value: [],
            SalesSchema.PRODUCT_ID.value: [],
            SalesSchema.PRODUCT_NAME.value: [],
            SalesSchema.CATEGORY.value: [],
            SalesSchema.PRICE.value: [],
            SalesSchema.QUANTITY_SOLD.value: [],
            SalesSchema.STORE_LOCATION.value: []
        }

    @staticmethod
    def get_sales_schema():
        return StructType([
            StructField(SalesSchema.DATE_COL.value, TimestampType(), True),
            StructField(SalesSchema.PRODUCT_ID.value, IntegerType(), True),
            StructField(SalesSchema.PRODUCT_NAME.value, StringType(), True),
            StructField(SalesSchema.CATEGORY.value, StringType(), True),
            StructField(SalesSchema.PRICE.value, FloatType(), True),
            StructField(SalesSchema.QUANTITY_SOLD.value, IntegerType(), True),
            StructField(SalesSchema.STORE_LOCATION.value, StringType(), True)
        ])
