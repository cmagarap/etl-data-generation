# Python ETL Pipeline

This repository contains a simple Python ETL (Extract, Transform, Load) pipeline that demonstrates how to generate and extract data, perform transformation tasks, and load the processed data into the target SQLite database (`data/sales_data.db`).

## Prerequisites

Before running this ETL pipeline, ensure you have the following installed on your system:

- [Python 3](https://www.python.org/downloads/)
- Pip (Python package manager)
- Install [Java 11](https://www.oracle.com/ph/java/technologies/javase/jdk11-archive-downloads.html) depending on your Operating System. You may use [Homebrew](https://formulae.brew.sh/formula/openjdk@11) for Mac. (PySpark depends on Java.)


## Folder Structure
```
├── data -- folder for JSON, CSV, and SQLite data
│   ├── products.json
│   ├── sales_data.csv
│   ├── sales_data.db
│   └── stores.json
├── enums -- folder for constants
│   └── sales_schema.py -- contains the schema for Sales table
├── figures -- folder for graph/chart
│   └── line-fig.html
├── utils -- folder helper functions
│   └── utils.py
├── data_visualization.py
├── extract_transform_load.py
├── generate_data.py
├── README.md
└── requirements.txt
```

## Setup Instructions
1. Clone the repository to your local machine:
   ```bash
   git clone https://github.com/cmagarap/etl-data-generation.git
   ```

2. Navigate to the project directory:
    ```bash
   cd etl-data-generation
   ```

3. Install the required Python packages using pip:
    ```bash
   pip install -r requirements.txt
    ```
   
## Execution Instructions
1. From the root directory, generate the synthetic Sales dataset:
   ```bash
   python3 generate_data.py
   ```
   
2. Run the ETL script:
   ```bash
   python3 extract_transform_load.py
   ```
   
3. Execute the script for producing graph visualization:
   ```bash
   python3 data_visualization.py
   ```
   An interactive graph will open in your default web browser and an HTML file is saved in the `figures` folder.

## Output
#### Visualization:
![Line Chart](https://raw.githubusercontent.com/cmagarap/etl-data-generation/main/figures/figure-screenshot.png)

#### Sales Table sample:
```bash
Date               |ProductID|ProductName                                              |Category              |Price             |QuantitySold|StoreLocation                                       |TotalSale         |
-------------------+---------+---------------------------------------------------------+----------------------+------------------+------------+----------------------------------------------------+------------------+
2022-01-01 06:17:57|    20028|Gap Cargo Shorts                                         |Clothing              |20.079999923706055|        1586|Best Buy, 2311 Los Robles Avenue, Pasadena, CA 91104|   31846.880859375|
2022-01-01 18:00:55|    60051|Super Smash Bros. Yoshi Amiibo                           |Toys & Games          |  10.4399995803833|         124|Best Buy, 2311 Los Robles Avenue, Pasadena, CA 91104|  1294.56005859375|
2022-01-01 03:05:32|    50003|Callaway Men's Golf Clubs (Complete Set)                 |Sports                | 713.7000122070312|          99|Walmart, 5230 Newell Road, Palo Alto, CA 94301      |      70656.296875|
2022-01-01 18:40:02|    60009|Baby Alive Real As Can Be Baby Doll                      |Toys & Games          |116.86000061035156|           0|Marketplace, 789 Oak Avenue, Suburbia, TX 77002     |               0.0|
2022-01-01 10:24:17|    40025|The Road                                                 |Books                 |11.640000343322754|          25|Best Buy, 2311 Los Robles Avenue, Pasadena, CA 91104|             291.0|
2022-01-01 18:10:01|    60033|The Legend of Zelda: Breath of the Wild (Nintendo Switch)|Toys & Games          | 59.59000015258789|         218|Marketplace, 789 Oak Avenue, Suburbia, TX 77002     |  12990.6201171875|
2022-01-01 05:54:54|    10033|Google Nest Wifi Router                                  |Electronics           | 221.7100067138672|         217|Corner Store, 101 Pine Street, Smalltown, AZ 85001  |     48111.0703125|
2022-01-01 09:49:56|    40001|To Kill a Mockingbird                                    |Books                 |11.710000038146973|         648|Walmart, 5230 Newell Road, Palo Alto, CA 94301      |    7588.080078125|
2022-01-01 13:37:21|    50012|NordicTrack Commercial 1750 Treadmill                    |Sports                | 1121.969970703125|          96|Landmark, 456 Elm Street, Cityville, NY 10001       |    107709.1171875|
2022-01-01 07:57:37|    20003|Wrangler Denim Jacket                                    |Clothing              | 79.94999694824219|        2439|Walmart, 5230 Newell Road, Palo Alto, CA 94301      |     194998.046875|
2022-01-01 20:17:30|    20001|Levi's Skinny Jeans                                      |Clothing              |57.540000915527344|        1270|Best Buy, 2311 Los Robles Avenue, Pasadena, CA 91104|      73075.796875|
2022-01-01 03:14:07|    20021|Adidas Joggers                                           |Clothing              |41.189998626708984|        2330|Landmark, 456 Elm Street, Cityville, NY 10001       |      95972.703125|
2022-01-01 21:23:45|    40000|The Great Gatsby                                         |Books                 |10.800000190734863|         635|Landmark, 456 Elm Street, Cityville, NY 10001       |            6858.0|
2022-01-01 01:02:27|    30019|Oneida Flatware Set                                      |Home & Kitchen        |49.630001068115234|         160|Best Buy, 2311 Los Robles Avenue, Pasadena, CA 91104|   7940.7998046875|
2022-01-01 16:41:20|    10003|Google Pixel 6                                           |Electronics           | 570.1199951171875|         104|City Mart, 231 West 78th Street, New York, NY 10024 |    59292.48046875|
2022-01-01 00:10:57|    70007|Bioderma Sensibio H2O Micellar Water                     |Beauty & Personal Care|12.850000381469727|          72|Corner Store, 101 Pine Street, Smalltown, AZ 85001  | 925.2000122070312|
2022-01-01 17:09:38|    20004|Dockers Chinos                                           |Clothing              |29.510000228881836|        1355|Shopwise, 42 Olive Street, Los Angeles, CA 90048    |    39986.05078125|
2022-01-01 23:37:54|    60016|Funko POP! Marvel: Avengers Endgame - Captain America    |Toys & Games          | 9.210000038146973|         110|Corner Store, 101 Pine Street, Smalltown, AZ 85001  |1013.0999755859375|
```

#### TopProducts Table:
```bash
Date               |ProductID|ProductName                         |Category   |Price             |QuantitySold|StoreLocation                                       |TotalSale  |
-------------------+---------+------------------------------------+-----------+------------------+------------+----------------------------------------------------+-----------+
2022-08-12 00:31:22|    20019|Calvin Klein Peacoat                |Clothing   |             84.25|       19338|Best Buy, 2311 Los Robles Avenue, Pasadena, CA 91104|  1629226.5|
2022-03-08 11:24:43|    10006|Apple MacBook Pro 16-inch           |Electronics|   2548.9599609375|         525|Best Buy, 2311 Los Robles Avenue, Pasadena, CA 91104|  1338204.0|
2022-09-22 09:18:25|    20013|AllSaints Leather Jacket            |Clothing   | 83.94000244140625|       14095|Best Buy, 2311 Los Robles Avenue, Pasadena, CA 91104|1183134.375|
2023-12-03 13:53:28|    10047|Sony A8H 65-inch OLED 4K TV         |Electronics| 2573.219970703125|         672|City Mart, 231 West 78th Street, New York, NY 10024 |1729203.875|
2022-12-23 07:48:04|    10010|Lenovo ThinkPad X1 Carbon           |Electronics|1460.3900146484375|        1045|City Mart, 231 West 78th Street, New York, NY 10024 |1526107.625|
2022-10-04 19:29:59|    20009|Burberry Trench Coat                |Clothing   | 95.52999877929688|       14553|City Mart, 231 West 78th Street, New York, NY 10024 |1390248.125|
2023-03-22 00:49:28|    10047|Sony A8H 65-inch OLED 4K TV         |Electronics| 2412.280029296875|         937|Corner Store, 101 Pine Street, Smalltown, AZ 85001  |  2260306.5|
2022-03-28 06:01:09|    20013|AllSaints Leather Jacket            |Clothing   | 89.45999908447266|       14661|Corner Store, 101 Pine Street, Smalltown, AZ 85001  |  1311573.0|
2023-10-16 18:45:04|    10023|Fujifilm X-T4 Mirrorless Camera     |Electronics| 2049.610107421875|         620|Corner Store, 101 Pine Street, Smalltown, AZ 85001  | 1270758.25|
2023-07-01 08:56:06|    10009|HP Spectre x360                     |Electronics| 1812.719970703125|        1067|DM Store, 1725 Slough Avenue, Scranton, PA 18503    | 1934172.25|
2022-08-28 09:44:03|    20006|Crewneck Sweater Gap                |Clothing   | 71.05999755859375|       16709|DM Store, 1725 Slough Avenue, Scranton, PA 18503    |  1187341.5|
2023-11-13 14:54:25|    20009|Burberry Trench Coat                |Clothing   |101.01000213623047|       11083|DM Store, 1725 Slough Avenue, Scranton, PA 18503    |1119493.875|
2023-06-15 08:45:34|    10038|Microsoft Surface Laptop 4          |Electronics| 1429.050048828125|        1191|Landmark, 456 Elm Street, Cityville, NY 10001       |1701998.625|
2023-06-15 19:41:34|    10022|Sony Alpha a7 III                   |Electronics|   1780.2900390625|         626|Landmark, 456 Elm Street, Cityville, NY 10001       |1114461.625|
2023-07-08 05:31:18|    10008|Dell XPS 13                         |Electronics|   1184.7900390625|         925|Landmark, 456 Elm Street, Cityville, NY 10001       | 1095930.75|
2022-02-21 11:59:22|    10039|Asus ROG Strix Scar 15 Gaming Laptop|Electronics| 2069.969970703125|         828|Marketplace, 789 Oak Avenue, Suburbia, TX 77002     |1713935.125|
2023-02-24 19:42:20|    10047|Sony A8H 65-inch OLED 4K TV         |Electronics|  2903.93994140625|         584|Marketplace, 789 Oak Avenue, Suburbia, TX 77002     |1695900.875|
2023-10-06 16:15:45|    10006|Apple MacBook Pro 16-inch           |Electronics|    2792.919921875|         468|Marketplace, 789 Oak Avenue, Suburbia, TX 77002     |  1307086.5|
2022-12-15 03:13:46|    20013|AllSaints Leather Jacket            |Clothing   |102.05999755859375|       18216|Shopwise, 42 Olive Street, Los Angeles, CA 90048    |1859124.875|
2022-03-05 21:15:06|    20009|Burberry Trench Coat                |Clothing   | 96.58000183105469|       17052|Shopwise, 42 Olive Street, Los Angeles, CA 90048    | 1646882.25|
2023-02-18 20:19:38|    10006|Apple MacBook Pro 16-inch           |Electronics| 2140.909912109375|         694|Shopwise, 42 Olive Street, Los Angeles, CA 90048    |  1485791.5|
2023-03-12 19:34:53|    20013|AllSaints Leather Jacket            |Clothing   |111.36000061035156|       12504|Walmart, 5230 Newell Road, Palo Alto, CA 94301      |  1392445.5|
2023-12-22 20:37:26|    20011|Levi's High-Waisted Jeans           |Clothing   |63.209999084472656|       20410|Walmart, 5230 Newell Road, Palo Alto, CA 94301      |1290116.125|
2022-06-14 15:57:14|    10039|Asus ROG Strix Scar 15 Gaming Laptop|Electronics|   2103.9599609375|         540|Walmart, 5230 Newell Road, Palo Alto, CA 94301      |1136138.375|
```

#### TotalSalesPerCategory Table:
```bash
Category              |TotalSales   |
----------------------+-------------+
Beauty & Personal Care| 234717118.52|
Books                 | 103150148.12|
Clothing              |1244021016.86|
Electronics           | 923703952.43|
Home & Kitchen        |  343450182.9|
Sports                | 325501928.62|
Toys & Games          | 310754310.65|
```