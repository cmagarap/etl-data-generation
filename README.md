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
