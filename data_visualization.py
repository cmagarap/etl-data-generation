import pandas as pd
import sqlite3
import plotly.express as px
import os
import logging

logging.basicConfig(level=logging.INFO)


def fetch_sales_data(conn):
    """Fetch sales data from an SQLite database and return as a DataFrame.

    Parameters:
    - conn: SQLite database connection object.

    Returns:
    DataFrame:
        DataFrame containing fetched sales data.
        Columns:
        - 'YearMonth': Year and month in 'YYYY-MM' format.
        - 'MonthlyTotalSale': Total sales amount for each month.

    This function executes an SQL query to retrieve sales data from an SQLite database using the provided
    database connection ('conn'). The query calculates monthly total sales ('MonthlyTotalSale') grouped by
    year and month ('YearMonth'). The fetched data is returned as a pandas DataFrame with the 'YearMonth' column
    set as the index.

    Note:
    Ensure that the provided database connection ('conn') is established and points to the appropriate SQLite database.
    """

    query = '''
    SELECT strftime('%Y-%m', Date) AS YearMonth,
           SUM(TotalSale) AS MonthlyTotalSale
    FROM Sales
    GROUP BY strftime('%Y-%m', Date)
    ORDER BY strftime('%Y-%m', Date)
    '''
    sales_data = pd.read_sql(query, conn)
    sales_data.set_index('YearMonth', inplace=True)
    return sales_data


def generate_sales_line_chart(dataframe):
    """Generate a line chart visualizing total sales per month using Plotly.

    Parameters:
    - dataframe (DataFrame): DataFrame containing sales data.
        Expected columns:
        - Index: 'YearMonth' (Year and month in 'YYYY-MM' format).
        - Column: 'MonthlyTotalSale' (Total sales amount for each month).

    Returns:
    plotly.graph_objs._figure.Figure:
        Plotly Figure object representing the generated line chart.

    This function creates a line chart using Plotly to visualize the total sales per month based on the
    provided DataFrame ('dataframe'). The line chart displays the trend of total sales over time, with
    markers indicating individual data points. The title, x-axis label ('Month & Year'), and y-axis label
    ('Total Sales') are customized for clarity."""

    fig = px.line(dataframe, title='Total Sales Per Month: Year 2022 to 2023', markers=True)
    fig.update_layout(
        xaxis_title='Month & Year',
        yaxis_title='Total Sales'
    )
    return fig


def save_figure_to_html(fig, filename):
    """Save a Plotly figure to an HTML file.

    Parameters:
    - fig (plotly.graph_objs._figure.Figure): Plotly Figure object to be saved.
    - filename (str): Name of the HTML file (without extension) to save the figure.

    Returns:
    None

    This function saves the provided Plotly Figure object ('fig') as an HTML file in the 'figures' directory.
    If the 'figures' directory does not exist, it will be created automatically. The HTML file is named
    according to the provided 'filename' and saved with the '.html' extension."""

    if not os.path.exists('figures'):
        os.mkdir('figures')
    filepath = os.path.join('figures', filename)
    fig.write_html(filepath)


def main():
    # Connect to the SQLite database
    conn = sqlite3.connect('data/sales_data.db')

    # Fetch sales data
    sales_data = fetch_sales_data(conn)
    logging.info('Sales data fetched.')
    conn.close()

    # Generate line chart
    fig = generate_sales_line_chart(sales_data)
    logging.info('Data figure generated.')
    fig.show()

    # Save figure to HTML file
    save_figure_to_html(fig, 'line-fig.html')
    logging.info('Data figure saved as HTML.')


if __name__ == '__main__':
    main()
