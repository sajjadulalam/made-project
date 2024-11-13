import pandas as pd
import requests
import sqlite3
import os

# Dataset URLs
nla_url = "https://www.epa.gov/system/files/other-files/2024-08/nla22_condition_combined_2024-08-13_0.csv"
ecos_url = "https://ecos.fws.gov/ServCat/DownloadFile/173741?Reference=117348"

# Directory to save data
data_dir = './data'
os.makedirs(data_dir, exist_ok=True)

# File paths
nla_path = os.path.join(data_dir, "nla_condition.csv")
ecos_path = os.path.join(data_dir, "ecos_data.csv")
database_path = os.path.join(data_dir, "water_quality_analysis.db")

# Function to download datasets
def download_file(url, path):
    response = requests.get(url)
    with open(path, "wb") as file:
        file.write(response.content)
    print(f"Downloaded {url} to {path}")

# Download the datasets
download_file(nla_url, nla_path)
download_file(ecos_url, ecos_path)

def clean_data():
    # Load data
    nla_data = pd.read_csv('./data/nla_condition.csv')
    ecos_data = pd.read_csv('./data/ecos_data.csv')
    
    # Debugging: Print first 5 rows and column names
    print("Raw NLA Data (first 5 rows):")
    print(nla_data.head())
    print("Raw ECOS Data (first 5 rows):")
    print(ecos_data.head())
    
    print("NLA Data Columns:", nla_data.columns)
    print("ECOS Data Columns:", ecos_data.columns)

    # Clean column names (strip spaces if any)
    nla_data.columns = nla_data.columns.str.strip()
    ecos_data.columns = ecos_data.columns.str.strip()
    
    # Select relevant columns (assuming column names match)
    nla_columns = [
    'Study_Population', 'Type', 'Subpopulation', 'Indicator', 'Category',
    'nResp', 'Estimate.P', 'StdError.P', 'MarginofError.P', 'LCB95Pct.P',
    'UCB95Pct.P', 'Estimate.U', 'StdError.U', 'MarginofError.U',
    'LCB95Pct.U', 'UCB95Pct.U']

    ecos_columns = [
    'Site_Id', 'Unit_Id', 'Read_Date', 'Salinity (ppt)', 'Dissolved Oxygen (mg/L)',
    'pH (standard units)', 'Secchi Depth (m)', 'Water Depth (m)', 'Water Temp (?C)',
    'Air Temp-Celsius', 'Air Temp (?F)', 'Time (24:00)', 'Field_Tech', 'DateVerified',
    'WhoVerified', 'AirTemp (C)', 'Year']
    
    # Filter data with specified columns
    nla_data = nla_data[nla_columns]
    ecos_data = ecos_data[ecos_columns]
    
    # Return cleaned data
    return nla_data, ecos_data

# Save datasets to SQLite without deleting the database file
def save_to_sqlite(df1, df2):
    conn = sqlite3.connect(database_path)
    try:
        df1.to_sql('nla_condition', conn, if_exists='replace', index=False)
        df2.to_sql('ecos_data', conn, if_exists='replace', index=False)
    except sqlite3.OperationalError as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()
    print(f"Data saved to {database_path}")

# Main pipeline function
def run_pipeline():
    nla_data, ecos_data = clean_data()
    save_to_sqlite(nla_data, ecos_data)
    print("Pipeline executed successfully.")

if __name__ == "__main__":
    run_pipeline()
