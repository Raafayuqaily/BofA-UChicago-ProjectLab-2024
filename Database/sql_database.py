#%%
import pandas as pd
import sqlite3

# Create SQLite database connection (local file storage)
conn = sqlite3.connect('zack_database.db')

# Function: Save parquet file to a specified table
def import_parquet_to_table(parquet_file_path, table_name):
    df = pd.read_parquet(parquet_file_path)  # Load parquet file into DataFrame
    df.to_sql(table_name, conn, if_exists='replace', index=False)  # Write DataFrame to the specified table
    print(f"Data from {parquet_file_path} successfully imported into table '{table_name}'.")

# Example: Store multiple parquet files into different tables
parquet_files = {
    "t_zacks_fc.parquet": "t_zacks_fc",
    "t_zacks_fr.parquet": "t_zacks_fr",
    "t_zacks_mktv.parquet": "t_zacks_mktv",
    "t_zacks_shrs.parquet": "t_zacks_shrs"
}

# Import each CSV file into the corresponding table
for file_path, table_name in parquet_files.items():
    import_parquet_to_table(file_path, table_name)

#%% Extracting Data
# Example query: Extract data from different tables
# Extract data from 'zacks_data' table
query_zacks = """
SELECT ticker, comp_name, per_end_date, eps_diluted_net
FROM t_zacks_fc
WHERE ticker = 'AAPL'
ORDER BY per_end_date DESC;
"""
zacks_df = pd.read_sql_query(query_zacks, conn)

# Close database connection
conn.close()

# %%
