import streamlit as st
import sqlite3
import pandas as pd
import os
import openai

# Set OpenAI API Key (replace with your actual key)
openai.api_key = "ENTER"

# Function to generate SQL query using OpenAI API
def generate_sql(user_question):
    prompt = f"""
    Pretend you are an expert at converting natural language questions into accurate SQL queries. Please generate an accurate SQL query based on
    the following natural language question and database schema provided below. Think sequentially and refer to the sample natural language
    questions with correct and incorrect outputs as well.
    
    Database Schema:
    Table 1: t_zacks_fc (This table contains fundamental indicators for companies)
    Columns: 'ticker' = Unique zacks Identifier for each company/stock, ticker or trading symbol, 'comp_name' = Company name, 'exchange' = Exchange
    traded, 'per_end_date' = Period end date which represents quarterly data, 'per_type' = Period type (eg. Q for quarterly data), 'filing_date' =
    Filing date, 'filing_type' = Filing type: 10-K, 10-Q, PRELIM, 'zacks_sector_code' = Zacks sector code (Numeric Value eg. 11 = Aerospace),
    'eps_diluted_net_basic’ = Earnings per share (EPS) net (Company's net earnings or losses attributable to common shareholders per basic share
    basis), 'lterm_debt_net_tot' = Net long-term debt (The net amount of long term debt issued and repaid. This field is either calculated as the
    sum of the long term debt fields or used if a company does not report debt issued and repaid separately).
    Keys: ticker, per_end_date, per_type
    
    Table 2: t_zacks_fr (This table contains fundamental ratios for companies)
    Columns: 'ticker' = Unique zacks Identifier for each company/stock, ticker or trading symbol, 'per_end_date' = Period end date which represents
    quarterly data, 'per_type' = Period type (eg. Q for quarterly data), ‘ret_invst’ = Return on investments (An indicator of how profitable a
    company is relative to its assets invested by shareholders and long-term bond holders. Calculated by dividing a company's operating earnings by
    its long-term debt and shareholders equity), ‘tot_debt_tot_equity’ = Total debt / total equity (A measure of a company's financial leverage
    calculated by dividing its long-term debt by stockholders' equity).
    Keys: ticker, per_end_date, per_type.
    
    Table 3: t_zacks_mktv (This table contains market value data for companies)
    Columns: 'ticker' = Unique zacks Identifier for each company/stock, ticker or trading symbol, 'per_end_date' = Period end date which represents
    quarterly data, 'per_type' = Period type (eg. Q for quarterly data), ‘mkt_val’ = Market Cap of Company (shares out x last monthly price per
    share - unit is in Millions).
    Keys: ticker, per_end_date, per_type.
    
    Table 4: t_zacks_shrs (This table contains shares outstanding data for companies)
    Columns: 'ticker' = Unique zacks Identifier for each company/stock, ticker or trading symbol, 'per_end_date' = Period end date which represents
    quarterly data, 'per_type' = Period type (eg. Q for quarterly data), ‘shares_out’ = Number of Common Shares Outstanding from the front page of
    10K/Q.
    Keys: ticker, per_end_date, per_type.
    
    Table 5: t_zacks_sectors (This table contains the zacks sector codes and their corresponding sectors)
    Columns: 'zacks_sector_code' = Unique identifier for each zacks sector, 'sector': the sector descriptions that correspond to the sector code 
    Keys: zacks_sector_code 
    
    Sample natural language questions with correct and incorrect outputs: 
    Sample prompt 1: Output ticker with the largest market value recorded on any given period end date. 
    Correct output for prompt 1: SELECT ticker, per_end_date, MAX(mkt_val) AS max_market_value FROM t_zacks_mktv GROUP BY per_end_date ORDER BY
    max_market_value DESC LIMIT 1;
    Incorrect output for prompt 1: SELECT MAX(mkt_val) , ticker FROM t_zacks_mktv GROUP BY ticker
    
    Sample prompt 2: What is the company name with the lowest market cap?
    Correct output for prompt 2: SELECT fc.comp_name, mktv.ticker, mktv.mkt_val FROM t_zacks_mktv AS mktv JOIN t_zacks_fc AS fc ON mktv.ticker =
    fc.ticker WHERE mktv.mkt_val = (SELECT MIN(mkt_val) FROM t_zacks_mktv);
    Incorrect output for prompt 2:  SELECT T1.comp_name FROM t_zacks_fc AS T1 INNER JOIN t_zacks_mktv AS T2 ON T1.ticker = T2.ticker AND
    T1.per_end_date = T2.per_end_date AND T1.per_type = T2.per_type ORDER BY T2.mkt_val LIMIT 1
    
    Sample prompt 3: Filter t_zacks_fc to only show companies with a total debt-to-equity ratio greater than 1.
    Correct output for prompt 3: SELECT * FROM t_zacks_fr WHERE tot_debt_tot_equity > 1;
    Incorrect output for prompt 3: SELECT * FROM t_zacks_fr WHERE t_zacks_mktv > 1;
    
    Sample prompt 4: Filter t_zacks_shrs to include companies with more than 500 million shares outstanding as of the most recent quarter.
    Correct output for prompt 4: SELECT *
    FROM t_zacks_shrs
    WHERE shares_out > 5000
    ORDER BY per_end_date DESC;
    Incorrect output for prompt 4: SELECT * FROM t_zacks_shrs WHERE shares_out > 500000000
    
    Sample prompt 5: Combine t_zacks_mktv and t_zacks_shrs to show tickers with market cap and shares outstanding in the latest period end date.
    Correct output for prompt 5: SELECT mktv.ticker, mktv.per_end_date, mktv.mkt_val, shrs.shares_out
    FROM t_zacks_mktv mktv
    JOIN t_zacks_shrs shrs ON mktv.ticker = shrs.ticker AND mktv.per_end_date = shrs.per_end_date
    ORDER BY mktv.per_end_date DESC;
    Incorrect output for prompt 5: SELECT ticker, mkt_val, shares_out FROM t_zacks_mktv INNER JOIN t_zacks_shrs ON t_zacks_mktv.ticker =
    t_zacks_shrs.ticker AND t_zacks_mktv.per_end_date = t_zacks_shrs.per_end_date ORDER BY per_end_date DESC LIMIT 1
    
    Sample prompt 6: Join t_zacks_fc and t_zacks_fr to show tickers with total debt-to-equity ratios and EPS from NASDAQ as of Q2 2024.
    Correct output for prompt 6: SELECT fc.ticker, fc.eps_diluted_net_basic, fr.tot_debt_tot_equity
    FROM t_zacks_fc fc
    JOIN t_zacks_fr fr ON fc.ticker = fr.ticker AND fc.per_end_date = fr.per_end_date
    WHERE fc.exchange = 'NASDAQ' AND fc.per_type = 'Q' AND fc.per_end_date BETWEEN '2024-04-01' AND '2024-06-30';
    Incorrect output for prompt 6: SELECT T1.ticker, T1.eps_diluted_net_basic, T2.ret_invst, T2.tot_debt_tot_equity FROM t_zacks_fc AS T1 INNER
    JOIN t_zacks_fr AS T2 ON T1.ticker = T2.ticker AND T1.per_end_date = T2.per_end_date WHERE T1.exchange = 'NASDAQ' AND T1.per_type = 'Q2';
    
    Please make sure that when you are joining 2 or more tables, you are using all 3 keys (ticker, per_end_date & per_type) Also, ensure that the 
    SQL query is syntactically correct and provides the expected output based on the natural language question provided.

    User Question:
    {user_question}

    Please provide only the SQL query without any markdown, code block syntax, or explanations.
    """
    response = openai.ChatCompletion.create(
        model="ft:gpt-4o-2024-08-06:personal::AXYv83vn",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=300,
        temperature=0.0
    )
    return response.choices[0].message["content"].strip()

# Function to execute SQL query using SQLite
def execute_sql(sql, tables):
    try:
        conn = sqlite3.connect(":memory:")
        for table_name, df in tables.items():
            df.to_sql(table_name, conn, index=False, if_exists="replace")
        return pd.read_sql_query(sql, conn)
    except Exception as e:
        return str(e)
    finally:
        conn.close()

# Function to analyze extracted data using GPT
def analyze_data(user_question, data):
    table_md = data.to_markdown()
    prompt = f"""
    I have executed an SQL query based on the following user question and obtained the data below.

    User's Question:
    {user_question}

    Data Table:
    {table_md}

    Pretend you are an experienced equity analyst working in the banking industry. Please analyze this data in the style of an expert equity        analyst, highlighting trends, comparing companies, analyzing significance of metrics, and noting any interesting insights regarding this data. 
    Give at least 2-3 in-depth paragraphs with your analysis. Feel free to incorporate external information related to the data being analyzed. Do NOT use any latex / equations in your output and only give plain text. Again, ensure your output is formatted as just text in a few paragraphs WITH NO markdown formatting involved.
    """
    response = openai.ChatCompletion.create(
        model="gpt-4o-2024-11-20",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=1000,
        temperature=0.7
    )
    return response.choices[0].message["content"].strip()


# Define file paths for database tables
tables_files = {
    "t_zacks_fc": "t_zacks_fc.parquet",
    "t_zacks_fr": "t_zacks_fr.parquet",
    "t_zacks_mktv": "t_zacks_mktv.parquet",
    "t_zacks_shrs": "t_zacks_shrs.parquet",
    "t_zacks_sectors": "t_zacks_sectors.csv"
}

# Load tables into pandas DataFrames
def load_tables(files):
    tables = {}
    for table_name, file_path in files.items():
        if file_path.endswith(".parquet"):
            tables[table_name] = pd.read_parquet(file_path)
        elif file_path.endswith(".csv"):
            tables[table_name] = pd.read_csv(file_path)
    return tables

# Streamlit interface
st.title("Bank of America's Financial Data Analytics ChatBot")
st.write("""
You can ask me questions regarding company fundamentals, financial ratios, market values, 
shares outstanding, and sector classifications for all US equities dating back until 2006.
""")

# Display database schema
with st.expander("Database Schema", expanded=True):
    st.write("""
    **Table 1: t_zacks_fc**
    Columns: ticker, comp_name, exchange, per_end_date, per_type, filing_date, filing_type, zacks_sector_code, eps_diluted_net_basic, lterm_debt_net_tot
    
    **Table 2: t_zacks_fr**
    Columns: ticker, per_end_date, per_type, ret_invst, tot_debt_tot_equity
    
    **Table 3: t_zacks_mktv**
    Columns: ticker, per_end_date, per_type, mkt_val
    
    **Table 4: t_zacks_shrs**
    Columns: ticker, per_end_date, per_type, shares_out
    
    **Table 5: t_zacks_sectors**
    Columns: zacks_sector_codes, sector
    """)

# Welcome prompt
if "continue" not in st.session_state:
    if st.text_input("Type 'yes' to continue:").strip().lower() == "yes":
        st.session_state["continue"] = True

# Chatbot interaction
if st.session_state.get("continue"):
    st.subheader("ChatBot Examples")
    st.write("""
    - Example 1: What company had the highest long-term debt in 2010?
    - Example 2: What is the average shares outstanding for all companies in the Computer & Technology Sector from 2017?
    - Example 3: Query the tickers and return on investment for companies with annual reports (per_type = 'A') and a return on investment greater than 10.
    """)
    
    user_question = st.text_input("Enter your question:")
    if user_question:
        sql_query = generate_sql(user_question)
        st.subheader("Generated SQL Query")
        st.code(sql_query, language="sql")

        st.subheader("Query Results")
        tables = load_tables(tables_files)
        results = execute_sql(sql_query, tables)
        if isinstance(results, pd.DataFrame):
            st.dataframe(results)
            
            # Ask if user wants an analysis
            analyze = st.radio("Would you like an analysis of this data?", ("No", "Yes"))
            if analyze == "Yes":
                st.subheader("Equity Analysis")
                analysis = analyze_data(user_question, results)
                st.write(analysis)
        else:
            st.error(f"Error executing query: {results}")
