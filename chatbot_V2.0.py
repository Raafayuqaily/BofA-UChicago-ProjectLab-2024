# -*- coding: utf-8 -*-
"""
Created on Wed Dec  4 11:46:08 2024

@author: 14211
"""

import streamlit as st
import sqlite3
import pandas as pd
import os
import openai
import io
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer

# Set OpenAI API Key (replace with your actual key)
openai.api_key = "API here"

#%%
# Function to generate SQL query using OpenAI API
def generate_sql(user_question, error_message=None):
    error_prompt = f"\nThe previous SQL query resulted in the following error:\n{error_message}\nPlease correct the SQL query." if error_message else ""
    prompt = f"""
Pretend you are an expert at converting natural language questions into accurate SQL queries. Please generate an accurate SQL query based on
the following natural language question and database schema provided below. Think sequentially and refer to the sample natural language
questions with correct and incorrect outputs as well.

Database Schema:
Table 1: t_zacks_fc (This table contains fundamental indicators for companies)
Columns: 'ticker' = Unique zacks Identifier for each company/stock, ticker or trading symbol, 'comp_name' = Company name, 'exchange' = Exchange
traded, 'per_end_date' = Period end date which represents quarterly data, 'per_type' = Period type (eg. Q for quarterly data), 'filing_date' =
Filing date, 'filing_type' = Filing type: 10-K, 10-Q, PRELIM, 'zacks_sector_code' = Zacks sector code (Numeric Value eg. 11 = Aerospace),
'eps_diluted_net_basic' = Earnings per share (EPS) net (Company's net earnings or losses attributable to common shareholders per basic share
basis), 'lterm_debt_net_tot' = Net long-term debt (The net amount of long term debt issued and repaid. This field is either calculated as the
sum of the long term debt fields or used if a company does not report debt issued and repaid separately).
Keys: ticker, per_end_date, per_type

Table 2: t_zacks_fr (This table contains fundamental ratios for companies)
Columns: 'ticker' = Unique zacks Identifier for each company/stock, ticker or trading symbol, 'per_end_date' = Period end date which represents
quarterly data, 'per_type' = Period type (eg. Q for quarterly data), 'ret_invst' = Return on investments (An indicator of how profitable a
company is relative to its assets invested by shareholders and long-term bond holders. Calculated by dividing a company's operating earnings by
its long-term debt and shareholders equity), 'tot_debt_tot_equity' = Total debt / total equity (A measure of a company's financial leverage
calculated by dividing its long-term debt by stockholders' equity).
Keys: ticker, per_end_date, per_type.

Table 3: t_zacks_mktv (This table contains market value data for companies)
Columns: 'ticker' = Unique zacks Identifier for each company/stock, ticker or trading symbol, 'per_end_date' = Period end date which represents
quarterly data, 'per_type' = Period type (eg. Q for quarterly data), 'mkt_val' = Market Cap of Company (shares out x last monthly price per
share - unit is in Millions).
Keys: ticker, per_end_date, per_type.

Table 4: t_zacks_shrs (This table contains shares outstanding data for companies)
Columns: 'ticker' = Unique zacks Identifier for each company/stock, ticker or trading symbol, 'per_end_date' = Period end date which represents
quarterly data, 'per_type' = Period type (eg. Q for quarterly data), 'shares_out' = Number of Common Shares Outstanding from the front page of
10K/Q.
Keys: ticker, per_end_date, per_type.

Table 5: t_zacks_sectors (This table contains the zacks sector codes and their corresponding sectors)
Columns: 'zacks_sector_code' = Unique identifier for each zacks sector, 'sector' = The sector descriptions that correspond to the sector code 
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
JOIN t_zacks_fr fr ON fc.ticker = fr.ticker AND fc.per_end_date = fr.per_end_date AND fc.per_type = fr.per_type
WHERE fc.exchange = 'NASDAQ' AND fc.per_type = 'Q' AND fc.per_end_date BETWEEN '2024-04-01' AND '2024-06-30';
Incorrect output for prompt 6: SELECT T1.ticker, T1.eps_diluted_net_basic, T2.ret_invst, T2.tot_debt_tot_equity FROM t_zacks_fc AS T1 INNER
JOIN t_zacks_fr AS T2 ON T1.ticker = T2.ticker AND T1.per_end_date = T2.per_end_date WHERE T1.exchange = 'NASDAQ' AND T1.per_type = 'Q2';

Please make sure that when you are joining 2 or more tables, you are using all 3 keys (ticker, per_end_date & per_type). Also, ensure that the 
SQL query is syntactically correct and provides the expected output based on the natural language question provided.

User Question:
{user_question}
{error_prompt}

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
        result = pd.read_sql_query(sql, conn)
        return result
    except Exception as e:
        return str(e)
    finally:
        conn.close()

# Function to analyze extracted data using GPT
def analyze_data(user_question, data):
    table_md = data.to_markdown(index=False)
    prompt = f"""
I have executed an SQL query based on the following user question and obtained the data below.

User's Question:
{user_question}

Data Table:
{table_md}

Pretend you are an experienced equity analyst working in the banking industry. Please analyze this data in the style of an expert equity analyst, highlighting trends, comparing companies, analyzing significance of metrics, and noting any interesting insights regarding this data. 
Give at least 2-3 in-depth paragraphs with your analysis. Feel free to incorporate external information related to the data being analyzed. Do NOT use any latex / equations in your output and only give plain text. Again, ensure your output is formatted as just text in a few paragraphs WITH NO markdown formatting involved.
"""
    response = openai.ChatCompletion.create(
        model="gpt-4o-2024-11-20",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=1000,
        temperature=0.7
    )
    return response.choices[0].message["content"].strip()

# Function to save analysis as PDF with proper formatting
def save_analysis_as_pdf(user_question, analysis_text):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    styles = getSampleStyleSheet()
    elements = []

    # Title
    title_style = styles['Title']
    title = Paragraph("Equity Analysis Report", title_style)
    elements.append(title)
    elements.append(Spacer(1, 12))

    # User Question
    heading_style = styles['Heading2']
    question_heading = Paragraph("User's Question:", heading_style)
    elements.append(question_heading)
    elements.append(Spacer(1, 6))
    body_style = styles['BodyText']
    question_paragraph = Paragraph(user_question, body_style)
    elements.append(question_paragraph)
    elements.append(Spacer(1, 12))

    # Analysis
    analysis_heading = Paragraph("Analysis:", heading_style)
    elements.append(analysis_heading)
    elements.append(Spacer(1, 6))
    # Split analysis text into paragraphs
    for para in analysis_text.split('\n\n'):
        analysis_paragraph = Paragraph(para.strip(), body_style)
        elements.append(analysis_paragraph)
        elements.append(Spacer(1, 12))

    doc.build(elements)
    buffer.seek(0)
    return buffer

# Function to save the full conversation as PDF
def save_full_conversation_as_pdf(conversation):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    styles = getSampleStyleSheet()
    elements = []

    # Title
    title_style = styles['Title']
    title = Paragraph("Full Equity Analysis Report", title_style)
    elements.append(title)
    elements.append(Spacer(1, 12))

    # Iterate over the conversation history
    for idx, entry in enumerate(conversation):
        if entry['role'] == 'user':
            # User question
            question_heading = Paragraph(f"Question {idx // 2 + 1}:", styles['Heading2'])
            elements.append(question_heading)
            elements.append(Spacer(1, 6))
            question_paragraph = Paragraph(entry['content'], styles['BodyText'])
            elements.append(question_paragraph)
            elements.append(Spacer(1, 12))
        else:
            # Assistant analysis
            analysis_heading = Paragraph("Analysis:", styles['Heading2'])
            elements.append(analysis_heading)
            elements.append(Spacer(1, 6))
            # Split analysis text into paragraphs
            for para in entry['content'].split('\n\n'):
                analysis_paragraph = Paragraph(para.strip(), styles['BodyText'])
                elements.append(analysis_paragraph)
                elements.append(Spacer(1, 12))

    doc.build(elements)
    buffer.seek(0)
    return buffer

# Load tables into pandas DataFrames
def load_tables(files):
    tables = {}
    for table_name, file_path in files.items():
        if file_path.endswith(".parquet"):
            tables[table_name] = pd.read_parquet(file_path)
        elif file_path.endswith(".csv"):
            tables[table_name] = pd.read_csv(file_path)
    return tables

# Function to check if the question is finance-related
def is_finance_related(question):
    finance_keywords = ['company', 'market', 'shares', 'investment', 'debt', 'equity', 'finance', 'stock', 'ticker', 'sector', 'EPS', 'return on investment']
    return any(keyword.lower() in question.lower() for keyword in finance_keywords)

#%%
# Streamlit interface
st.title("Bank of America's Financial Data Analytics ChatBot")
st.write("""
You can ask me questions regarding company fundamentals, financial ratios, market values, 
shares outstanding, and sector classifications for all US equities dating back until 2006.
""")

# Display database schema
with st.expander("Database Schema", expanded=False):
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
    Columns: zacks_sector_code, sector
    """)

# Welcome prompt
if "continue" not in st.session_state:
    if st.text_input("Type 'yes' to continue:").strip().lower() == "yes":
        st.session_state["continue"] = True

# Initialize session state for conversation history
if "conversation" not in st.session_state:
    st.session_state["conversation"] = []

# Initialize session state for analyses
if "analyses" not in st.session_state:
    st.session_state["analyses"] = {}

# Chatbot interaction
if st.session_state.get("continue"):
    st.subheader("ChatBot Examples")
    st.write("""
    - Example 1: What company had the highest long-term debt in 2010?
    - Example 2: What is the average shares outstanding for all companies in the Computer & Technology Sector from 2017?
    - Example 3: Query the tickers and return on investment for companies with annual reports (per_type = 'A') and a return on investment greater than 10.
    """)

    with st.form(key='user_question_form'):
        user_question = st.text_input("Enter your question:")
        submit_button = st.form_submit_button(label='Submit')

    if submit_button:
        if is_finance_related(user_question):
            # Add user question to conversation history
            st.session_state["conversation"].append({"role": "user", "content": user_question})

            # Generate SQL query
            sql_query = generate_sql(user_question)
            st.subheader("Generated SQL Query")
            st.code(sql_query, language="sql")

            # Load tables
            tables_files = {
                "t_zacks_fc": "t_zacks_fc.parquet",
                "t_zacks_fr": "t_zacks_fr.parquet",
                "t_zacks_mktv": "t_zacks_mktv.parquet",
                "t_zacks_shrs": "t_zacks_shrs.parquet",
                "t_zacks_sectors": "t_zacks_sectors.csv"
            }
            tables = load_tables(tables_files)

            # Execute SQL query
            results = execute_sql(sql_query, tables)
            attempt = 1
            max_attempts = 3
            while isinstance(results, str) and attempt <= max_attempts:
                # If there's an error, re-generate the SQL query
                error_message = results
                st.error(f"Error executing query: {error_message}")
                sql_query = generate_sql(user_question, error_message=error_message)
                st.subheader(f"Re-generated SQL Query (Attempt {attempt})")
                st.code(sql_query, language="sql")
                results = execute_sql(sql_query, tables)
                attempt += 1

            if isinstance(results, pd.DataFrame) and not results.empty:
                st.subheader("Query Results")
                st.dataframe(results)

                # Store results and question in session state
                st.session_state['results'] = results
                st.session_state['user_question'] = user_question
                st.session_state['analysis_generated'] = False
            else:
                st.error(f"Error executing query after {max_attempts} attempts: {results}")
        else:
            st.write("I'm sorry, I can only assist with finance-related questions. Please ask a question related to financial data analysis.")

    # Display results and analysis
    if 'results' in st.session_state and 'user_question' in st.session_state:
        results = st.session_state['results']
        user_question = st.session_state['user_question']
        st.subheader("Query Results")
        st.dataframe(results)

        # Check if analysis already exists
        if user_question in st.session_state['analyses']:
            st.subheader("Equity Analysis")
            st.write(st.session_state['analyses'][user_question])

            # Download buttons
            pdf_buffer = save_analysis_as_pdf(user_question, st.session_state['analyses'][user_question])
            st.download_button(
                label="Download Analysis as PDF",
                data=pdf_buffer,
                file_name="equity_analysis.pdf",
                mime="application/pdf",
                key=f"download_analysis_{len(st.session_state['analyses'])}"
            )

            # Option to download the full conversation as PDF
            full_pdf_buffer = save_full_conversation_as_pdf(st.session_state["conversation"])
            st.download_button(
                label="Download Full Analysis as PDF",
                data=full_pdf_buffer,
                file_name="full_equity_analysis.pdf",
                mime="application/pdf",
                key="download_full_conversation"
            )
        else:
            # Ask if user wants an analysis
            analyze = st.radio("Would you like an analysis of this data?", ("No", "Yes"), key='analyze')
            if analyze == "Yes" and not st.session_state.get('analysis_generated', False):
                # Generate and display analysis
                st.subheader("Equity Analysis")
                analysis = analyze_data(user_question, results)
                st.write(analysis)

                # Store analysis in session state
                st.session_state['analyses'][user_question] = analysis
                st.session_state['analysis_generated'] = True

                # Add analysis to conversation history
                st.session_state["conversation"].append({"role": "assistant", "content": analysis})

                # Download buttons
                pdf_buffer = save_analysis_as_pdf(user_question, analysis)
                st.download_button(
                    label="Download Analysis as PDF",
                    data=pdf_buffer,
                    file_name="equity_analysis.pdf",
                    mime="application/pdf",
                    key=f"download_analysis_{len(st.session_state['analyses'])}"
                )

                # Option to download the full conversation as PDF
                full_pdf_buffer = save_full_conversation_as_pdf(st.session_state["conversation"])
                st.download_button(
                    label="Download Full Analysis as PDF",
                    data=full_pdf_buffer,
                    file_name="full_equity_analysis.pdf",
                    mime="application/pdf",
                    key="download_full_conversation"
                )

    # Handle follow-up questions
    if st.session_state["conversation"]:
        st.subheader("Conversation History")
        with st.expander("Show Conversation History", expanded=False):
            for idx, msg in enumerate(st.session_state["conversation"]):
                if msg["role"] == "user":
                    st.markdown(f"**User:** {msg['content']}")
                else:
                    st.markdown(f"**Assistant:** {msg['content']}")
