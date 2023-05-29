

import pandas
import requests

from urllib.error import URLError
import snowflake.connector
import streamlit as st

# Snowflake connection credentials (hardcoded)
snowflake_username = "admin"
snowflake_password = "EL#1iebr"
snowflake_account = "nszzdnc-xa32927.ca-central-1.aws"
snowflake_warehouse = "compute_wh"

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=snowflake_username,
    password=snowflake_password,
    account=snowflake_account,
    warehouse=snowflake_warehous
)

# Step 1: List entries in the Snowflake stage
stage_name = 'evt_2_sf'
stage_entries = conn.cursor().execute(f"LIST @ {stage_name}").fetchall()

# Display stage entries to the user
st.write("Stage Entries:")
for entry in stage_entries:
    st.write(entry[0])

# Step 2: Allow user to select an entry
selected_entry = st.selectbox("Select an entry", [entry[0] for entry in stage_entries])

# Step 3: Create a list of mappings from JSON values
query = f"SELECT $1 FROM @{stage_name}/{selected_entry} (file_format => JSON)"
result = conn.cursor().execute(query).fetchall()

# Extract JSON values and display to the user
mappings = [row[0] for row in result]
selected_mapping = st.selectbox("Select a mapping", mappings)

# Step 4: Create a SELECT command with selected fields
select_command = f"SELECT {selected_mapping} FROM @{stage_name}/{selected_entry} (file_format => JSON)"

# Display the generated SELECT command
st.write("Generated SELECT command:")
st.code(select_command)

# Prompt user to enter Snowflake connection credentials
snowflake_username = st.text_input("Snowflake Username")
snowflake_password = st.text_input("Snowflake Password", type="password")
snowflake_account = st.text_input("Snowflake Account URL")
snowflake_warehouse = st.text_input("Snowflake Warehouse")
snowflake_database = st.text_input("Snowflake Database")
snowflake_schema = st.text_input("Snowflake Schema")

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=snowflake_username,
    password=snowflake_password,
    account=snowflake_account,
    warehouse=snowflake_warehouse,
    database=snowflake_database,
    schema=snowflake_schema
)

# Step 1: List entries in the Snowflake stage
stage_name = 'evt_2_sf'
stage_entries = conn.cursor().execute(f"LIST @ {stage_name}").fetchall()

# Display stage entries to the user
st.write("Stage Entries:")
for entry in stage_entries:
    st.write(entry[0])

# Step 2: Allow user to select an entry
selected_entry = st.selectbox("Select an entry", [entry[0] for entry in stage_entries])

# Step 3: Create a list of mappings from JSON values
query = f"SELECT $1 FROM @{stage_name}/{selected_entry} (file_format => JSON)"
result = conn.cursor().execute(query).fetchall()

# Extract JSON values and display to the user
mappings = [row[0] for row in result]
selected_mapping = st.selectbox("Select a mapping", mappings)

# Step 4: Create a SELECT command with selected fields
select_command = f"SELECT {selected_mapping} FROM @{stage_name}/{selected_entry} (file_format => JSON)"

# Display the generated SELECT command
st.write("Generated SELECT command:")
st.code(select_command)
