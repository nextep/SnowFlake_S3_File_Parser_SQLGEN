

import pandas
import requests

from urllib.error import URLError
import snowflake.connector
import streamlit as st

# Snowflake connection credentials (hardcoded)
snowflake_username = "admin"
snowflake_password = "EL#1iebr"
snowflake_account = "om05611.ca-central-1.aws"
snowflake_warehouse = "compute_wh"
snowflake_database = "EVENTS"
snowflake_schema = "EVENTS"

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
stage_entries = conn.cursor().execute(f"LIST @evt_2_sf").fetchall()

# Streamlit UI
st.title("Snowflake Stage File Analysis")
st.write("Select a file from the stage and choose a mapping")

# Display stage entries to the user
st.write("Stage Entries:")
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
