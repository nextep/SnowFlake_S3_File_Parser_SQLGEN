

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

# Extract the filename from the selected entry
selected_entry = selected_entry.split("/")[-1].strip()

# Step 3: Analyze JSON structure and generate field mappings
query = f"SELECT PARSE_JSON($1) FROM @{stage_name}/{selected_entry} (file_format => JSON)"
result = conn.cursor().execute(query).fetchall()

# Analyze the first row to determine the JSON structure
json_structure = result[0][0]

# Generate field mappings based on the JSON structure
field_mappings = []
for node in json_structure.flatten():
    field_name = node.get_path()
    field_type = node.get_type().name
    field_mapping = f"$1:{field_name}::{field_type}"
    field_mappings.append(field_mapping)

# Step 4: Create a SELECT command with selected fields
select_command = f"SELECT {', '.join(field_mappings)} FROM @{stage_name}/{selected_entry} (file_format => JSON)"

# Display the generated SELECT command
st.write("Generated SELECT command:")
st.code(select_command)
