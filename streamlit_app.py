import snowflake.connector
import streamlit as st

import pandas
import requests
import json
from urllib.error import URLError


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
query = f"SELECT $1 FROM @{stage_name}/{selected_entry} (file_format => JSON) LIMIT 1"
result = conn.cursor().execute(query).fetchone()

# Get the JSON structure as a dictionary
json_structure = result[0]

# Generate field mappings based on the JSON structure
field_mappings = []

def generate_field_mappings(json_structure, parent_key=''):
    for key, value in json_structure.items():
        field_name = f"{parent_key}.{key}" if parent_key else key

        if isinstance(value, dict):
            generate_field_mappings(value, field_name)
        else:
            field_type = get_field_type(value)
            field_mapping = f"$1:{field_name}::{field_type}"
            field_mappings.append(field_mapping)

def get_field_type(value):
    if isinstance(value, int):
        return "number"
    elif isinstance(value, bool):
        return "boolean"
    else:
        return "varchar"

# Generate field mappings recursively
generate_field_mappings(json_structure)

# Step 4: Create a SELECT command with selected fields
select_command = f"SELECT {', '.join(field_mappings)} FROM @{stage_name}/{selected_entry} (file_format => JSON)"

# Display the generated SELECT command
st.write("Generated SELECT command:")
st.code(select_command)
