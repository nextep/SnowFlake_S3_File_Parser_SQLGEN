import snowflake.connector
import streamlit as st

import pandas as pd
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


# Step 2: Retrieve JSON structure of the selected file
query = f"SELECT $1 FROM @{stage_name}/{selected_entry} (file_format => JSON) LIMIT 1"
result = conn.cursor().execute(query).fetchone()

# Get the JSON structure as a string
json_string = result[0]

# Parse JSON structure into a dictionary
json_structure = json.loads(json_string)

# Function to generate field mappings
def generate_field_mappings(json_structure, parent_key=''):
    mappings = []
    for key, value in json_structure.items():
        field_name = f"{parent_key}.{key}" if parent_key else key

        if isinstance(value, dict):
            mappings.extend(generate_field_mappings(value, field_name))
        else:
            mappings.append((field_name, get_field_type(value)))

    return mappings

# Function to determine field type
def get_field_type(value):
    if isinstance(value, int):
        return "number"
    elif isinstance(value, bool):
        return "boolean"
    elif isinstance(value, str):
        # Check if the string value is a timestamp
        try:
            pd.Timestamp(value)
            return "timestamp"
        except ValueError:
            pass
    return "varchar"

# Generate field mappings
field_mappings = generate_field_mappings(json_structure)

# Display the field mappings
st.write("Field Mappings:")
for field_name, field_type in field_mappings:
    st.write(f"{field_name}: {field_type}")
