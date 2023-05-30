import snowflake.connector
import streamlit as st

import pandas as pd
import requests
import json
from urllib.error import URLError

# Streamlit secrets
streamlit_secrets = st.secrets["snowflake"]

# Snowflake connection credentials
snowflake_username = streamlit_secrets["user"]
snowflake_password = streamlit_secrets["password"]
snowflake_account = streamlit_secrets["account"]
snowflake_warehouse = streamlit_secrets["warehouse"]
snowflake_database = streamlit_secrets["database"]
snowflake_schema = streamlit_secrets["schema"]

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

# Display result
st.write("Result:")
st.write(result)

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
key_field = '$1'

# Streamlit UI for field mappings and text inputs
st.write("Field Mappings:")
columns = st.beta_columns(3)
selected_fields = []
for field_name, field_type in field_mappings:
    with columns[0]:
        st.write(f"{key_field}:{field_name}::{field_type}")
    with columns[1]:
        input_key = f"{field_name}"
        input_value = st.text_input(f"Enter field name for {field_name}", "")
    with columns[2]:
        if input_value:
            if "@" in field_name:
                selected_fields.append(f"{key_field}:\"{field_name}\"::{field_type} as {input_value}")
            else:
                selected_fields.append(f"{key_field}:{field_name}::{field_type} as {input_value}")

# Button to generate SQL statement
if st.button("Generate SQL") and selected_fields:
    select_statement = "SELECT " + ", ".join(selected_fields) + f" FROM @{stage_name}/{selected_entry} (file_format => JSON)"
    st.write("Generated Select Statement:")
    st.code(select_statement)
else:
    st.write("Please provide values for the corresponding fields.")
