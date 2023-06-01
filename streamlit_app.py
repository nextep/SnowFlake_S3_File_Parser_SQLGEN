import snowflake.connector
import streamlit as st
import pandas as pd
import requests
import json
import re
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

# File formats
file_formats = ["JSON", "CSV", "UNKNOWN", "AVRO", "XML", "PARQUET", "CUSTOM"]

# File format selection
selected_file_format = st.selectbox("Select a file format", file_formats)

# Initialize 'result' to None
result = None

if selected_file_format == "CUSTOM":
    regex_pattern = st.text_input("Enter your regex pattern")

try:
    if selected_file_format != "CUSTOM":
        # Step 2: Retrieve structure of the selected file
        query = f"SELECT $1 FROM @{stage_name}/{selected_entry} (file_format => {selected_file_format}) LIMIT 1"
        result = conn.cursor().execute(query).fetchone()
    else:
        query = f"SELECT $1 FROM @{stage_name}/{selected_entry}"
        result = conn.cursor().execute(query).fetchone()
except snowflake.connector.errors.ProgrammingError as e:
    st.error("Not the parser, try another.")
    st.error(str(e))

# If 'result' is None, print error message and skip the rest of the script
if result is None:
    st.error("No result could be retrieved from the database.")
else:
    # Display result
    st.write("Result:")
    st.write(result)

    # Get the structure as a string
    structure_string = result[0]

    if selected_file_format == "JSON":
        try:
            # Parse JSON structure into a dictionary
            structure_dict = json.loads(structure_string)
        except json.JSONDecodeError:
            st.error("The result could not be parsed as a JSON string.")
            structure_dict = None
    elif selected_file_format == "CUSTOM":
        # Use regex to find matches in the string
        regex_matches = re.finditer(regex_pattern, structure_string)
        if regex_matches:
            field_mapping_data = []
for i, match in enumerate(regex_matches, start=1):
    field_name = st.text_input(f"Enter field name for Token {i}", key=f"field_{i}", max_chars=20)
    regex_pattern = f"(?P<{field_name}>{re.escape(match.group())})" if field_name else None
    field_mapping_data.extend([{"Field Name": field_name, "Value": value, "Regex Pattern": regex_pattern} for value in match.groups()])
field_mapping_df = pd.DataFrame(field_mapping_data)
st.write("Field Mappings:")
if not field_mapping_df.empty:
    st.table(field_mapping_df)

# Create an input field for each row to enter field values
field_values = []
if not field_mapping_df.empty:
    for i, row in field_mapping_df.iterrows():
        field_value = st.text_input(f"Enter field value for Field Name '{row['Field Name']}'", key=f"field_value_{i}")
        field_values.append(field_value)

# Add the entered field values to the DataFrame
field_mapping_df["Field Value"] = field_values

# Get selected field names for query
selected_fields = field_mapping_df["Field Name"].tolist()
selected_fields = [field for field in selected_fields if field]

# Generate SQL statement
if st.button("Generate SQL") and selected_fields:
    select_statement = "SELECT " + ", ".join(selected_fields) + f" FROM @{stage_name}/{selected_entry} (file_format => {selected_file_format})"
    st.write("Generated Select Statement:")
    st.code(select_statement)

    if st.button("Generate Regex") and selected_regex_patterns:
        combined_regex = "|".join(selected_regex_patterns)
        st.write("Generated Regex:")
        st.code(combined_regex)
    else:
        st.error("No matches found in the result using the provided regex pattern.")
else:
    st.error("Invalid file format selected.")

