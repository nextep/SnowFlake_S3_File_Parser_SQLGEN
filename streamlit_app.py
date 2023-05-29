
import streamlit as st
import pandas
import requests
import snowflake.connector
from urllib.error import URLError


# Retrieve Snowflake credentials from Streamlit secrets
snowflake_secrets = st.secrets["snowflake"]

# Connect to Snowflake
conn = snowflake.connector.connect(**snowflake_secrets)

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
