import snowflake.connector
import streamlit as st

# Retrieve Snowflake credentials from Streamlit secrets
snowflake_username = st.secrets["snowflake_username"]
snowflake_password = st.secrets["snowflake_password"]
snowflake_account = st.secrets["snowflake_account"]
snowflake_warehouse = st.secrets["snowflake_warehouse"]
snowflake_database = st.secrets["snowflake_database"]
snowflake_schema = st.secrets["snowflake_schema"]

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
