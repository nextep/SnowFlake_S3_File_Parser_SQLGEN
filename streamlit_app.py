

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

# Step 3: Flatten the JSON and generate the SELECT command with field types
query = f"""
SELECT
    FLATTEN(input:$1:"$.alert")::VARCHAR AS alert,
    FLATTEN(input:$1:"$.app_proto")::VARCHAR AS app_proto,
    FLATTEN(input:$1:"$.dest_ip")::VARCHAR AS dest_ip,
    FLATTEN(input:$1:"$.dest_port")::NUMBER AS dest_port,
    FLATTEN(input:$1:"$.event_type")::VARCHAR AS event_type,
    FLATTEN(input:$1:"$.flow.bytes_toclient")::NUMBER AS bytes_toclient,
    FLATTEN(input:$1:"$.flow.bytes_toserver")::NUMBER AS bytes_toserver,
    FLATTEN(input:$1:"$.flow.pkts_toclient")::NUMBER AS pkts_toclient,
    FLATTEN(input:$1:"$.flow.pkts_toserver")::NUMBER AS pkts_toserver,
    FLATTEN(input:$1:"$.flow.start")::VARCHAR AS flow_start,
    FLATTEN(input:$1:"$.flow_id")::NUMBER AS flow_id,
    FLATTEN(input:$1:"$.http.hostname")::VARCHAR AS hostname,
    FLATTEN(input:$1:"$.http.http_content_type")::VARCHAR AS http_content_type,
    FLATTEN(input:$1:"$.http.http_method")::VARCHAR AS http_method,
    FLATTEN(input:$1:"$.http.http_user_agent")::VARCHAR AS http_user_agent,
    FLATTEN(input:$1:"$.http.length")::NUMBER AS http_length,
    FLATTEN(input:$1:"$.http.protocol")::VARCHAR AS http_protocol,
    FLATTEN(input:$1:"$.http.status")::NUMBER AS http_status,
    FLATTEN(input:$1:"$.http.url")::VARCHAR AS http_url,
    FLATTEN(input:$1:"$.payload")::VARCHAR AS payload,
    FLATTEN(input:$1:"$.pcap_cnt")::NUMBER AS pcap_cnt,
    FLATTEN(input:$1:"$.proto")::VARCHAR AS proto,
    FLATTEN(input:$1:"$.src_ip")::VARCHAR AS src_ip,
    FLATTEN(input:$1:"$.src_port")::NUMBER AS src_port,
    FLATTEN(input:$1:"$.stream")::NUMBER AS stream,
    FLATTEN(input:$1:"$.timestamp")::VARCHAR AS timestamp,
    FLATTEN(input:$1:"$.tx_id")::NUMBER AS tx_id
FROM
    @{stage_name}/{selected_entry}
(file_format => JSON)
"""

# Display the generated SELECT command and the extracted filename
st.write("Selected Filename:")
st.write(selected_entry)

st.write("Generated SELECT command:")
st.code(query)
