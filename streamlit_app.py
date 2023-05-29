import streamlit as st
import snowflake.connector

# Snowpark connection configuration
account = 'nszzdnc-xa32927'
user = None
password = None

# Page styling and logo
st.set_page_config(
    initial_sidebar_state="collapsed",
    page_title="SIEMTrax",
    page_icon='siemtrax_logo.jpg'
)

# Logo image
logo_image = 'siemtrax_logo.jpg'
st.image(logo_image, use_column_width=True)

# Authenticate using Snowflake connector
def authenticate(username, password):
    connection = snowflake.connector.connect(
        user=username,
        password=password,
        account=account
    )
    # Perform authentication and return the connection object
    return connection

# Define the SessionState class for session handling
class SessionState:
    def __init__(self, username=None):
        self.username = username
        self.password = None
        self.connection = None

# Get or create the session state
def get_session_state():
    if 'session' not in st.session_state:
        st.session_state['session'] = SessionState()
    return st.session_state['session']

# Get or create the session state
session_state = get_session_state()

# Check if the user is already authenticated
authenticated = session_state.connection is not None

if not authenticated:
    # Username and password input
    username = st.text_input('Username')
    password = st.text_input('Password', type='password')

    # Submit button for login
    if st.button('Login'):
        if username and password:
            session_state.username = username
            session_state.password = password
            session_state.connection = authenticate(username, password)

            # Clear the password field after authentication
            session_state.password = None

            # Refresh the page after login
            st.experimental_rerun()

# Check if the user is authenticated before rendering menu sections
if session_state.connection or authenticated:
    menu = ['Alerts', 'Dashboards', 'Security Use Cases', 'Reports']
    choice = st.selectbox('Menu', menu)

    if choice == 'Alerts':
        # Add your code for the Alerts section
        st.write('This is the Alerts section.')

    elif choice == 'Dashboards':
        # Add your code for the Dashboards section
        st.write('This is the Dashboards section.')

    elif choice == 'Security Use Cases':
        # Add your code for the Security Use Cases section
        st.write('This is the Security Use Cases section.')

    elif choice == 'Reports':
        # Add your code for the Reports section
        st.write('This is the Reports section.')

# Logout button at the bottom right
col1, col2 = st.beta_columns([1, 3])
with col2:
    if st.button('Logout', key='logout_button'):
        # Clear the session and refresh the page
        session_state.connection = None
        session_state.username = None
        session_state.password = None
        st.experimental_rerun()
