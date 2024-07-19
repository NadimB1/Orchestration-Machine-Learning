import streamlit as st
import requests
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pandas as pd

# Function to check user credentials
def check_user(username, password):
    return username == "admin" and password == "admin"

# Function to display the login form
def login_user():
    st.title("Login")
    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submit_button = st.form_submit_button("Login")
        
        if submit_button:
            if check_user(username, password):
                st.session_state['logged_in'] = True
                st.session_state['username'] = username
                st.experimental_rerun()  # Reload the app to update the state
            else:
                st.error("Invalid username or password")

# Function to display the air quality forecast
def display_forecast():
    st.title("Air Quality Forecast")
    
    # Mapping user-friendly descriptions to numerical values
    options = {
        "Tomorrow": 1,
        "2 Days": 2,
        "4 Days": 3,
        "Next Week": 4
    }

    # Options for forecast days with user-friendly descriptions
    selected_option = st.selectbox("Select forecast option:", list(options.keys()))

    # Button to get forecast
    if st.button("Get Forecast"):
        # Fetch forecast data from FastAPI
        response = requests.get(f"http://fastapi:8000/forecast/?option={options[selected_option]}")

        if response.status_code == 200:
            data = response.json()

            # Display Metrics
            st.subheader("Metrics")
            if "metrics" in data:
                metrics_df = pd.DataFrame([data["metrics"][0]["metrics"]])
                st.table(metrics_df)  # Display metrics in a table format
            else:
                st.error("Metrics not found in the response")


            # Check if there is forecast_chart data
            st.subheader("Forecast Chart")
            if "forecast_chart" in data:
                forecast_chart = data["forecast_chart"]
                fig = go.Figure(data=forecast_chart["data"], layout=forecast_chart["layout"])

                # Assuming additional forecast and historical data are provided in the metrics
                if "metrics" in data and "data" in data["metrics"][0]:
                    all_data = data["metrics"][0]["data"]

                    # Convert to DataFrame for easier manipulation
                    df = pd.DataFrame(all_data)
                    df['date'] = pd.to_datetime(df['date'])  # Ensure 'date' is datetime type

                    # Filter data to only show from the last month
                    one_month_ago = datetime.now() - timedelta(days=30)
                    df = df[df['date'] >= one_month_ago]

                    # Separate historical and forecast data after filtering
                    historical_df = df[df['type'] == 'historical']
                    forecast_historical_df = df[df['type'] == 'historical_forecast']

                    # Plot historical data
                    fig.add_trace(go.Scatter(
                        x=historical_df['date'],
                        y=historical_df['value'],
                        mode='lines+markers',
                        name='Historical',
                        marker=dict(color='red')
                    ))
                    fig.update_layout(title='Air Quality Forecast with Historical Data')
                    # Plot historical data
                    fig.add_trace(go.Scatter(
                        x=forecast_historical_df['date'],
                        y=forecast_historical_df['value'],
                        mode='lines+markers',
                        name='historical_forecast',
                        marker=dict(color='green')
                    ))
                    fig.update_layout(title='Air Quality Forecast with Historical Data')

                # Display the updated chart
                st.plotly_chart(fig)
            else:
                st.error("Forecast chart not found in the response")
        else:
            st.error("Failed to fetch forecast data")

# Main function to control the app flow
def main():
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False

    if st.session_state['logged_in']:
        display_forecast()
    else:
        login_user()

if __name__ == "__main__":
    main()
