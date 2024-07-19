from fastapi import FastAPI, HTTPException
import joblib
from datetime import datetime
import pandas as pd
import plotly.graph_objects as go
import json

app = FastAPI()

# Load the ARIMA model
model = joblib.load('model/model.pkl')

# Load metrics from a JSON file
def load_metrics():
    with open('/app/model/metrics.json', 'r') as file:
        metrics = json.load(file)
    return metrics

@app.get("/forecast/")
async def forecast(option: int):
    if option not in [1, 2, 3, 4]:
        raise HTTPException(status_code=400, detail="Invalid option provided")

    # Map option to days: 1 day, 2 days, 4 days, and 7 days
    forecast_days = [1, 2, 4, 7][option - 1]

    # Use the loaded model to forecast
    forecast_index = pd.date_range(start=datetime.now(), periods=forecast_days + 1, freq='D')[1:]  # Start forecasting from tomorrow
    forecast_values = model.get_forecast(steps=forecast_days).predicted_mean

    # Load metrics
    metrics = load_metrics()

    # Plotly chart
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=forecast_index, y=forecast_values, mode='lines+markers', name='Forecast'))
    fig.update_layout(title='Air Quality Forecast', xaxis_title='Date', yaxis_title='Air Quality Index')

    # Convert Plotly figure to JSON
    graph_json = json.loads(fig.to_json())

    return {
        "option": option,
        "metrics": metrics,
        "forecast_chart": graph_json
    }

# Run the server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
