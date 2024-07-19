import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np
from statsmodels.tsa.stattools import acf
import joblib
import json

def run_model_and_forecast(historical_file_path, new_day_file_path):
    # Read the parquet files
    historical_df = pd.read_parquet(historical_file_path)
    new_day_df = pd.read_parquet(new_day_file_path)

    # Combine the two dataframes
    df = pd.concat([historical_df, new_day_df])

    # Convert 'date_de_fin' to datetime
    df['date de fin'] = pd.to_datetime(df['date de fin'])

    # Sort by date
    df = df.sort_values('date de fin')

    # Set the date as the index
    df.set_index('date de fin', inplace=True)

    # Only keep the 'valeur' column for forecasting
    ts = df['valeur']

    # Remove duplicate dates by averaging their values
    ts = ts.groupby(ts.index).mean()

    # Ensure the time series has a daily frequency
    ts = ts.asfreq('D')

    # Define the ARIMA model
    model = ARIMA(ts, order=(4, 2, 2))  # (p, d, q) parameters should be tuned
    model_fit = model.fit()

    # Summary of the model
    print(model_fit.summary())

    # Generate in-sample predictions for the last 7 days
    in_sample_forecast_start = len(ts) - 7
    in_sample_forecast_end = len(ts) - 1
    in_sample_forecast = model_fit.predict(start=in_sample_forecast_start, end=in_sample_forecast_end)

    # Forecast the next 7 days
    steps = 7
    forecast = model_fit.get_forecast(steps=steps)
    forecast_values = forecast.predicted_mean

    # Create date range for forecast
    forecast_index = pd.date_range(start=ts.index[-1] + pd.Timedelta(days=1), periods=steps, freq='D')

    # Convert forecast to a Series
    forecast_series = pd.Series(forecast_values, index=forecast_index)

    # Combine the original and forecasted values
    combined = pd.concat([ts, forecast_series])

    # Calculate metrics for the in-sample predictions
    actual_values = ts[in_sample_forecast_start:in_sample_forecast_end + 1]
    mae = mean_absolute_error(actual_values, in_sample_forecast)
    mse = mean_squared_error(actual_values, in_sample_forecast)
    rmse = np.sqrt(mse)

    print(f"Mean Absolute Error (MAE): {mae}")
    print(f"Mean Squared Error (MSE): {mse}")
    print(f"Root Mean Squared Error (RMSE): {rmse}")

    # Calculate ACF1
    residuals = actual_values - in_sample_forecast
    acf_values = acf(residuals, nlags=1)
    acf1 = acf_values[1]

    print(f"ACF1: {acf1}")

    # Calculate MAPE
    mape = np.mean(np.abs((actual_values - in_sample_forecast) / actual_values)) * 100
    print(f"Mean Absolute Percentage Error (MAPE): {mape}%")

    # Save the model
    joblib.dump(model_fit, 'model.pkl')

    # Prepare JSON output
    metrics = {
        "MAE": mae,
        "MSE": mse,
        "RMSE": rmse,
        "ACF1": acf1,
        "MAPE": mape
    }

    forecast_data = [
        {"date": date.strftime("%Y-%m-%d"), "value": value, "type": "forecast"}
        for date, value in forecast_series.items()
    ]

    historical_data = [
        {"date": date.strftime("%Y-%m-%d"), "value": value, "type": "historical"}
        for date, value in ts.items()
    ]

    in_sample_forecast_data = [
        {"date": date.strftime("%Y-%m-%d"), "value": value, "type": "historical_forecast"}
        for date, value in zip(actual_values.index, in_sample_forecast)
    ]

    json_output = {
        "metrics": metrics,
        "data": historical_data + in_sample_forecast_data + forecast_data
    }

    return json_output, metrics

# Paths to your parquet files
historical_file_path = '/opt/airflow/dags/data/gazs_output_parquet/main_data.parquet'
new_day_file_path = '/opt/airflow/dags/data/gazs_output_parquet/ZAG_PARIS_combined_output.parquet'

# Run the model and get the JSON output
json_result, metrics = run_model_and_forecast(historical_file_path, new_day_file_path)
print(json.dumps(json_result, indent=4))
