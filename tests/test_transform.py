import pandas as pd


def test_weather_columns():
    dataframe = pd.DataFrame([
        {
            "latitude": 50.638,
            "longitude": 3.045,
            "observation_time": "2026-09-10T08:00",
            "temperature": 14.1,
            "humidity": 75,
            "wind_speed": 10.1,
        }
    ])

    expected_columns = [
        "latitude",
        "longitude",
        "observation_time",
        "temperature",
        "humidity",
        "wind_speed",
    ]

    assert list(dataframe.columns) == expected_columns