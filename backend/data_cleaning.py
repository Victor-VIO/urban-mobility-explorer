from datetime import datetime

import numpy as np
import pandas as pd

# For tracking what we remove
cleaning_log = []


def log_step(message):
    """Helper function to log cleaning steps"""
    print(message)
    cleaning_log.append(message)


def load_data(filepath):
    """Load the raw taxi data"""
    print("Loading data...")
    df = pd.read_csv(filepath)

    log_step(f"Initial dataset size: {len(df)} rows, {len(df.columns)} columns")
    log_step(f"Columns: {list(df.columns)}")
    log_step(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

    # Show first few rows
    print("\nFirst 5 rows:")
    print(df.head())

    # Show data types
    print("\nData types:")
    print(df.dtypes)

    # Show basic statistics
    print("\nBasic statistics:")
    print(df.describe())

    return df


def check_data_quality(df):
    """Identify data quality issues"""
    print("\n" + "=" * 50)
    print("DATA QUALITY CHECK")
    print("=" * 50)

    # 1. Missing values
    print("\n1. Missing Values:")
    missing = df.isnull().sum()
    missing_pct = 100 * missing / len(df)
    missing_df = pd.DataFrame(
        {
            "Column": missing.index,
            "Missing_Count": missing.values,
            "Percentage": missing_pct.values,
        }
    )
    print(missing_df[missing_df["Missing_Count"] > 0])

    if missing_df["Missing_Count"].sum() == 0:
        print("   No missing values found!")

    # 2. Duplicates
    duplicates = df.duplicated().sum()
    print(f"\n2. Duplicate rows: {duplicates}")

    # 3. Check specific columns for issues
    print("\n3. Value Range Checks:")

    # Check trip_duration (in seconds)
    print(f"\n   TRIP DURATION:")
    print(f"   - Negative durations: {(df['trip_duration'] < 0).sum()}")
    print(f"   - Zero durations: {(df['trip_duration'] == 0).sum()}")
    print(f"   - Very short trips (<60 sec): {(df['trip_duration'] < 60).sum()}")
    print(f"   - Very long trips (>3 hours): {(df['trip_duration'] > 10800).sum()}")
    print(
        f"   - Extremely long trips (>24 hours): {(df['trip_duration'] > 86400).sum()}"
    )
    print(
        f"   - Range: {df['trip_duration'].min()} to {df['trip_duration'].max()} seconds"
    )
    print(
        f"   - Mean: {df['trip_duration'].mean():.2f} seconds ({df['trip_duration'].mean()/60:.2f} minutes)"
    )

    # Check passenger_count
    print(f"\n   PASSENGER COUNT:")
    print(f"   - Zero passengers: {(df['passenger_count'] == 0).sum()}")
    print(f"   - Invalid counts (<1): {(df['passenger_count'] < 1).sum()}")
    print(f"   - Suspicious counts (>6): {(df['passenger_count'] > 6).sum()}")
    print(f"   - Range: {df['passenger_count'].min()} to {df['passenger_count'].max()}")
    print(f"   - Distribution:")
    print(df["passenger_count"].value_counts().sort_index())

    # Check coordinates
    print(f"\n   COORDINATES:")

    # Pickup coordinates
    print(f"   Pickup:")
    print(
        f"   - Latitude range: {df['pickup_latitude'].min():.6f} to {df['pickup_latitude'].max():.6f}"
    )
    print(
        f"   - Longitude range: {df['pickup_longitude'].min():.6f} to {df['pickup_longitude'].max():.6f}"
    )
    print(
        f"   - Outside NYC latitude bounds (40.5-41.0): {((df['pickup_latitude'] < 40.5) | (df['pickup_latitude'] > 41.0)).sum()}"
    )
    print(
        f"   - Outside NYC longitude bounds (-74.3 to -73.7): {((df['pickup_longitude'] < -74.3) | (df['pickup_longitude'] > -73.7)).sum()}"
    )

    # Dropoff coordinates
    print(f"   Dropoff:")
    print(
        f"   - Latitude range: {df['dropoff_latitude'].min():.6f} to {df['dropoff_latitude'].max():.6f}"
    )
    print(
        f"   - Longitude range: {df['dropoff_longitude'].min():.6f} to {df['dropoff_longitude'].max():.6f}"
    )
    print(
        f"   - Outside NYC latitude bounds (40.5-41.0): {((df['dropoff_latitude'] < 40.5) | (df['dropoff_latitude'] > 41.0)).sum()}"
    )
    print(
        f"   - Outside NYC longitude bounds (-74.3 to -73.7): {((df['dropoff_longitude'] < -74.3) | (df['dropoff_longitude'] > -73.7)).sum()}"
    )

    # Check vendor_id
    print(f"\n   VENDOR ID:")
    print(f"   - Distribution:")
    print(df["vendor_id"].value_counts().sort_index())

    # Check store_and_fwd_flag
    print(f"\n   STORE AND FORWARD FLAG:")
    print(df["store_and_fwd_flag"].value_counts())

    # Look for obvious outliers
    print("\n" + "=" * 50)
    print("4. Potential Outliers (statistical):")
    print("=" * 50)

    # Trip duration outliers
    Q1_duration = df["trip_duration"].quantile(0.25)
    Q3_duration = df["trip_duration"].quantile(0.75)
    IQR_duration = Q3_duration - Q1_duration
    lower_duration = Q1_duration - 1.5 * IQR_duration
    upper_duration = Q3_duration + 1.5 * IQR_duration
    outliers_duration = (
        (df["trip_duration"] < lower_duration) | (df["trip_duration"] > upper_duration)
    ).sum()
    print(
        f"   Trip duration outliers: {outliers_duration} ({100*outliers_duration/len(df):.2f}%)"
    )
    print(f"   Normal range: {lower_duration:.0f} to {upper_duration:.0f} seconds")

    return missing_df


def clean_missing_values(df):
    """Handle missing values"""
    print("\n" + "=" * 50)
    print("STEP 1: Cleaning Missing Values")
    print("=" * 50)

    initial_rows = len(df)

    # Check for any missing values in critical columns
    critical_columns = [
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_longitude",
        "pickup_latitude",
        "dropoff_longitude",
        "dropoff_latitude",
        "trip_duration",
        "passenger_count",
    ]

    # Remove rows with missing critical values
    df = df.dropna(subset=critical_columns)

    removed = initial_rows - len(df)
    log_step(f"Removed {removed} rows with missing critical values")
    log_step(f"Remaining rows: {len(df)}")

    return df


def remove_duplicates(df):
    """Remove duplicate records"""
    print("\n" + "=" * 50)
    print("STEP 2: Removing Duplicates")
    print("=" * 50)

    initial_rows = len(df)
    df = df.drop_duplicates()
    removed = initial_rows - len(df)

    log_step(f"Removed {removed} duplicate rows")
    log_step(f"Remaining rows: {len(df)}")

    return df


def remove_invalid_values(df):
    """Remove physically impossible values"""
    print("\n" + "=" * 50)
    print("STEP 3: Removing Invalid Values")
    print("=" * 50)

    initial_rows = len(df)

    # Remove zero or negative trip durations
    before = len(df)
    df = df[df["trip_duration"] > 0]
    log_step(f"Removed {before - len(df)} rows with zero or negative trip duration")

    # Remove extremely long trips (over 24 hours = 86400 seconds)
    before = len(df)
    df = df[df["trip_duration"] <= 86400]
    log_step(f"Removed {before - len(df)} rows with trip duration > 24 hours")

    # Remove very short trips (less than 10 seconds - likely errors)
    before = len(df)
    df = df[df["trip_duration"] >= 10]
    log_step(f"Removed {before - len(df)} rows with trip duration < 10 seconds")

    # Remove invalid passenger counts (0 or more than 6)
    before = len(df)
    df = df[(df["passenger_count"] >= 1) & (df["passenger_count"] <= 6)]
    log_step(f"Removed {before - len(df)} rows with invalid passenger counts (0 or >6)")

    # Remove coordinates outside NYC bounds
    # NYC approximate bounds: lat 40.5-41.0, lon -74.3 to -73.7
    before = len(df)
    df = df[
        (df["pickup_latitude"] >= 40.5)
        & (df["pickup_latitude"] <= 41.0)
        & (df["pickup_longitude"] >= -74.3)
        & (df["pickup_longitude"] <= -73.7)
    ]
    log_step(f"Removed {before - len(df)} rows with invalid pickup coordinates")

    before = len(df)
    df = df[
        (df["dropoff_latitude"] >= 40.5)
        & (df["dropoff_latitude"] <= 41.0)
        & (df["dropoff_longitude"] >= -74.3)
        & (df["dropoff_longitude"] <= -73.7)
    ]
    log_step(f"Removed {before - len(df)} rows with invalid dropoff coordinates")

    total_removed = initial_rows - len(df)
    log_step(f"Total removed in this step: {total_removed}")
    log_step(f"Remaining rows: {len(df)}")

    return df


def remove_outliers(df):
    """Remove statistical outliers using IQR method"""
    print("\n" + "=" * 50)
    print("STEP 4: Removing Outliers")
    print("=" * 50)

    initial_rows = len(df)

    # Remove trip_duration outliers
    before = len(df)

    # Calculate IQR for trip_duration
    Q1 = df["trip_duration"].quantile(0.25)
    Q3 = df["trip_duration"].quantile(0.75)
    IQR = Q3 - Q1

    # Define bounds (using 1.5 * IQR)
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    # Make sure lower bound is at least 0
    lower_bound = max(0, lower_bound)

    # Filter
    df = df[(df["trip_duration"] >= lower_bound) & (df["trip_duration"] <= upper_bound)]

    removed = before - len(df)
    log_step(f"Removed {removed} outliers from trip_duration")
    log_step(
        f"  - Bounds: {lower_bound:.0f} to {upper_bound:.0f} seconds ({lower_bound/60:.1f} to {upper_bound/60:.1f} minutes)"
    )

    total_removed = initial_rows - len(df)
    log_step(f"Total outliers removed: {total_removed}")
    log_step(f"Remaining rows: {len(df)}")

    return df


def create_derived_features(df):
    """Create new calculated columns"""
    print("\n" + "=" * 50)
    print("STEP 5: Creating Derived Features")
    print("=" * 50)

    initial_rows = len(df)

    # Convert datetime columns
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
    df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"])
    log_step("Converted datetime columns to datetime objects")

    # Feature 1: Trip duration in minutes (convert from seconds)
    df["trip_duration_minutes"] = df["trip_duration"] / 60
    log_step("Created feature: trip_duration_minutes")

    # Feature 2: Trip distance (Haversine formula)
    # This calculates straight-line distance between pickup and dropoff
    def haversine_distance(lat1, lon1, lat2, lon2):
        """Calculate distance between two points on Earth in miles"""
        # Convert to radians
        lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])

        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
        c = 2 * np.arcsin(np.sqrt(a))

        # Radius of Earth in miles
        r = 3959

        return c * r

    df["trip_distance_miles"] = haversine_distance(
        df["pickup_latitude"],
        df["pickup_longitude"],
        df["dropoff_latitude"],
        df["dropoff_longitude"],
    )
    log_step("Created feature: trip_distance_miles (Haversine distance)")

    # Feature 3: Average speed (mph)
    df["avg_speed_mph"] = df["trip_distance_miles"] / (df["trip_duration"] / 3600)
    df["avg_speed_mph"] = df["avg_speed_mph"].replace([np.inf, -np.inf], 0)

    # Remove unrealistic speeds (over 100 mph is suspicious)
    before = len(df)
    df = df[df["avg_speed_mph"] <= 100]
    log_step(f"Removed {before - len(df)} rows with unrealistic speeds (>100 mph)")
    log_step("Created feature: avg_speed_mph")

    # Feature 4: Hour of day
    df = df.copy()
    df["pickup_hour"] = df["pickup_datetime"].dt.hour
    log_step("Created feature: pickup_hour")

    # Feature 5: Day of week (0=Monday, 6=Sunday)
    df["pickup_day_of_week"] = df["pickup_datetime"].dt.dayofweek
    log_step("Created feature: pickup_day_of_week")

    # Feature 6: Day name
    df["pickup_day_name"] = df["pickup_datetime"].dt.day_name()
    log_step("Created feature: pickup_day_name")

    # Feature 7: Month
    df["pickup_month"] = df["pickup_datetime"].dt.month
    log_step("Created feature: pickup_month")

    # Feature 8: Is weekend?
    df["is_weekend"] = df["pickup_day_of_week"].isin([5, 6]).astype(int)
    log_step("Created feature: is_weekend")

    # Feature 9: Time of day category
    def categorize_time(hour):
        if 6 <= hour < 12:
            return "morning"
        elif 12 <= hour < 17:
            return "afternoon"
        elif 17 <= hour < 21:
            return "evening"
        else:
            return "night"

    df["time_of_day"] = df["pickup_hour"].apply(categorize_time)
    log_step("Created feature: time_of_day")

    # Feature 10: Rush hour flag (morning: 7-9am, evening: 5-7pm)
    df["is_rush_hour"] = df["pickup_hour"].isin([7, 8, 17, 18]).astype(int)
    log_step("Created feature: is_rush_hour")

    # Feature 11: Trip speed category
    def categorize_speed(speed):
        if speed < 5:
            return "very_slow"
        elif speed < 15:
            return "slow"
        elif speed < 25:
            return "moderate"
        elif speed < 40:
            return "fast"
        else:
            return "very_fast"

    df["speed_category"] = df["avg_speed_mph"].apply(categorize_speed)
    log_step("Created feature: speed_category")

    log_step(f"Total features in dataset: {len(df.columns)}")
    log_step(f"Rows after feature creation: {len(df)}")

    return df


def save_cleaned_data(df, output_path):
    """Save the cleaned dataset"""
    print("\n" + "=" * 50)
    print("STEP 6: Saving Cleaned Data")
    print("=" * 50)

    # Save to CSV
    df.to_csv(output_path, index=False)
    log_step(f"Saved cleaned data to: {output_path}")
    log_step(f"Final dataset size: {len(df)} rows, {len(df.columns)} columns")

    # Save cleaning log
    log_path = "./logs/cleaning_log.txt"
    with open(log_path, "w") as f:
        f.write("DATA CLEANING LOG\n")
        f.write("=" * 50 + "\n")
        f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 50 + "\n\n")
        for entry in cleaning_log:
            f.write(entry + "\n")

    print(f"\nCleaning log saved to: {log_path}")

    # Print summary statistics
    print("\n" + "=" * 50)
    print("FINAL DATA SUMMARY")
    print("=" * 50)
    print(f"Columns: {list(df.columns)}")
    print(f"\nSample statistics:")
    print(
        f"  Trip duration: {df['trip_duration'].mean():.2f} seconds ({df['trip_duration_minutes'].mean():.2f} minutes)"
    )
    print(f"  Trip distance: {df['trip_distance_miles'].mean():.2f} miles")
    print(f"  Average speed: {df['avg_speed_mph'].mean():.2f} mph")
    print(f"  Passenger count: {df['passenger_count'].mean():.2f}")

    return df


def main():
    """Main execution pipeline"""
    print("=" * 50)
    print("NYC TAXI DATA CLEANING PIPELINE")
    print("=" * 50)

    # Load data
    df = load_data("../data/raw/train.csv")

    # Check quality
    check_data_quality(df)

    # Record initial count
    initial_count = len(df)

    # Clean step by step
    df = clean_missing_values(df)
    df = remove_duplicates(df)
    df = remove_invalid_values(df)
    df = remove_outliers(df)
    df = create_derived_features(df)

    # Save
    df = save_cleaned_data(df, "../data/cleaned/cleaned_taxi_data.csv")

    # Final summary
    print("\n" + "=" * 50)
    print("CLEANING COMPLETE!")
    print("=" * 50)
    print(f"Original rows: {initial_count:,}")
    print(f"Final rows: {len(df):,}")
    print(f"Rows removed: {initial_count - len(df):,}")
    print(f"Retention rate: {100 * len(df) / initial_count:.2f}%")
    print(f"Final columns: {len(df.columns)}")


if __name__ == "__main__":
    main()
