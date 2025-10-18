import pandas as pd

# Load cleaned data
df = pd.read_csv("../data/cleaned/cleaned_taxi_data.csv")

print("=" * 60)
print("CLEANED DATA VERIFICATION")
print("=" * 60)

# Basic info
print(f"\nDataset Shape: {df.shape}")
print(f"Total Rows: {len(df):,}")
print(f"Total Columns: {len(df.columns)}")

# Column names
print(f"\nColumns ({len(df.columns)}):")
for i, col in enumerate(df.columns, 1):
    print(f"  {i:2d}. {col}")

# Check for any remaining issues
print("\n" + "=" * 60)
print("DATA QUALITY VERIFICATION")
print("=" * 60)

print("\n1. Missing Values:")
missing = df.isnull().sum()
if missing.sum() == 0:
    print("   ✓ No missing values!")
else:
    print(missing[missing > 0])

print("\n2. Value Ranges:")
print(
    f"   Passenger count: {df['passenger_count'].min()} to {df['passenger_count'].max()}"
)
print(
    f"   Trip duration: {df['trip_duration'].min()} to {df['trip_duration'].max()} seconds"
)
print(
    f"   Trip distance: {df['trip_distance_miles'].min():.2f} to {df['trip_distance_miles'].max():.2f} miles"
)
print(
    f"   Average speed: {df['avg_speed_mph'].min():.2f} to {df['avg_speed_mph'].max():.2f} mph"
)

print("\n3. Coordinate Bounds:")
print(
    f"   Pickup latitude: {df['pickup_latitude'].min():.6f} to {df['pickup_latitude'].max():.6f}"
)
print(
    f"   Pickup longitude: {df['pickup_longitude'].min():.6f} to {df['pickup_longitude'].max():.6f}"
)
print(
    f"   Dropoff latitude: {df['dropoff_latitude'].min():.6f} to {df['dropoff_latitude'].max():.6f}"
)
print(
    f"   Dropoff longitude: {df['dropoff_longitude'].min():.6f} to {df['dropoff_longitude'].max():.6f}"
)

print("\n4. Summary Statistics:")
print(
    df[
        [
            "trip_duration",
            "trip_duration_minutes",
            "trip_distance_miles",
            "avg_speed_mph",
            "passenger_count",
        ]
    ].describe()
)

print("\n5. Categorical Features:")
print(f"\n   Time of Day Distribution:")
print(df["time_of_day"].value_counts())

print(f"\n   Day of Week Distribution:")
print(df["pickup_day_name"].value_counts())

print(f"\n   Speed Category Distribution:")
print(df["speed_category"].value_counts())

print("\n6. Sample Records (first 5):")
print(
    df[
        [
            "pickup_datetime",
            "trip_duration_minutes",
            "trip_distance_miles",
            "avg_speed_mph",
            "time_of_day",
            "is_weekend",
        ]
    ].head()
)

print("\n" + "=" * 60)
print("VERIFICATION COMPLETE!")
print("=" * 60)
print("\nData is ready for database import!")