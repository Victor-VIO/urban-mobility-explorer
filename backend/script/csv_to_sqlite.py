import os
import sqlite3

import pandas as pd


def create_database_schema(db_path):
    """Create the database schema with proper indexes"""
    print("=" * 60)
    print("CREATING DATABASE SCHEMA")
    print("=" * 60)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Drop existing tables if they exist
    cursor.execute("DROP TABLE IF EXISTS trip_categories")
    cursor.execute("DROP TABLE IF EXISTS trip_temporal")
    cursor.execute("DROP TABLE IF EXISTS trips")

    # Table 1: Main trips table
    cursor.execute(
        """
        CREATE TABLE trips (
            id TEXT PRIMARY KEY,
            vendor_id INTEGER NOT NULL,
            pickup_datetime DATETIME NOT NULL,
            dropoff_datetime DATETIME NOT NULL,
            passenger_count INTEGER NOT NULL,
            pickup_longitude REAL NOT NULL,
            pickup_latitude REAL NOT NULL,
            dropoff_longitude REAL NOT NULL,
            dropoff_latitude REAL NOT NULL,
            store_and_fwd_flag TEXT,
            trip_duration INTEGER NOT NULL,
            trip_duration_minutes REAL NOT NULL,
            trip_distance_miles REAL NOT NULL,
            avg_speed_mph REAL NOT NULL
        )
    """
    )
    print("✓ Created table: trips")

    # Table 2: Temporal features
    cursor.execute(
        """
        CREATE TABLE trip_temporal (
            trip_id TEXT PRIMARY KEY,
            pickup_hour INTEGER NOT NULL,
            pickup_day_of_week INTEGER NOT NULL,
            pickup_day_name TEXT NOT NULL,
            pickup_month INTEGER NOT NULL,
            is_weekend INTEGER NOT NULL,
            time_of_day TEXT NOT NULL,
            is_rush_hour INTEGER NOT NULL,
            FOREIGN KEY (trip_id) REFERENCES trips(id) ON DELETE CASCADE
        )
    """
    )
    print("✓ Created table: trip_temporal")

    # Table 3: Categories
    cursor.execute(
        """
        CREATE TABLE trip_categories (
            trip_id TEXT PRIMARY KEY,
            speed_category TEXT NOT NULL,
            FOREIGN KEY (trip_id) REFERENCES trips(id) ON DELETE CASCADE
        )
    """
    )
    print("✓ Created table: trip_categories")

    # Create indexes for fast queries
    print("\nCreating indexes...")

    # Indexes on trips table
    cursor.execute("CREATE INDEX idx_trips_pickup_datetime ON trips(pickup_datetime)")
    cursor.execute("CREATE INDEX idx_trips_vendor_id ON trips(vendor_id)")
    cursor.execute("CREATE INDEX idx_trips_passenger_count ON trips(passenger_count)")
    cursor.execute(
        "CREATE INDEX idx_trips_pickup_coords ON trips(pickup_latitude, pickup_longitude)"
    )
    cursor.execute(
        "CREATE INDEX idx_trips_dropoff_coords ON trips(dropoff_latitude, dropoff_longitude)"
    )
    cursor.execute("CREATE INDEX idx_trips_duration ON trips(trip_duration)")
    cursor.execute("CREATE INDEX idx_trips_distance ON trips(trip_distance_miles)")
    cursor.execute("CREATE INDEX idx_trips_speed ON trips(avg_speed_mph)")
    print("✓ Created 8 indexes on trips table")

    # Indexes on trip_temporal table
    cursor.execute(
        "CREATE INDEX idx_temporal_pickup_hour ON trip_temporal(pickup_hour)"
    )
    cursor.execute(
        "CREATE INDEX idx_temporal_day_of_week ON trip_temporal(pickup_day_of_week)"
    )
    cursor.execute("CREATE INDEX idx_temporal_month ON trip_temporal(pickup_month)")
    cursor.execute("CREATE INDEX idx_temporal_is_weekend ON trip_temporal(is_weekend)")
    cursor.execute(
        "CREATE INDEX idx_temporal_time_of_day ON trip_temporal(time_of_day)"
    )
    cursor.execute(
        "CREATE INDEX idx_temporal_is_rush_hour ON trip_temporal(is_rush_hour)"
    )
    print("✓ Created 6 indexes on trip_temporal table")

    # Index on trip_categories table
    cursor.execute(
        "CREATE INDEX idx_categories_speed_category ON trip_categories(speed_category)"
    )
    print("✓ Created 1 index on trip_categories table")

    conn.commit()
    conn.close()

    print(f"\n✓ Database schema created successfully at: {db_path}")
    print(f"✓ Total indexes created: 15")


def load_data_to_database(csv_path, db_path, batch_size=10000):
    """Load cleaned CSV data into SQLite database"""
    print("\n" + "=" * 60)
    print("LOADING DATA INTO DATABASE")
    print("=" * 60)

    # Read the cleaned CSV
    print(f"\nReading CSV: {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"✓ Loaded {len(df):,} rows")

    # Connect to database
    conn = sqlite3.connect(db_path)

    # Prepare data for trips table
    print("\nPreparing data for 'trips' table...")
    trips_df = df[
        [
            "id",
            "vendor_id",
            "pickup_datetime",
            "dropoff_datetime",
            "passenger_count",
            "pickup_longitude",
            "pickup_latitude",
            "dropoff_longitude",
            "dropoff_latitude",
            "store_and_fwd_flag",
            "trip_duration",
            "trip_duration_minutes",
            "trip_distance_miles",
            "avg_speed_mph",
        ]
    ].copy()

    # Prepare data for trip_temporal table
    print("Preparing data for 'trip_temporal' table...")
    temporal_df = df[
        [
            "id",
            "pickup_hour",
            "pickup_day_of_week",
            "pickup_day_name",
            "pickup_month",
            "is_weekend",
            "time_of_day",
            "is_rush_hour",
        ]
    ].copy()
    temporal_df.columns = [
        "trip_id",
        "pickup_hour",
        "pickup_day_of_week",
        "pickup_day_name",
        "pickup_month",
        "is_weekend",
        "time_of_day",
        "is_rush_hour",
    ]

    # Prepare data for trip_categories table
    print("Preparing data for 'trip_categories' table...")
    categories_df = df[["id", "speed_category"]].copy()
    categories_df.columns = ["trip_id", "speed_category"]

    # Insert data in batches
    print(f"\nInserting data in batches of {batch_size:,}...")

    total_rows = len(df)

    # Insert trips
    print("\nInserting into 'trips' table...")
    for i in range(0, len(trips_df), batch_size):
        batch = trips_df.iloc[i : i + batch_size]
        batch.to_sql("trips", conn, if_exists="append", index=False)
        progress = min(i + batch_size, total_rows)
        print(
            f"  Progress: {progress:,}/{total_rows:,} ({100*progress/total_rows:.1f}%)",
            end="\r",
        )
    print(f"\n✓ Inserted {len(trips_df):,} rows into 'trips' table")

    # Insert temporal
    print("\nInserting into 'trip_temporal' table...")
    for i in range(0, len(temporal_df), batch_size):
        batch = temporal_df.iloc[i : i + batch_size]
        batch.to_sql("trip_temporal", conn, if_exists="append", index=False)
        progress = min(i + batch_size, total_rows)
        print(
            f"  Progress: {progress:,}/{total_rows:,} ({100*progress/total_rows:.1f}%)",
            end="\r",
        )
    print(f"\n✓ Inserted {len(temporal_df):,} rows into 'trip_temporal' table")

    # Insert categories
    print("\nInserting into 'trip_categories' table...")
    for i in range(0, len(categories_df), batch_size):
        batch = categories_df.iloc[i : i + batch_size]
        batch.to_sql("trip_categories", conn, if_exists="append", index=False)
        progress = min(i + batch_size, total_rows)
        print(
            f"  Progress: {progress:,}/{total_rows:,} ({100*progress/total_rows:.1f}%)",
            end="\r",
        )
    print(f"\n✓ Inserted {len(categories_df):,} rows into 'trip_categories' table")

    conn.close()
    print("\n✓ Data loading complete!")


def verify_database(db_path):
    """Verify the database was created correctly"""
    print("\n" + "=" * 60)
    print("DATABASE VERIFICATION")
    print("=" * 60)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print(f"\nTables created: {len(tables)}")
    for table in tables:
        print(f"  - {table[0]}")

    # Count rows in each table
    print("\nRow counts:")
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
        count = cursor.fetchone()[0]
        print(f"  - {table[0]}: {count:,} rows")

    # Get index information
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
    indexes = cursor.fetchall()
    print(f"\nIndexes created: {len(indexes)}")

    # Sample query
    print("\nSample query (first 5 trips):")
    cursor.execute(
        """
        SELECT 
            t.id,
            t.pickup_datetime,
            t.trip_duration_minutes,
            t.trip_distance_miles,
            t.avg_speed_mph,
            tt.time_of_day,
            tc.speed_category
        FROM trips t
        JOIN trip_temporal tt ON t.id = tt.trip_id
        JOIN trip_categories tc ON t.id = tc.trip_id
        LIMIT 5
    """
    )

    results = cursor.fetchall()
    for row in results:
        print(
            f"  {row[0]}: {row[1]} | {row[2]:.2f} min | {row[3]:.2f} mi | {row[4]:.2f} mph | {row[5]} | {row[6]}"
        )

    # Database file size
    db_size = os.path.getsize(db_path) / (1024**2)  # Convert to MB
    print(f"\nDatabase file size: {db_size:.2f} MB")

    conn.close()
    print("\n✓ Database verification complete!")


def main():
    """Main execution pipeline"""
    print("=" * 60)
    print("NYC TAXI DATA - CSV TO SQLITE CONVERSION")
    print("=" * 60)

    # Configuration
    csv_path = "../../data/cleaned/cleaned_taxi_data.csv"
    db_path = "../script/taxi_data.db"

    # Check if CSV exists
    if not os.path.exists(csv_path):
        print(f"\n❌ Error: CSV file not found at {csv_path}")
        print("Please run the data cleaning script first.")
        return

    # Step 1: Create database schema
    create_database_schema(db_path)

    # Step 2: Load data
    load_data_to_database(csv_path, db_path)

    # Step 3: Verify
    verify_database(db_path)

    print("\n" + "=" * 60)
    print("CONVERSION COMPLETE!")
    print("=" * 60)
    print(f"\n✓ Database created: {db_path}")
    

if __name__ == "_main_":
    main()