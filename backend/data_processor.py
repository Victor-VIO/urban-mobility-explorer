import pandas as pd
import numpy as np

class TaxiDataProcessor:
    """
    This class cleans messy taxi data and prepares it for the database
    Think of it like a factory that takes dirty data and makes it clean!
    """
    
    def init(self, csv_file):
        """
        Initialize with the path to your CSV file
        
        Args:
            csv_file: Path to CSV (accepts both relative and absolute paths)
                      Examples: 'data/taxi.csv' or '/home/user/data/taxi.csv'
        """
        self.csv_file = csv_file
        self.raw_data = None
        self.clean_data = None
        self.excluded_records = []  # Keep track of bad data we remove
        
    def load_data(self):
        """Step 1: Load the CSV file into memory"""
        print("Loading data from CSV...")
        self.raw_data = pd.read_csv(self.csv_file)
        print(f"Loaded {len(self.raw_data)} records")
        return self.raw_data
    
    def clean_data(self):
        """Step 2: Clean the messy data"""
        print("\nStarting data cleaning process...")
        
        # Make a copy so we don't mess up the original
        df = self.raw_data.copy()
        initial_count = len(df)
        
        # CLEANING RULE 1: Remove rows with missing important values
        print("Removing records with missing values...")
        before = len(df)
        df = df.dropna(subset=['pickup_datetime', 'dropoff_datetime', 
                               'pickup_longitude', 'pickup_latitude',
                               'dropoff_longitude', 'dropoff_latitude',
                               'trip_duration'])
        removed = before - len(df)
        print(f"  Removed {removed} records with missing values")
        
        # CLEANING RULE 2: Remove duplicate trips (exact same data)
        print("Removing duplicate records...")
        before = len(df)
        df = df.drop_duplicates()
        removed = before - len(df)
        print(f"  Removed {removed} duplicate records")
        
        # CLEANING RULE 3: Remove trips with invalid coordinates (outside NYC)
        print("Removing trips with invalid coordinates...")
        before = len(df)
        # NYC is roughly between these coordinates
        df = df[
            (df['pickup_latitude'] >= 40.5) & (df['pickup_latitude'] <= 41.0) &
            (df['pickup_longitude'] >= -74.3) & (df['pickup_longitude'] <= -73.7) &
            (df['dropoff_latitude'] >= 40.5) & (df['dropoff_latitude'] <= 41.0) &
            (df['dropoff_longitude'] >= -74.3) & (df['dropoff_longitude'] <= -73.7)
        ]
        removed = before - len(df)
        print(f"  Removed {removed} records with invalid coordinates")
        
        # CLEANING RULE 4: Remove trips with unrealistic duration
        print("Removing trips with unrealistic duration...")
        before = len(df)
        # Trip should be between 1 minute and 4 hours (in seconds)
        df = df[(df['trip_duration'] >= 60) & (df['trip_duration'] <= 14400)]
        removed = before - len(df)
        print(f"  Removed {removed} records with unrealistic duration")
        
        # CLEANING RULE 5: Remove outliers in passenger count
        print("Fixing passenger count...")
        before = len(df)
        df = df[(df['passenger_count'] >= 1) & (df['passenger_count'] <= 6)]
        removed = before - len(df)
        print(f"  Removed {removed} records with invalid passenger count")
        
        self.clean_data = df
        final_count = len(df)
        total_removed = initial_count - final_count
        
        print(f"\nCleaning complete!")
        print(f"  Started with: {initial_count} records")
        print(f"  Ended with: {final_count} records")
        print(f"  Total removed: {total_removed} records ({(total_removed/initial_count)*100:.2f}%)")
        return self.clean_data
    
    def create_derived_features(self):
        """
        Step 3: Create NEW useful information from existing data
        These are called 'derived features' - like calculating speed from distance and time
        """
        print("\nCreating derived features...")
        df = self.clean_data.copy()
        
        # DERIVED FEATURE 1: Trip Speed (distance / time)
        # This tells us how fast the taxi was going
        print("  Calculating trip speed...")
        # Calculate distance using Haversine formula (distance between two GPS points)
        df['trip_distance_km'] = self._calculate_distance(
            df['pickup_latitude'], df['pickup_longitude'],
            df['dropoff_latitude'], df['dropoff_longitude']
        )
        # Speed = distance / time (convert duration from seconds to hours)
        df['avg_speed_kmh'] = df['trip_distance_km'] / (df['trip_duration'] / 3600)
        
        # DERIVED FEATURE 2: Time of day categories
        print("  Extracting time features...")
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
        df['pickup_hour'] = df['pickup_datetime'].dt.hour
        df['pickup_day_of_week'] = df['pickup_datetime'].dt.dayofweek  # 0=Monday, 6=Sunday
        
        # Create time period categories
        df['time_of_day'] = pd.cut(df['pickup_hour'], 
                                    bins=[0, 6, 12, 18, 24],
                                    labels=['Night', 'Morning', 'Afternoon', 'Evening'],
                                    include_lowest=True)
        
        # DERIVED FEATURE 3: Is this a weekend trip?
        df['is_weekend'] = (df['pickup_day_of_week'] >= 5).astype(int)
        
        # DERIVED FEATURE 4: Rush hour indicator
        df['is_rush_hour'] = (
            ((df['pickup_hour'] >= 7) & (df['pickup_hour'] <= 9)) |  # Morning rush
            ((df['pickup_hour'] >= 17) & (df['pickup_hour'] <= 19))   # Evening rush
        ).astype(int)
        
        self.clean_data = df
        print(f"  Created {5} new derived features")
        
        return self.clean_data
    
    def _calculate_distance(self, lat1, lon1, lat2, lon2):
        """
        Calculate the distance between two GPS coordinates using Haversine formula
        Returns distance in kilometers
        """
        # Earth's radius in kilometers
        R = 6371
        
        # Convert degrees to radians
        lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
        
        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = np.sin(dlat/2)*2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)*2
        c = 2 * np.arcsin(np.sqrt(a))
        distance = R * c
        
        return distance
    
    def save_clean_data(self, output_file):
        """
        Step 4: Save the cleaned data to a new CSV file
        
        Args:
            output_file: Path where to save the cleaned CSV (relative or absolute)
                        Example: 'cleaned_taxi_data.csv' or '/output/clean_data.csv'
        """
        if self.clean_data is None:
            raise ValueError("No cleaned data to save! Run clean_data() first.")
        
        print(f"\nSaving cleaned data to '{output_file}'...")
        self.clean_data.to_csv(output_file, index=False)
        print(f"Successfully saved {len(self.clean_data)} records to {output_file}")
        
        return output_file
    
    def get_summary_stats(self):
        """Get a summary of the cleaned data"""
        if self.clean_data is None:
            raise ValueError("No cleaned data available! Run clean_data() first.")
        
        print("\n" + "="*60)
        print("SUMMARY STATISTICS")
        print("="*60)
        
        stats = {'Total Records': len(self.clean_data),
            'Avg Trip Duration (min)': self.clean_data['trip_duration'].mean() / 60,
            'Avg Trip Distance (km)': self.clean_data['trip_distance_km'].mean(),
            'Avg Speed (km/h)': self.clean_data['avg_speed_kmh'].mean(),
            'Avg Passenger Count': self.clean_data['passenger_count'].mean(),
            'Weekend Trips (%)': (self.clean_data['is_weekend'].sum() / len(self.clean_data)) * 100,
            'Rush Hour Trips (%)': (self.clean_data['is_rush_hour'].sum() / len(self.clean_data)) * 100
        }
        
        for key, value in stats.items():
            print(f"{key}: {value:.2f}")
        
        print("="*60)
        
        return stats
    
    def process_all(self, output_file='cleaned_taxi_data.csv'):
        """
        Convenience method: Run the entire pipeline at once!
        
        Args:
            output_file: Where to save the cleaned data (default: 'cleaned_taxi_data.csv')
        
        Returns:
            Path to the output file
        """
        print("="*60)
        print("STARTING TAXI DATA PROCESSING PIPELINE")
        print("="*60)
        
        # Step 1: Load
        self.load_data()
        
        # Step 2: Clean
        self.clean_data()
        
        # Step 3: Create features
        self.create_derived_features()
        
        # Step 4: Save
        self.save_clean_data(output_file)
        
        # Step 5: Show summary
        self.get_summary_stats()
        
        print("\n" + "="*60)
        print("PIPELINE COMPLETE!")
        print("="*60)
        
        return output_file

if __name__ == "__main__":
    processor = TaxiDataProcessor('data/train.csv')
    
    processor.process_all(output_file='data/cleaned_train.csv')