# NYC Taxi Trip Data - Cleaning Documentation

## Executive Summary

This document describes the data cleaning process applied to the New York City Taxi Trip dataset. We processed *1,458,644 raw trip records* and produced a clean dataset of *1,381,701 records* (94.73% retention rate) with *22 features* (11 original + 11 derived). The cleaning process removed invalid records, handled outliers, and created meaningful derived features for urban mobility analysis.

---

## 1. Dataset Overview

### Source Data
- *Dataset:* NYC Taxi Trip Records (train.csv)
- *Original Size:* 1,458,644 rows × 11 columns
- *File Size:* 417.32 MB
- *Time Period:* January 2016 - June 2016

### Original Features
1. id - Unique trip identifier
2. vendor_id - Taxi vendor (1 or 2)
3. pickup_datetime - Trip start timestamp
4. dropoff_datetime - Trip end timestamp
5. passenger_count - Number of passengers
6. pickup_longitude - Pickup location longitude
7. pickup_latitude - Pickup location latitude
8. dropoff_longitude - Dropoff location longitude
9. dropoff_latitude - Dropoff location latitude
10. store_and_fwd_flag - Whether trip data was stored locally before transmission
11. trip_duration - Trip duration in seconds

---

## 2. Data Quality Issues Identified

### Missing Values
- *Result:* No missing values detected in any column
- *Impact:* No data loss from missing values

### Duplicate Records
- *Result:* No duplicate records found
- *Impact:* No data loss from duplicates

### Invalid Trip Durations
- *Very short trips (<10 seconds):* 1,984 records (0.14%)
  - Likely meter errors or immediate cancellations
- *Very long trips (>24 hours):* 4 records (0.0003%)
  - Probable system errors where meter wasn't properly stopped
- *Statistical outliers:* 73,922 records (5.1%)
  - Trips outside normal duration range using IQR method

### Invalid Passenger Counts
- *Zero passengers:* 60 records
- *More than 6 passengers:* 5 records
- *Total invalid:* 65 records (0.004%)
- *Issue:* NYC taxis cannot operate with 0 passengers or exceed 6-passenger capacity

### Geographic Data Issues
- *Invalid pickup coordinates:* 223 records (0.015%)
- *Invalid dropoff coordinates:* 742 records (0.051%)
- *Issue:* Coordinates outside NYC boundaries (some as far as California)
  - Latitude range found: 32.18°N to 51.88°N (NYC should be 40.5-41.0°N)
  - Longitude range found: -121.93°W to -61.34°W (NYC should be -74.3 to -73.7°W)
- *Cause:* GPS malfunction, data entry errors, or collection system errors

### Unrealistic Speeds
- *Records removed:* 27 trips with speeds >100 mph
- *Issue:* Physically impossible in NYC urban environment with traffic

---

## 3. Data Cleaning Strategy

### Step 1: Handle Missing Values

Action: Check all critical columns for missing values
Result: No missing values found
Rows Removed: 0


### Step 2: Remove Duplicates

Action: Identify and remove duplicate trip records
Result: No duplicates found
Rows Removed: 0


### Step 3: Remove Invalid Values

Actions:
- Remove trips with duration < 10 seconds
- Remove trips with duration > 24 hours (86,400 seconds)
- Remove trips with passenger_count < 1 or > 6
- Remove trips with coordinates outside NYC bounds
  - Valid latitude: 40.5°N to 41.0°N
  - Valid longitude: -74.3°W to -73.7°W

Results:
- Trip duration < 10 sec: 1,984 removed
- Trip duration > 24 hours: 4 removed
- Invalid passenger counts: 41 removed
- Invalid pickup coordinates: 223 removed
- Invalid dropoff coordinates: 742 removed

Total Rows Removed: 2,994 (0.21%)
Remaining Rows: 1,455,650


### Step 4: Remove Statistical Outliers

Method: Interquartile Range (IQR) with 1.5 × IQR bounds
Formula:
- Q1 = 25th percentile = 386 seconds
- Q3 = 75th percentile = 991 seconds
- IQR = Q3 - Q1 = 605 seconds
- Lower bound = Q1 - 1.5 × IQR = 0 seconds (floor applied)
- Upper bound = Q3 + 1.5 × IQR = 2,090 seconds (34.8 minutes)

Results:
- Outliers removed: 73,922 (5.1%)
- Valid trip duration range: 10 to 2,090 seconds

Remaining Rows: 1,381,728


### Step 5: Remove Unrealistic Speeds

Action: Remove trips with calculated speed > 100 mph
Justification: NYC traffic makes speeds over 100 mph impossible
Results:
- Rows removed: 27
- Valid speed range: 0 to 99.9 mph

Final Rows: 1,381,701


---

## 4. Derived Features Created

We created *11 new features* to enhance analytical capabilities:

### Temporal Features

1. *trip_duration_minutes*
   - Conversion of trip_duration from seconds to minutes
   - Formula: trip_duration / 60
   - Use case: More intuitive time representation

2. *pickup_hour*
   - Hour of day (0-23) when trip started
   - Extracted from: pickup_datetime
   - Use case: Analyze hourly demand patterns

3. *pickup_day_of_week*
   - Day of week (0=Monday, 6=Sunday)
   - Extracted from: pickup_datetime
   - Use case: Weekly pattern analysis

4. *pickup_day_name*
   - Day name (Monday, Tuesday, etc.)
   - Extracted from: pickup_datetime
   - Use case: Human-readable day analysis

5. *pickup_month*
   - Month (1-12) when trip occurred
   - Extracted from: pickup_datetime
   - Use case: Seasonal trend analysis

6. *is_weekend*
   - Binary flag (0=weekday, 1=weekend)
   - Formula: 1 if pickup_day_of_week in [5,6] else 0
   - Use case: Weekend vs weekday comparison

7. *time_of_day*
   - Categorical time period
   - Categories:
     - Morning: 6:00 AM - 11:59 AM
     - Afternoon: 12:00 PM - 4:59 PM
     - Evening: 5:00 PM - 8:59 PM
     - Night: 9:00 PM - 5:59 AM
   - Use case: Time-of-day demand analysis

8. *is_rush_hour*
   - Binary flag for peak traffic hours
   - Rush hours: 7-8 AM, 5-6 PM
   - Formula: 1 if pickup_hour in [7,8,17,18] else 0
   - Use case: Rush hour impact analysis

### Geographic and Performance Features

9. *trip_distance_miles*
   - Straight-line distance between pickup and dropoff
   - Method: Haversine formula
   - Formula: 
     
     a = sin²(Δlat/2) + cos(lat1) × cos(lat2) × sin²(Δlon/2)
     c = 2 × arcsin(√a)
     distance = R × c  (where R = 3,959 miles, Earth's radius)
     
   - Use case: Distance-based analysis and fare estimation

10. *avg_speed_mph*
    - Average trip speed in miles per hour
    - Formula: trip_distance_miles / (trip_duration / 3600)
    - Use case: Traffic congestion analysis, route efficiency

11. *speed_category*
    - Categorical speed classification
    - Categories:
      - Very Slow: < 5 mph (18% of trips)
      - Slow: 5-15 mph (72% of trips)
      - Moderate: 15-25 mph (9% of trips)
      - Fast: 25-40 mph (1% of trips)
      - Very Fast: > 40 mph (<0.1% of trips)
    - Use case: Quick speed-based filtering and analysis

---

## 5. Final Dataset Characteristics

### Dataset Dimensions
- *Rows:* 1,381,701 trips
- *Columns:* 22 features
- *Retention Rate:* 94.73%
- *Total Removed:* 76,943 records

### Data Quality Metrics
- ✅ No missing values
- ✅ No duplicate records
- ✅ All passenger counts valid (1-6)
- ✅ All coordinates within NYC bounds
- ✅ All trip durations reasonable (10-2,090 seconds)
- ✅ All speeds realistic (0-100 mph)

### Summary Statistics

| Metric | Mean | Median | Std Dev | Min | Max |
|--------|------|--------|---------|-----|-----|
| Trip Duration (min) | 12.21 | 10.53 | 7.45 | 0.17 | 34.83 |
| Trip Distance (miles) | 1.81 | 1.24 | 1.78 | 0.00 | 21.30 |
| Average Speed (mph) | 8.85 | 7.87 | 4.77 | 0.00 | 99.90 |
| Passenger Count | 1.66 | 1.00 | 1.31 | 1 | 6 |

### Distribution Insights

*Time of Day:*
- Night (9PM-6AM): 393,375 trips (28.5%)
- Morning (6AM-12PM): 339,554 trips (24.6%)
- Afternoon (12PM-5PM): 325,453 trips (23.6%)
- Evening (5PM-9PM): 323,319 trips (23.4%)

*Day of Week:*
- Saturday: 212,719 trips (15.4%)
- Friday: 210,585 trips (15.2%)
- Thursday: 204,224 trips (14.8%)
- Wednesday: 197,480 trips (14.3%)
- Tuesday: 191,570 trips (13.9%)
- Sunday: 187,335 trips (13.6%)
- Monday: 177,788 trips (12.9%)

*Speed Categories:*
- Very Slow (<5 mph): 252,195 trips (18.3%)
- Slow (5-15 mph): 995,757 trips (72.1%)
- Moderate (15-25 mph): 118,751 trips (8.6%)
- Fast (25-40 mph): 14,739 trips (1.1%)
- Very Fast (>40 mph): 259 trips (0.02%)

---

## 6. Key Assumptions and Justifications

### Geographic Boundaries
*Assumption:* Valid NYC taxi trips occur within latitude 40.5-41.0°N and longitude -74.3 to -73.7°W

*Justification:*
- These bounds cover all five NYC boroughs plus immediate surroundings
- Coordinates outside this range indicate GPS errors or misclassified data
- Found coordinates as far as California, confirming data quality issues

### Trip Duration Bounds
*Assumption:* Valid trips last between 10 seconds and 24 hours

*Justification:*
- Trips under 10 seconds are physically impossible (meter errors/cancellations)
- Trips over 24 hours likely indicate forgotten meter stops
- 99.7% of trips fall within these bounds

### Passenger Capacity
*Assumption:* NYC taxis carry 1-6 passengers

*Justification:*
- Standard NYC yellow cabs seat maximum 5 passengers
- Some larger vehicles accommodate up to 6
- Zero passengers indicates data error (taxi can't operate empty for revenue)

### Outlier Detection Method
*Assumption:* Use IQR method with 1.5 × IQR multiplier for trip duration

*Justification:*
- Industry-standard statistical method
- Conservative approach (1.5 × IQR vs. 3.0 × IQR)
- Balances data retention with quality improvement
- Removed only 5.1% as outliers, keeping legitimate long trips

### Speed Threshold
*Assumption:* Maximum realistic speed is 100 mph

*Justification:*
- NYC speed limits: 25-30 mph on most roads
- Even on highways, traffic congestion limits speeds
- Average observed speed was 8.85 mph, confirming heavy traffic
- Speeds over 100 mph indicate calculation errors

---

## 7. Unexpected Observations

### Finding: Extremely Low Average Speeds

*Observation:*
The average taxi speed across all trips was only *8.85 mph*, despite NYC having speed limits of 25-30 mph.

*Analysis:*
- 72% of trips were in "slow" category (5-15 mph)
- 18% of trips averaged under 5 mph (very slow)
- Only 1% of trips exceeded 25 mph

*Impact on Design:*
This finding influenced our decision to create temporal features (time_of_day, is_rush_hour) because:
1. Traffic congestion is the dominant factor in NYC taxi operations
2. Speed patterns likely vary dramatically by time of day
3. Rush hour vs. off-peak analysis became a key analytical dimension
4. Understanding when and where congestion occurs is critical for urban mobility insights

*Business Implications:*
- Drivers should expect very slow speeds in most conditions
- Time-based pricing may be more fair than distance-based
- Route optimization must prioritize avoiding congestion, not just shortest distance

---

## 8. Cleaning Process Transparency

### Records Excluded by Reason

| Exclusion Reason | Count | Percentage |
|------------------|-------|------------|
| Trip duration < 10 seconds | 1,984 | 0.14% |
| Trip duration > 24 hours | 4 | 0.0003% |
| Invalid passenger count | 41 | 0.003% |
| Invalid pickup coordinates | 223 | 0.015% |
| Invalid dropoff coordinates | 742 | 0.051% |
| Statistical outliers (IQR) | 73,922 | 5.07% |
| Unrealistic speed | 27 | 0.002% |
| *Total Excluded* | *76,943* | *5.28%* |

### Data Retention by Stage

| Cleaning Stage | Rows Remaining | Retention Rate |
|----------------|----------------|----------------|
| Initial Dataset | 1,458,644 | 100.00% |
| After Missing Value Check | 1,458,644 | 100.00% |
| After Duplicate Removal | 1,458,644 | 100.00% |
| After Invalid Value Removal | 1,455,650 | 99.79% |
| After Outlier Removal | 1,381,728 | 94.73% |
| After Speed Validation | 1,381,701 | 94.72% |

---

## 9. Technical Implementation

### Tools and Libraries
- *Python 3.x*
- *pandas 2.x* - Data manipulation and analysis
- *numpy* - Numerical computations
- *datetime* - Timestamp processing

### Code Structure
```

```

### Reproducibility
All cleaning steps are:
- ✅ Documented with clear justifications
- ✅ Logged with specific counts
- ✅ Reproducible from the provided script
- ✅ Version-controlled with timestamp

### Running the Cleaning Script
bash
# Change to working directory
cd backend

# Create virtual env on windows
py -m venv env

# Activate virtual env
.\env\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run cleaning
python data_cleaning.py

# Verify results
python verify_cleaned.py

# Run fastapi
fastapi dev main.py

# Visit this url
http://127.0.0.1:8000/docs

# Issues with port, can change port with this command(7000, 9000, etc, replace with any)
fastapi dev main.py --port 7000

# To load the csv data to db
cd script
py csv_to_sqlite.py


---

## 10. Data Quality Validation

### Post-Cleaning Validation Results

*Coordinate Validation:*
- Pickup latitude range: 40.506°N to 40.998°N ✅
- Pickup longitude range: -74.295°W to -73.704°W ✅
- Dropoff latitude range: 40.509°N to 40.998°N ✅
- Dropoff longitude range: -74.295°W to -73.700°W ✅

*Value Range Validation:*
- Passenger count: 1 to 6 ✅
- Trip duration: 10 to 2,090 seconds ✅
- Trip distance: 0.00 to 21.30 miles ✅
- Average speed: 0.00 to 99.90 mph ✅

*Completeness Check:*
- Missing values: 0 across all columns ✅
- Duplicate records: 0 ✅

---

## 12. Conclusion

The data cleaning process successfully transformed a raw dataset of 1.46 million NYC taxi trips into a high-quality analytical dataset of 1.38 million trips. With a 94.73% retention rate, we removed only problematic records while preserving the vast majority of valid data.

The cleaned dataset now includes:
- ✅ Zero missing values
- ✅ No duplicates
- ✅ Valid geographic coordinates
- ✅ Reasonable trip durations
- ✅ Realistic speeds
- ✅ 11 meaningful derived features


The most significant insight from cleaning was the discovery of extremely low average speeds (8.85 mph), revealing that traffic congestion is the dominant factor in NYC taxi operations. This finding directly influenced our feature engineering decisions and will guide subsequent analysis phases.

---

## Appendix: Cleaning Log Summary

*Output Files:*
- data/cleaned/cleaned_taxi_data.csv (1,381,701 rows × 22 columns)
- backend/logs/cleaning_log.txt (detailed step-by-step log)

---