import sqlite3
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

version = "v1"

app = FastAPI(
    title="NYC Taxi Trip Data API",
    description="API for exploring NYC taxi trip data and urban mobility patterns",
    version=version,
)

# origins = ["http://localhost:5173", "http://127.0.0.1:5500/frontend/index.html"]
# origins = ["http://localhost:8000", "http://localhost:8080"]
origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5500",
    "http://localhost:5500",
    "http://127.0.0.1:8080",
    "http://localhost:8080",
    "null"  # For file:// protocol if opening HTML directly
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Database path
DB_PATH = "../backend/script/taxi_data.db"


# Pydantic models for response validation
class TripBase(BaseModel):
    id: str
    vendor_id: int
    pickup_datetime: str
    dropoff_datetime: str
    passenger_count: int
    trip_duration_minutes: float
    trip_distance_miles: float
    avg_speed_mph: float
    time_of_day: Optional[str] = None
    speed_category: Optional[str] = None


class TripDetail(TripBase):
    pickup_longitude: float
    pickup_latitude: float
    dropoff_longitude: float
    dropoff_latitude: float
    pickup_hour: Optional[int] = None
    pickup_day_name: Optional[str] = None
    is_weekend: Optional[int] = None
    is_rush_hour: Optional[int] = None


class StatsResponse(BaseModel):
    total_trips: int
    avg_duration_minutes: float
    avg_distance_miles: float
    avg_speed_mph: float
    avg_passengers: float


# Database helper function
def get_db_connection():
    """Create database connection"""
    # conn = sqlite3.connect(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # Return rows as dictionaries
    return conn


# Root endpoint
@app.get("/")
def root():
    """API root endpoint"""
    return {
        "message": "NYC Taxi Trip Data API",
        "version": version,
        "endpoints": {
            "trips": "/api/trips",
            "trip_detail": "/api/trips/{trip_id}",
            "statistics": "/api/stats",
            "time_patterns": "/api/patterns/time",
            "speed_distribution": "/api/patterns/speed",
            "location_heatmap": "/api/patterns/locations",
        },
    }


# Get all trips with filters
@app.get("/api/trips", response_model=List[TripBase])
def get_trips(
    limit: int = Query(100, ge=1, le=1000, description="Number of trips to return"),
    offset: int = Query(0, ge=0, description="Number of trips to skip"),
    time_of_day: Optional[str] = Query(
        None, description="Filter by time of day: morning, afternoon, evening, night"
    ),
    speed_category: Optional[str] = Query(None, description="Filter by speed category"),
    min_distance: Optional[float] = Query(
        None, ge=0, description="Minimum trip distance in miles"
    ),
    max_distance: Optional[float] = Query(
        None, ge=0, description="Maximum trip distance in miles"
    ),
):
    """Get trips with optional filters"""

    conn = get_db_connection()
    cursor = conn.cursor()

    # Build query with filters
    query = """
        SELECT 
            t.id, t.vendor_id, t.pickup_datetime, t.dropoff_datetime,
            t.passenger_count, t.trip_duration_minutes, t.trip_distance_miles,
            t.avg_speed_mph, tt.time_of_day, tc.speed_category
        FROM trips t
        LEFT JOIN trip_temporal tt ON t.id = tt.trip_id
        LEFT JOIN trip_categories tc ON t.id = tc.trip_id
        WHERE 1=1
    """

    params = []

    if time_of_day:
        query += " AND tt.time_of_day = ?"
        params.append(time_of_day)

    if speed_category:
        query += " AND tc.speed_category = ?"
        params.append(speed_category)

    if min_distance is not None:
        query += " AND t.trip_distance_miles >= ?"
        params.append(min_distance)

    if max_distance is not None:
        query += " AND t.trip_distance_miles <= ?"
        params.append(max_distance)

    query += " LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]


# Get single trip by ID
@app.get("/api/trips/{trip_id}", response_model=TripDetail)
def get_trip_detail(trip_id: str):
    """Get detailed information for a specific trip"""

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT 
            t.*,
            tt.pickup_hour, tt.pickup_day_name, tt.is_weekend,
            tt.time_of_day, tt.is_rush_hour,
            tc.speed_category
        FROM trips t
        LEFT JOIN trip_temporal tt ON t.id = tt.trip_id
        LEFT JOIN trip_categories tc ON t.id = tc.trip_id
        WHERE t.id = ?
    """,
        (trip_id,),
    )

    row = cursor.fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Trip not found")

    return dict(row)


# Get overall statistics
@app.get("/api/stats", response_model=StatsResponse)
def get_statistics():
    """Get overall dataset statistics"""

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT 
            COUNT(*) as total_trips,
            AVG(trip_duration_minutes) as avg_duration_minutes,
            AVG(trip_distance_miles) as avg_distance_miles,
            AVG(avg_speed_mph) as avg_speed_mph,
            AVG(passenger_count) as avg_passengers
        FROM trips
    """
    )

    row = cursor.fetchone()
    conn.close()

    return dict(row)


# Get time-based patterns
@app.get("/api/patterns/time")
def get_time_patterns():
    """Get trip patterns by time of day and day of week"""

    conn = get_db_connection()
    cursor = conn.cursor()

    # Trips by time of day
    cursor.execute(
        """
        SELECT 
            time_of_day,
            COUNT(*) as trip_count,
            AVG(t.avg_speed_mph) as avg_speed,
            AVG(t.trip_duration_minutes) as avg_duration
        FROM trip_temporal tt
        JOIN trips t ON tt.trip_id = t.id
        GROUP BY time_of_day
        ORDER BY trip_count DESC
    """
    )
    time_of_day_data = [dict(row) for row in cursor.fetchall()]

    # Trips by hour
    cursor.execute(
        """
        SELECT 
            pickup_hour,
            COUNT(*) as trip_count,
            AVG(t.avg_speed_mph) as avg_speed
        FROM trip_temporal tt
        JOIN trips t ON tt.trip_id = t.id
        GROUP BY pickup_hour
        ORDER BY pickup_hour
    """
    )
    hourly_data = [dict(row) for row in cursor.fetchall()]

    # Trips by day of week
    cursor.execute(
        """
        SELECT 
            pickup_day_name,
            COUNT(*) as trip_count,
            AVG(t.trip_distance_miles) as avg_distance
        FROM trip_temporal tt
        JOIN trips t ON tt.trip_id = t.id
        GROUP BY pickup_day_name
        ORDER BY trip_count DESC
    """
    )
    daily_data = [dict(row) for row in cursor.fetchall()]

    # Rush hour vs non-rush hour
    cursor.execute(
        """
        SELECT 
            CASE WHEN is_rush_hour = 1 THEN 'Rush Hour' ELSE 'Non-Rush Hour' END as period,
            COUNT(*) as trip_count,
            AVG(t.avg_speed_mph) as avg_speed,
            AVG(t.trip_duration_minutes) as avg_duration
        FROM trip_temporal tt
        JOIN trips t ON tt.trip_id = t.id
        GROUP BY is_rush_hour
    """
    )
    rush_hour_data = [dict(row) for row in cursor.fetchall()]

    conn.close()

    return {
        "by_time_of_day": time_of_day_data,
        "by_hour": hourly_data,
        "by_day_of_week": daily_data,
        "rush_hour_comparison": rush_hour_data,
    }


# Get speed distribution patterns
@app.get("/api/patterns/speed")
def get_speed_patterns():
    """Get speed distribution and patterns"""

    conn = get_db_connection()
    cursor = conn.cursor()

    # Speed category distribution
    cursor.execute(
        """
        SELECT 
            speed_category,
            COUNT(*) as trip_count,
            AVG(t.trip_distance_miles) as avg_distance,
            AVG(t.trip_duration_minutes) as avg_duration
        FROM trip_categories tc
        JOIN trips t ON tc.trip_id = t.id
        GROUP BY speed_category
        ORDER BY trip_count DESC
    """
    )
    speed_distribution = [dict(row) for row in cursor.fetchall()]

    # Speed by time of day
    cursor.execute(
        """
        SELECT 
            tt.time_of_day,
            tc.speed_category,
            COUNT(*) as trip_count
        FROM trip_temporal tt
        JOIN trip_categories tc ON tt.trip_id = tc.trip_id
        GROUP BY tt.time_of_day, tc.speed_category
        ORDER BY tt.time_of_day, trip_count DESC
    """
    )
    speed_by_time = [dict(row) for row in cursor.fetchall()]

    conn.close()

    return {
        "speed_distribution": speed_distribution,
        "speed_by_time_of_day": speed_by_time,
    }


# Get location patterns for heatmap
@app.get("/api/patterns/locations")
def get_location_patterns(
    limit: int = Query(1000, ge=100, le=10000, description="Number of points to return")
):
    """Get pickup/dropoff location data for heatmap visualization"""

    conn = get_db_connection()
    cursor = conn.cursor()

    # Sample pickup locations
    cursor.execute(
        """
        SELECT 
            pickup_latitude as lat,
            pickup_longitude as lng,
            COUNT(*) as trip_count
        FROM trips
        GROUP BY pickup_latitude, pickup_longitude
        ORDER BY trip_count DESC
        LIMIT ?
    """,
        (limit,),
    )
    pickup_locations = [dict(row) for row in cursor.fetchall()]

    # Sample dropoff locations
    cursor.execute(
        """
        SELECT 
            dropoff_latitude as lat,
            dropoff_longitude as lng,
            COUNT(*) as trip_count
        FROM trips
        GROUP BY dropoff_latitude, dropoff_longitude
        ORDER BY trip_count DESC
        LIMIT ?
    """,
        (limit,),
    )
    dropoff_locations = [dict(row) for row in cursor.fetchall()]

    conn.close()

    return {
        "pickup_locations": pickup_locations,
        "dropoff_locations": dropoff_locations,
    }


# Health check endpoint
@app.get("/health")
def health_check():
    """Health check endpoint"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM trips")
        count = cursor.fetchone()[0]
        conn.close()

        return {"status": "healthy", "database": "connected", "total_trips": count}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
