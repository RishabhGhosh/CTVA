from datetime import date
from fastapi import FastAPI, Query, Depends
from fastapi_pagination import Page, add_pagination
from fastapi_pagination.ext.sqlalchemy import paginate
from models import WeatherRecord, WeatherStats, init_db
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import Optional

app = FastAPI(title="Weather Data API")

# Pydantic models for API response validation and serialization
class WeatherResponse(BaseModel):
    """
    Schema for individual weather record responses.
    Includes daily weather measurements for a specific station.
    """
    station_id: str
    date: date
    max_temp: Optional[float]
    min_temp: Optional[float]
    precipitation: Optional[float]
    
    class Config:
        from_attributes = True

class StatsResponse(BaseModel):
    """
    Schema for yearly weather statistics responses.
    Includes aggregated yearly statistics for a specific station.
    """
    station_id: str
    year: int
    avg_max_temp: Optional[float]
    avg_min_temp: Optional[float]
    total_precipitation: Optional[float]
    
    class Config:
        from_attributes = True

# Initialize database connection
SessionLocal = init_db()

def get_db():
    """
    Creates a new database session for each request and ensures proper cleanup.
    Used as a FastAPI dependency.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/api/weather", response_model=Page[WeatherResponse])
def get_weather(
    db: Session = Depends(get_db),
    station_id: Optional[str] = Query(None, description="Weather station identifier"),
    start_date: Optional[date] = Query(None, description="Start date for weather records"),
    end_date: Optional[date] = Query(None, description="End date for weather records")
):
    """
    Retrieve paginated weather records with optional filtering by station and date range.
    
    Args:
        db: Database session
        station_id: Optional station identifier to filter records
        start_date: Optional start date for filtering records
        end_date: Optional end date for filtering records
    
    Returns:
        Paginated list of weather records
    """
    # Build base query
    query = db.query(WeatherRecord)
    
    # Apply filters if provided
    if station_id:
        query = query.filter(WeatherRecord.station_id == station_id)
    if start_date:
        query = query.filter(WeatherRecord.date >= start_date)
    if end_date:
        query = query.filter(WeatherRecord.date <= end_date)
    
    # Return paginated results ordered by date
    return paginate(query.order_by(WeatherRecord.date))

@app.get("/api/weather/stats", response_model=Page[StatsResponse])
def get_weather_stats(
    db: Session = Depends(get_db),
    station_id: Optional[str] = Query(None, description="Weather station identifier"),
    year: Optional[int] = Query(None, description="Year for weather statistics")
):
    """
    Retrieve paginated yearly weather statistics with optional filtering by station and year.
    
    Args:
        db: Database session
        station_id: Optional station identifier to filter statistics
        year: Optional year to filter statistics
    
    Returns:
        Paginated list of yearly weather statistics
    """
    # Build base query
    query = db.query(WeatherStats)
    
    # Apply filters if provided
    if station_id:
        query = query.filter(WeatherStats.station_id == station_id)
    if year:
        query = query.filter(WeatherStats.year == year)
    
    # Return paginated results ordered by year
    return paginate(query.order_by(WeatherStats.year))

# Enable pagination for the FastAPI application
add_pagination(app)