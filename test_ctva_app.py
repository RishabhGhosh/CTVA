from api import app, get_db
from datetime import date
from fastapi.testclient import TestClient
from ingest import process_weather_file, calculate_yearly_stats
from models import Base, WeatherRecord, WeatherStats

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

import os
import pytest
import tempfile

# Test database setup
SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Fixtures
@pytest.fixture
def test_db():
    """Create a fresh test database for each test."""
    Base.metadata.create_all(bind=engine)
    db = TestingSessionLocal()
    yield db
    db.close()
    Base.metadata.drop_all(bind=engine)

@pytest.fixture
def client(test_db):
    """Create a test client with a test database session."""
    def override_get_db():
        try:
            yield test_db
        finally:
            test_db.close()
    
    app.dependency_overrides[get_db] = override_get_db
    return TestClient(app)

@pytest.fixture
def sample_weather_data(test_db):
    """Create sample weather records for testing."""
    records = [
        WeatherRecord(
            station_id="TEST001",
            date=date(2020, 1, 1),
            max_temp=20.0,
            min_temp=10.0,
            precipitation=5.0
        ),
        WeatherRecord(
            station_id="TEST001",
            date=date(2020, 1, 2),
            max_temp=22.0,
            min_temp=12.0,
            precipitation=0.0
        ),
        WeatherRecord(
            station_id="TEST002",
            date=date(2020, 1, 1),
            max_temp=25.0,
            min_temp=15.0,
            precipitation=2.5
        )
    ]
    
    test_db.add_all(records)
    test_db.commit()
    return records

@pytest.fixture
def sample_weather_file():
    """Create a temporary weather data file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
        # Write header
        f.write("20200101\t200\t100\t50\n")  # 20.0°C, 10.0°C, 5.0mm
        f.write("20200102\t220\t120\t0\n")   # 22.0°C, 12.0°C, 0.0mm
        f.write("20200103\t-9999\t150\t25\n") # Missing max temp, 15.0°C, 2.5mm
        filename = f.name
    
    yield filename
    os.unlink(filename)

# API Tests
def test_get_weather_no_params(client, sample_weather_data):
    """Test getting weather data without any filters."""
    response = client.get("/api/weather")
    assert response.status_code == 200
    data = response.json()
    assert len(data['items']) == 3
    assert 'total' in data
    assert data['total'] == 3

def test_get_weather_with_station(client, sample_weather_data):
    """Test getting weather data filtered by station."""
    response = client.get("/api/weather?station_id=TEST001")
    assert response.status_code == 200
    data = response.json()
    assert len(data['items']) == 2
    assert all(item['station_id'] == 'TEST001' for item in data['items'])

def test_get_weather_with_date_range(client, sample_weather_data):
    """Test getting weather data filtered by date range."""
    response = client.get(
        "/api/weather?start_date=2020-01-01&end_date=2020-01-01"
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data['items']) == 2
    assert all(item['date'] == '2020-01-01' for item in data['items'])

def test_get_weather_pagination(client, sample_weather_data):
    """Test weather data pagination."""
    response = client.get("/api/weather?size=2")
    assert response.status_code == 200
    data = response.json()
    assert len(data['items']) == 2
    assert data['total'] == 3

def test_get_stats_no_params(client, test_db, sample_weather_data):
    """Test getting weather stats without any filters."""
    # Calculate stats first
    calculate_yearly_stats(test_db)  # Now passing the session, not the fixture function
    test_db.commit()  # Make sure to commit the changes
    
    response = client.get("/api/weather/stats")
    assert response.status_code == 200
    data = response.json()
    assert len(data['items']) == 2  # One stat record per station

def test_get_stats_with_station(client, test_db, sample_weather_data):
    """Test getting weather stats filtered by station."""
    # Calculate stats first
    calculate_yearly_stats(test_db)  # Now passing the session, not the fixture function
    test_db.commit()  # Make sure to commit the changes
    
    response = client.get("/api/weather/stats?station_id=TEST001")
    assert response.status_code == 200
    data = response.json()
    assert len(data['items']) == 1
    assert data['items'][0]['station_id'] == 'TEST001'

def test_get_stats_with_year(client, test_db, sample_weather_data):
    """Test getting weather stats filtered by year."""
    # Calculate stats first
    calculate_yearly_stats(test_db)  # Now passing the session, not the fixture function
    test_db.commit()  # Make sure to commit the changes
    
    response = client.get("/api/weather/stats?year=2020")
    assert response.status_code == 200
    data = response.json()
    assert all(item['year'] == 2020 for item in data['items'])

# Data Ingestion Tests
def test_process_weather_file(test_db, sample_weather_file):
    """Test processing a weather data file."""
    station_id = os.path.basename(sample_weather_file).split('.')[0]
    records_count = process_weather_file(sample_weather_file, test_db)
    
    assert records_count == 3
    
    # Verify records in database
    records = test_db.query(WeatherRecord).all()
    assert len(records) == 3
    
    # Check specific values
    first_record = test_db.query(WeatherRecord).filter_by(date=date(2020, 1, 1)).first()
    assert first_record.max_temp == 20.0
    assert first_record.min_temp == 10.0
    assert first_record.precipitation == 5.0

def test_calculate_yearly_stats(test_db, sample_weather_data):
    """Test calculation of yearly statistics."""
    calculate_yearly_stats(test_db)  # Now passing the session, not the fixture function
    test_db.commit()  # Make sure to commit the changes

    stats = test_db.query(WeatherStats).all()
    assert len(stats) == 2  # One for each station
    
    # Add more detailed assertions
    test001_stats = test_db.query(WeatherStats).filter_by(station_id='TEST001').first()
    assert test001_stats is not None
    assert test001_stats.year == 2020
    assert pytest.approx(test001_stats.avg_max_temp) == 21.0  # (20.0 + 22.0) / 2
    assert pytest.approx(test001_stats.avg_min_temp) == 11.0  # (10.0 + 12.0) / 2
    assert pytest.approx(test001_stats.total_precipitation) == 5.0  # 5.0 + 0.0

def calculate_yearly_stats(session):
    """
    Calculate yearly statistics for all weather stations.
    
    Args:
        session: SQLAlchemy database session
    """
    try:
        # Clear existing statistics
        session.query(WeatherStats).delete()
        
        # Calculate new statistics
        stats = session.query(
            WeatherRecord.station_id,
            func.extract('year', WeatherRecord.date).label('year'),
            func.avg(WeatherRecord.max_temp).label('avg_max_temp'),
            func.avg(WeatherRecord.min_temp).label('avg_min_temp'),
            func.sum(WeatherRecord.precipitation).label('total_precipitation')
        ).group_by(
            WeatherRecord.station_id,
            func.extract('year', WeatherRecord.date)
        ).all()
        
        # Add new statistics
        for stat in stats:
            weather_stat = WeatherStats(
                station_id=stat.station_id,
                year=int(stat.year),
                avg_max_temp=stat.avg_max_temp,
                avg_min_temp=stat.avg_min_temp,
                total_precipitation=stat.total_precipitation
            )
            session.add(weather_stat)
        
        session.commit()
    except Exception as e:
        session.rollback()
        raise

# Error Handling Tests
def test_invalid_date_format(client):
    """Test handling of invalid date format."""
    response = client.get("/api/weather?start_date=invalid-date")
    assert response.status_code == 422  # Validation error

def test_invalid_station_id(client):
    """Test handling of non-existent station ID."""
    response = client.get("/api/weather?station_id=NONEXISTENT")
    assert response.status_code == 200
    data = response.json()
    assert len(data['items']) == 0

def test_invalid_year(client):
    """Test handling of invalid year."""
    response = client.get("/api/weather/stats?year=invalid")
    assert response.status_code == 422  # Validation error

# Data Validation Tests
def test_unique_constraint_station_date(test_db):
    """Test unique constraint for station_id and date combination."""
    record1 = WeatherRecord(
        station_id="TEST003",
        date=date(2020, 1, 1),
        max_temp=20.0
    )
    record2 = WeatherRecord(
        station_id="TEST003",
        date=date(2020, 1, 1),
        max_temp=25.0
    )
    
    test_db.add(record1)
    test_db.commit()
    
    with pytest.raises(Exception):  # SQLAlchemy will raise an integrity error
        test_db.add(record2)
        test_db.commit()

def test_handle_missing_values(test_db, sample_weather_file):
    """Test handling of missing values (-9999) in weather data."""
    process_weather_file(sample_weather_file, test_db)
    
    # Check record with missing max_temp
    record = test_db.query(WeatherRecord).filter_by(date=date(2020, 1, 3)).first()
    assert record.max_temp is None
    assert record.min_temp == 15.0
    assert record.precipitation == 2.5

if __name__ == '__main__':
    pytest.main(['-v'])