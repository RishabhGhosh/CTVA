from sqlalchemy import create_engine, Column, Integer, Date, Float, String, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class WeatherRecord(Base):
    """
    Database model for daily weather records.
    
    Attributes:
        id: Primary key
        station_id: Weather station identifier
        date: Date of weather record
        max_temp: Maximum temperature in Celsius
        min_temp: Minimum temperature in Celsius
        precipitation: Precipitation amount in centimeters
    """
    __tablename__ = 'weather_records'
    
    id = Column(Integer, primary_key=True)
    station_id = Column(String, nullable=False)
    date = Column(Date, nullable=False)
    max_temp = Column(Float)
    min_temp = Column(Float)
    precipitation = Column(Float)
    
    # Ensure each station has only one record per date
    __table_args__ = (
        UniqueConstraint('station_id', 'date', name='uix_station_date'),
    )

class WeatherStats(Base):
    """
    Database model for yearly weather statistics.
    
    Attributes:
        id: Primary key
        station_id: Weather station identifier
        year: Year of statistics
        avg_max_temp: Average maximum temperature in Celsius
        avg_min_temp: Average minimum temperature in Celsius
        total_precipitation: Total yearly precipitation in centimeters
    """
    __tablename__ = 'weather_stats'
    
    id = Column(Integer, primary_key=True)
    station_id = Column(String, nullable=False)
    year = Column(Integer, nullable=False)
    avg_max_temp = Column(Float)
    avg_min_temp = Column(Float)
    total_precipitation = Column(Float)
    
    # Ensure each station has only one record per year
    __table_args__ = (
        UniqueConstraint('station_id', 'year', name='uix_station_year'),
    )

def init_db(db_url='sqlite:///weather.db'):
    """
    Initialize the database connection and create tables if they don't exist.
    
    Args:
        db_url: Database connection URL (defaults to SQLite)
    
    Returns:
        SQLAlchemy session factory
    """
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)