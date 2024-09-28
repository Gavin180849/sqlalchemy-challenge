# Import the dependencies.
import numpy as np
import pandas as pd
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy import func

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

most_recent_date = session.query(func.max(Measurement.date)).scalar()
most_recent_date

# Calculate the date one year from the last date in data set.
one_year_ago = (pd.to_datetime(most_recent_date) - pd.DateOffset(years=1)).strftime('%Y-%m-%d')
    
#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    """List all available api routes"""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0//startdate<start><br/>"
        f"/api/v1.0/startendroute<start>/<end>"

    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    #Create our session (Link) from Python to the DB
    session = Session(engine)
    """Covert the query results from precipitation analysis to a dictionary using date as the key and prcp as the value"""
    
    # Perform a query to retrieve the data and precipitation scores
    P_analysis = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= one_year_ago).\
    filter(Measurement.date <= most_recent_date).all()

    # Save the query results as a Pandas DataFrame. Explicitly set the column names
    P_data_df = pd.DataFrame(P_analysis, columns=['date', 'prcp']).set_index('date')
    # Rename the column 
    P_data_df = P_data_df.rename(columns={'prcp': 'Precipitation'})

    # Sort the dataframe by date
    P_data_df = P_data_df.sort_index().dropna()

    # Check for duplicate indices and aggregate if necessary
    if P_data_df.index.duplicated().any():
        P_data_df = P_data_df.groupby(P_data_df.index).mean()  # Aggregate by mean if duplicates exist

    # Convert DataFrame to JSON
    precipitation_json = P_data_df.to_json(orient='index')
    session.close() 

    return jsonify(precipitation_json)

@app.route("/api/v1.0/stations")
def stations():
    """Return a JSON list of stations from the dataset"""
    Active_stations = session.query(Measurement.station, func.count(Measurement.station)).\
        group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).all()
    
    # Convert the result to a list of station names
    stations_list = [station for station, count in Active_stations]
    session.close()

    return jsonify(stations_list)

@app.route("/api/v1.0/tobs")
def tobs():
    """Query the dates and temperature observations of the most-active station for the previous year of data"""
  
    results = session.query(Measurement.date, Measurement.tobs).filter(Measurement.station == 'USC00519281', Measurement.date >= one_year_ago).all()

    # Convert the result to a list 
    temp_observ = []
    for date, tobs in results:
        temp_observ.append({"date": date, "temperature": tobs})
        
    session.close()

    print(temp_observ)  # Debugging line to check the output

    return jsonify(temp_observ)                                    

"""Returns a JSON list of the minimum temperature, the average temperature, and the maximum temperature for a specified start date"""
@app.route("/api/v1.0/startdate<start>")
def startdate(start):
    """For a spefied start, calculate TMIN, TAVG, and TMAX for all the dates greater than or equal to the start date"""
    date = dt.datetime.strptime(start, '%Y-%m-%d')
    temp_results_start = session.query(func.min(Measurement.tobs), 
                                       func.max(Measurement.tobs), 
                                       func.avg(Measurement.tobs)).filter(Measurement.date >= date).all()
        # Create a dictionary to hold the results
    temperature_data = {
        "TMIN": temp_results_start[0][0],
        "TAVG": temp_results_start[0][1],
        "TMAX": temp_results_start[0][2]
    }

    return jsonify(temperature_data)

@app.route("/api/v1.0/startendroute<start>/<end>")
def startendroute(start,end):
    """For a specified start date and end date, calculate TMIN, TAVG, and TMAX for the dates from the start date to the end date, inclusive"""
    start_date = dt.datetime.strptime(start, '%Y-%m-%d')
    end_date = dt.datetime.strptime(end, '%Y-%m-%d')

    # Query to get the min, max, and average temperatures for the specified date range
    temp_results_range = session.query(
        func.min(Measurement.tobs), 
        func.max(Measurement.tobs), 
        func.avg(Measurement.tobs)
    ).filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()
    
    # Create a dictionary to hold the results
    temperature_data = {
        "TMIN": temp_results_range[0][0],
        "TAVG": temp_results_range[0][1],
        "TMAX": temp_results_range[0][2]
    }

    return jsonify(temperature_data)

if __name__ == '__main__':
    app.run(debug=True)