# Import the dependencies.
import numpy as np
import pandas as pd
import datetime as dt

# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify, request
import json


#################################################
# Database Setup
#################################################

# Create engine using the `hawaii.sqlite` database file
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# Declare a Base using `automap_base()`
Base = automap_base()

# Use the Base class to reflect the database tables
Base.prepare(engine)

# Assign the measurement class to a variable called `Measurement` and
# the station class to a variable called `Station`
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create a session
session = Session(engine)

# calculate most recent date
most_recent_date = session.query(Measurement.date).order_by(sqlalchemy.desc(Measurement.date)).limit(1).all()[0].date
# Calculate the date one year from the last date in data set.
most_recent_date = dt.datetime.strptime(most_recent_date, "%Y-%m-%d")
date_one_year_ago = most_recent_date - dt.timedelta(days=365)
date_one_year_ago = date_one_year_ago.date()

# Find most active station count
station_count = session.query(Measurement.station, func.count(Measurement.station)).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).all()
most_active_station = station_count[0][0]

def cal_temp_data(start, end = None):
    temp = []

    start_date = dt.datetime.strptime(start, "%Y-%m-%d")
    end_date = most_recent_date
    if end:
        end_date = dt.datetime.strptime(end, "%Y-%m-%d")

    lowest_temp = session.query(func.min(Measurement.tobs)).filter(Measurement.station == most_active_station).filter(Measurement.date.between(start_date, end_date)).scalar()
    highest_temp = session.query(func.max(Measurement.tobs)).filter(Measurement.station == most_active_station).filter(Measurement.date.between(start_date, end_date)).scalar()
    average_temp = session.query(func.avg(Measurement.tobs)).filter(Measurement.station == most_active_station).filter(Measurement.date.between(start_date, end_date)).scalar()

    temp.append((lowest_temp, highest_temp, average_temp))

    return temp

#################################################
# Flask Setup
#################################################
app = Flask(__name__)



#################################################
# Flask Routes
#################################################

# Home page route
@app.route('/')
def home():

    base_url = request.url_root

    routes_dict = {
        "routes": [
            base_url + 'precipitation',
            base_url + 'stations',
            base_url + 'tobs',
            base_url + '<start>',
            base_url + '<start>/<end>'
        ]
    }

    return jsonify(routes_dict)


# Route precipitation
@app.route('/precipitation')
def precipitation():

    # Perform a query to retrieve the data and precipitation scores
    precipitation_data = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date.between(date_one_year_ago, most_recent_date)).all()

    # Save the query results as a Pandas DataFrame. Explicitly set the column names
    precipitation_df = pd.DataFrame(precipitation_data)

    # Sort the dataframe by date
    precipitation_df = precipitation_df.sort_values(by='date')
    precipitation_df = precipitation_df.dropna()

    prcp_dict = precipitation_df.set_index('date').to_dict()

    return jsonify(prcp_dict)

# Route stations
@app.route('/stations')
def stations():

    stations_list = session.query(func.distinct(Measurement.station)).all()
    stations_list = [tuple(row) for row in stations_list]
    stations_dict = {
        "stations": stations_list
    }
    return jsonify(stations_dict)

@app.route('/tobs')
def tobs():

    temp_data = session.query(Measurement.date, Measurement.tobs).filter(Measurement.station == most_active_station).filter(Measurement.date.between(date_one_year_ago, most_recent_date)).all()

    # Save the query results as a Pandas DataFrame. Explicitly set the column names
    temp_df = pd.DataFrame(temp_data)

    # Sort the dataframe by date
    temp_df = temp_df.sort_values(by='date')

    tobs_dict = temp_df.set_index('date').to_dict()

    return jsonify(tobs_dict)

@app.route('/<start>')
def start(start):

    temp_data = cal_temp_data(start)

    return jsonify(temp_data)

@app.route('/<start>/<end>')
def start_end(start, end):

    temp_data = cal_temp_data(start, end=end)

    return jsonify(temp_data)

if __name__ == '__main__':
    app.run(debug=True)