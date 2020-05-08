#################################################
# Climate App using Flask, sqlite, ORM
#   Author: Sanjive Agarwal
#  Updated: May 2020
#
# Tasks:
# List all routes that are available.
# / (Home page)
# /api/v1.0/precipitation
#   * Convert the query results to a dictionary using date
#       as the key and prcp as the value.
#   * Return the JSON representation of your dictionary.
# /api/v1.0/stations
#   * Return a JSON list of stations from the dataset.
# /api/v1.0/tobs
#   * Query the dates and temperature observations of the most
#       active station for the last year of data.
#   * Return a JSON list of temperature observations (TOBS)
#       for the previous year.
# /api/v1.0/<start> and /api/v1.0/<start>/<end>
#   * Return a JSON list of the minimum temperature, the average temperature,
#       and the max temperature for a given start or start-end range.
#   * When given the start only, calculate TMIN, TAVG, and TMAX for
#       all dates greater than and equal to the start date.
#   * When given the start and the end date, calculate the TMIN, TAVG,
#       and TMAX for dates between the start and end date inclusive.
#
# Hints:
#   * You will need to join the station and measurement tables for some of the queries.
#   * Use Flask jsonify to convert your API data into a valid JSON response object.
#
#################################################
# import the dependencies
import numpy as np
import pandas as pd

import datetime as dt
from dateutil.relativedelta import relativedelta

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, MetaData, Table, Column, ForeignKey, inspect, func

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(engine, reflect=True)

# We can view all of the classes that automap found
Base.classes.keys()

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
@app.route("/")
def Home():
    """
        List all the available routes in the app.
    """
    return (
        f"Welcome to the Hawaii Weather Home page!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/><br/>"
        f"Please enter start date in below api in the format YYYY-MM-DD<br/>"
        f"/api/v1.0/<start><br/><br/>"
        f"Please enter start and end dates in below api in the format YYYY-MM-DD/YYYY-MM-DD<br/>"
        f"/api/v1.0/<start>/<end><br/>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    """
        Return the list of dates and the precipitation observed for the
        specified dates.

       1. Query to list the dates and pcrp observations.
       2. Convert the query results to a Dictionary using date as
          the key and prcp as the value.
       3. Return the JSON representation as dictionary.
    """
    # Create our session (link) from Python to the DB
    session = Session(engine)

    #  Calculate the "max date" in the Measurement Table
    max_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()

    # Calculate the date a year before
    # (the date to be used to get all the records greater than the date)
    previous_year_date = (dt.datetime.strptime(max_date[0], '%Y-%m-%d').date() - relativedelta(years=1)).isoformat()

    #  Perform a query to retrieve the date and precipitation data
    measurement_result = session.query(Measurement.date, Measurement.prcp)            \
                                      .filter(Measurement.date >= previous_year_date) \
                                      .order_by( Measurement.date.desc(), Measurement.station.desc()).all()

    # Close the session after the query
    session.close()

    prcp_list = []
    for date, prcp in measurement_result:
        prcp_dict = {}
        prcp_dict["Date"] = date
        prcp_dict["Precipitation"] = prcp
        prcp_list.append(prcp_dict)

    # Returns the precipitation for past one year from the max date in the dataset
    return jsonify(prcp_list)

@app.route("/api/v1.0/stations")
def stations():
    """
        List of stations from the dataset.

        1. Query the unique list of Stations from the Measurement table and
           join with the Station table to get the station name
        2. Return a JSON list of stations from the dataset.
    """
    # Create our session (link) from Python to the DB
    session = Session(engine)

    #  Perform a query to retrieve the list of stations ordered by Station
    station_result = session.query(Measurement.station, Station.name)             \
                                  .filter(Measurement.station == Station.station) \
                                  .group_by(Measurement.station)                  \
                                  .order_by(Measurement.station.asc()).all()

    # Close the session after the query
    session.close()

    station_list = []
    for station, station_name in station_result:
        station_dict = {}
        station_dict["Station"] = station
        station_dict["Station Name"] = station_name
        station_list.append(station_dict)

    # Returns a list of distinct weather stations (Id and the Name) from the measurement table
    return jsonify(station_list)

@app.route("/api/v1.0/tobs")
def tobs():
    """
        List of Temperature Observations (tobs).

        1. Query the last 12 months of temperature observation (tobs) data for
           the most active station
    """
    # Create our session (link) from Python to the DB
    session = Session(engine)

    #  Calculate the "max date" in the Measurement Table
    max_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()

    # Calculate the date a year before
    # (the date to be used to get all the records greater than the date)
    previous_year_date = (dt.datetime.strptime(max_date[0], '%Y-%m-%d').date() - relativedelta(years=1)).isoformat()

    # Get the most active station (Station with most tobs records is most active)
    most_active_result = session.query(Measurement.station, func.count(Measurement.station)) \
                                      .group_by(Measurement.station)                         \
                                      .order_by(func.count(Measurement.station).desc()).first()

    # Get the tibs for the dates for the station identified above
    tobs_result = session.query(Measurement.station, Measurement.date, Measurement.tobs) \
                               .filter(Measurement.station == most_active_result[0])     \
                               .filter(Measurement.date >= previous_year_date)           \
                               .order_by(Measurement.station.asc(), Measurement.date.asc()).all()

    # Close the session after the query
    session.close()

    tobs_list = []
    for station, date, tobs in tobs_result:
        tobs_dict = {}
        tobs_dict["Station"] = station
        tobs_dict["Date"] = date
        tobs_dict["Tobs"] = tobs
        tobs_list.append(tobs_dict)

    # Return the Temperatures for the most active station for a year.
    return jsonify(tobs_list)

# Date validation function
def validate_date(date_text):
    try:
        if date_text != dt.datetime.strptime(date_text, "%Y-%m-%d").strftime('%Y-%m-%d'):
            raise ValueError
        return True
    except ValueError:
        return False

@app.route("/api/v1.0/<start_dt>")
def start(start_dt):
    """
        * Return a JSON list of the minimum, average and the
          max temperatures for a given start date.
        * When given the start date, calculate `TMIN`, `TAVG`, and `TMAX`
          for all dates greater than and equal to the start date.

        1. Get the date from the URL
        2. Validate the date, Raise and display error to the user and displaying the
           correct format of the input date.
        3. Return the Minimum, Average and Maximum temperatures for the dates greater then the
           user entered date.
    """
    # Validate the start date, raise error for invalid date entry.
    if not validate_date(start_dt):
        return jsonify({"error": f"Please, specify the date in 'YYYY-MM-DD' format: The entered date '{start_dt}' is not valid."}), 404
    start_date = dt.datetime.strptime(start_dt, '%Y-%m-%d')

    # Create our session (link) from Python to the DB
    session = Session(engine)

    min_temp = session.query(func.min(Measurement.tobs)).filter(Measurement.date >= start_date).scalar()
    avg_temp = session.query(func.round(func.avg(Measurement.tobs))).filter(Measurement.date >= start_date).scalar()
    max_temp = session.query(func.max(Measurement.tobs)).filter(Measurement.date >= start_date).scalar()

    # Close the session after the query
    session.close()

    result = [{"Minimum Temp":min_temp}, {"Average Temp":avg_temp}, {"Maximum Temp":max_temp}]

    # Return the Minimum, Average and Maximum temperatures for the dates greater then the user entered date
    return jsonify(result)

@app.route("/api/v1.0/<start_dt>/<end_dt>")
def start_end(start_dt, end_dt):
    """
        * Return a JSON list of the minimum, average and the
          max temperatures for range of start date and end date.
        * When given the start date and end date, calculate `TMIN`, `TAVG`, and `TMAX`
          for all dates in the range of start date and end date.

        1. Get the start date and the end date from the URL.
        2. Validate the dates, raise and display error to the user and displaying the
           correct format of the input dates.
        3. Return the Minimum, Average and Maximum temperatures for the dates in the range
           of user entered start date and end date.
    """
    # Validate the start and end dates and also check that the end date is greater than start date, raise error for invalid date entry.
    if not validate_date(start_dt):
        return jsonify({"error": f"Please, specify the start date in 'YYYY-MM-DD' format: The entered date '{start_dt}' is not valid."}), 404
    if not validate_date(end_dt):
        return jsonify({"error": f"Please, specify the end date in 'YYYY-MM-DD' format: The entered date '{end_dt}' is not valid."}), 404
    if end_dt < start_dt:
        return jsonify({"error": f"Please, specify start date less then end date. The dates entered are '{start_dt}' and '{end_dt}'"}), 404

    start_date = dt.datetime.strptime(start_dt, '%Y-%m-%d')
    end_date = dt.datetime.strptime(end_dt, '%Y-%m-%d')

    # Create our session (link) from Python to the DB
    session = Session(engine)

    min_temp = session.query(func.min(Measurement.tobs)).filter(Measurement.date >= start_date).scalar()
    avg_temp = session.query(func.round(func.avg(Measurement.tobs))).filter(Measurement.date.between(start_date, end_date)).scalar()
    max_temp = session.query(func.max(Measurement.tobs)).filter(Measurement.date >= start_date).scalar()

    # Close the session after the query
    session.close()

    result = [{"Minimum Temp":min_temp}, {"Average Temp":avg_temp}, {"Maximum Temp":max_temp}]

    # Return the Minimum, Average and Maximum temperatures for the specified date range.
    return jsonify(result)

if __name__ == "__main__":
    app.run(debug=True)
