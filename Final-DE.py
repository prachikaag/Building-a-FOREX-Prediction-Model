import os
from collections import deque
from datetime import datetime as dt, time, timedelta
from typing import Dict, List, Tuple
import json
import pandas as pd

# import sql alchemy library
import sqlite3
from sqlalchemy import create_engine, Select, Table, Column, Integer, String, MetaData, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from pathlib import Path
from sqlalchemy import func

# Import oandapyv20 library
import oandapyV20.endpoints.orders as orders
import oandapyV20.endpoints.pricing as pricing
from oandapyV20 import API
from oandapyV20.contrib.requests import MarketOrderRequest

# Define Oanda API token and account ID
API_TOKEN = "67af368533fa8517ab86b062f49714d6-fc8a88fd1b8c595746bde3f8c84a57d6"
ACCOUNT_ID = "101-001-25520574-002"
engine = create_engine('sqlite:///mydatabase2.db', echo=True)
metadata = MetaData()
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()

# declare the SQL table schema
class df_long(Base):
    __tablename__ = 'df_long'
    id = Column(Integer, primary_key=True)
    ACCOUNT_ID = Column('account_id',String)
    time = Column('time',DateTime)
    instrument = Column('instrument',String)
    price = Column('price',Float)
    units = Column('units',Float)
    pass

class df_short(Base):
    __tablename__ = 'df_short'
    id = Column(Integer, primary_key=True)
    ACCOUNT_ID = Column('account_id',String)
    time = Column('time',DateTime)
    instrument = Column('instrument',String)
    price = Column('price',Float)
    units = Column('units',Float)
    pass

class df_long_30(Base):
    __tablename__ = 'df_long_30'
    id = Column(Integer, primary_key=True)
    ACCOUNT_ID = Column('account_id',String)
    time = Column('time',DateTime)
    instrument = Column('instrument',String)
    price = Column('price',Float)
    units = Column('units',Float)
    pass

class df_short_30(Base):
    __tablename__ = 'df_short_30'
    id = Column(Integer, primary_key=True)
    ACCOUNT_ID = Column('account_id',String)
    time = Column('time',DateTime)
    instrument = Column('instrument',String)
    price = Column('price',Float)
    units = Column('units',Float)
    pass

class df_long_60(Base):
    __tablename__ = 'df_long_60'
    id = Column(Integer, primary_key=True)
    ACCOUNT_ID = Column('account_id',String)
    time = Column('time',DateTime)
    instrument = Column('instrument',String)
    price = Column('price',Float)
    units = Column('units',Float)
    pass

class df_short_60(Base):
    __tablename__ = 'df_short_60'
    id = Column(Integer, primary_key=True)
    ACCOUNT_ID = Column('account_id',String)
    time = Column('time',DateTime)
    instrument = Column('instrument',String)
    price = Column('price',Float)
    units = Column('units',Float)
    pass

Base.metadata.create_all(engine)

# HW 5 code starts
LONG_CONFIG = {"instrument": "NZD_JPY", "executed": 0, "avg": 0.0}
SHORT_CONFIG = {"instrument": "GBP_JPY", "executed": 0, "avg": 0.0}

TOTAL_UNITS = 100_000
BATCH_DURATION = 6
CARRY_FORWARD_UNITS = [0,0]

WINDOWS = [
    {"start": 2, "end": 3, "duration": 1, "units": 0.2 * TOTAL_UNITS},
    {"start": 3, "end": 4, "duration": 1, "units": 0.3 * TOTAL_UNITS},
    {"start": 4, "end": 5, "duration": 1, "units": 0.2 * TOTAL_UNITS},
    {"start": 5, "end": 6, "duration": 1, "units": 0.3 * TOTAL_UNITS},
]
CURRENT_WINDOW: int = 0

# Function to extract prices of instruments from the Oanda API response
# ASK -> LONG, BID -> SHORT
def extract_price_from_execution(response: Dict, type: int) -> float:
    return response["prices"][0]["closeoutBid" if type else "closeoutAsk"]


# Function to check if a given time is in a specified range
def time_in_range(start: time, end: time, x: time) -> bool:
    if start <= end:
        return start <= x <= end
    else:
        return start <= x or x <= end


def fetch_price_of_instrument(api: API, instrument: str, type: int) -> float:
    q = pricing.PricingInfo(
        accountID=ACCOUNT_ID,
        params={"instruments": instrument},
    )
    res = api.request(q)
    price = extract_price_from_execution(res, type)
    return price


def execute_market_order(api: API, instrument: str, units) -> Dict:
    q = orders.OrderCreate(
        accountID=ACCOUNT_ID,
        data=MarketOrderRequest(instrument=instrument, units=units).data,
    )
    res = api.request(q)
    return res

conn = engine.connect()

df_long_csv = pd.DataFrame(
        columns=["id", "accountID", "time", "instrument", "price", "units"]
        )
df_short_csv = pd.DataFrame(
        columns=["id", "accountID", "time", "instrument", "price", "units"]
        )


# Main function to run the trading algorithm
def main() -> None:
    api = API(access_token=API_TOKEN)
    global CURRENT_WINDOW
    global df_long_csv, df_short_csv

    if CURRENT_WINDOW > 0:
        LONG_CONFIG["avg"] = session.query(func.avg(df_long.price)).scalar()
        SHORT_CONFIG["avg"] = session.query(func.avg(df_short.price)).scalar()

    while CURRENT_WINDOW < len(WINDOWS):
        start_time = time(hour=WINDOWS[CURRENT_WINDOW]["start"])
        end_time = time(hour=WINDOWS[CURRENT_WINDOW]["end"])
        duration = WINDOWS[CURRENT_WINDOW]["duration"]
        units = int(WINDOWS[CURRENT_WINDOW]["units"])

        UNITS_PER_BATCH = [
            units // (duration * 10)
            + (CARRY_FORWARD_UNITS[0] if CURRENT_WINDOW > 1 else 0),
            units // (duration * 10)
            + (CARRY_FORWARD_UNITS[1] if CURRENT_WINDOW > 1 else 0),
        ]

        print(f"[DEBUG] WINDOW: {CURRENT_WINDOW}")
        print(f"[DEBUG] UNITS_PER_BATCH: {UNITS_PER_BATCH}")

        # Wait for the right hour to start the execution
        alert_once_flag = True
        while not time_in_range(start_time, end_time, dt.now().time()):
            if alert_once_flag:
                print("[DEBUG] Will wait for the right hour to start the execution")
                alert_once_flag = False

        # Begin execution
        print("[DEBUG] Starting!")
        
        batch_start_time = dt.now()

        # start trading for each currency pair i.e. instrument
        while time_in_range(start_time, end_time, dt.now().time()):
            # Loop through the instruments and place orders
            for i in range(2):
                # 0 -> LONG
                # 1 -> SHORT
                config = SHORT_CONFIG if i else LONG_CONFIG

                instrument = config["instrument"]
                # -ve -> SHORT
                # +ve -> LONG
                units = UNITS_PER_BATCH[i] * (-1 if i else 1)
                price = fetch_price_of_instrument(api, instrument, i)

                if i:
                    # SHORT BLOCK
                    if CURRENT_WINDOW and float(price) > SHORT_CONFIG["avg"]:
                        print(price,SHORT_CONFIG)
                        print("[DEBUG] Skipping short execution")
                        CARRY_FORWARD_UNITS[1] += units 
                        print(CARRY_FORWARD_UNITS)
                        continue
                    print(
                        f"[INFO] executing {instrument} for {units} units at {dt.now().time()}"
                    )
                    res = execute_market_order(api, instrument, units)
                    SHORT_CONFIG["executed"] += abs(units)
                    # Convert the transaction response to a DataFrame
                    id_n = res['orderCreateTransaction']['id']
                    acid_n = res['orderCreateTransaction']['accountID']
                    time_n = res['orderCreateTransaction']['time']
                    record_data = df_short(id=id_n,ACCOUNT_ID=acid_n,time=dt.now(),instrument=instrument,price=price,units=units )
                    session.add(record_data)
                    session.commit()
                else:
                    # LONG BLOCK
                    print(price)
                    if CURRENT_WINDOW and float(price) < LONG_CONFIG["avg"]:
                        print(price,LONG_CONFIG)
                        print("[DEBUG] Skipping long execution")
                        CARRY_FORWARD_UNITS[1] += units
                        print(CARRY_FORWARD_UNITS)
                        continue
                    print(
                        f"[INFO] executing {instrument} for {units} units at {dt.now().time()}"
                    )
                    res = execute_market_order(api, instrument, units)
    
                    # Convert the transaction response to a DataFrame
                    LONG_CONFIG["executed"] += abs(units)
                    id_n = res['orderCreateTransaction']['id']
                    acid_n = res['orderCreateTransaction']['accountID']
                    time_n = res['orderCreateTransaction']['time']
                    record_data = df_long(id=id_n,ACCOUNT_ID=acid_n,time=dt.now(),instrument=instrument,price=price,units=units )
                    session.add(record_data)
                    session.commit()

            # Wait for 6 minutes before executing the next batch
            while (dt.now() - batch_start_time).seconds < BATCH_DURATION * 60:
                pass
            print("[INFO] 6 min timer elapsed")
            print("[DEBUG] Writing the result to respective csv files")

            # Save the DataFrame to an output CSV file
            df_long_csv.to_csv("output_long.csv", index=False)
            df_short_csv.to_csv("output_short.csv", index=False)
            batch_start_time = dt.now()

        print("[INFO] Window elapsed")
        CURRENT_WINDOW += 1
        LONG_CONFIG["avg"] = session.query(func.avg(df_long.price)).scalar()
        SHORT_CONFIG["avg"] = session.query(func.avg(df_short.price)).scalar()
        print(
            f"""[IMP] {
            [
                CURRENT_WINDOW,
                LONG_CONFIG["executed"],
                CARRY_FORWARD_UNITS[0],
                LONG_CONFIG["avg"],
                SHORT_CONFIG["executed"],
                CARRY_FORWARD_UNITS[1],
                SHORT_CONFIG["avg"],
            ]
        }"""
        )

    print("[DEBUG] Completed!")
    print("[INFO] See the csv files for execution logs")
    print(f"[INFO] Remaining units: {CARRY_FORWARD_UNITS}")

    # HW 5 code ends
    
    min_30_execution_long = False
    min_30_execution_short = False 

    uncondional_units_long = CARRY_FORWARD_UNITS[0]*0.8
    uncondional_units_short = CARRY_FORWARD_UNITS[1]*0.8

    end_time_last = time(hour=WINDOWS[3]["end"])

    # 30 min window
    while (dt.now() - batch_start_time).seconds <  30* 60: 
        print("30 min hault")
        pass
    
    print("[INFO] Starting 30 min execution")
    for j in range(2):
        config = SHORT_CONFIG if j else LONG_CONFIG
        instrument = config["instrument"]
        price = fetch_price_of_instrument(api, instrument, j)

        if j:            
        # SHORT BLOCK
            if float(price) > SHORT_CONFIG["avg"]:
                print(price,SHORT_CONFIG)
                print("[DEBUG] Skipping short execution")
                continue
            else:
                min_30_execution_short = True
                unit_30 = CARRY_FORWARD_UNITS[1]*0.5*(-1)
                print(
                    f"[INFO] executing {instrument} for {unit_30} units at {dt.now().time()}"
                    )
                res = execute_market_order(api, instrument, unit_30)
                SHORT_CONFIG["executed"] += abs(unit_30)
                CARRY_FORWARD_UNITS[1] -= abs(unit_30)
                # Convert the transaction response to a DataFrame
                id_n = res['orderCreateTransaction']['id']
                acid_n = res['orderCreateTransaction']['accountID']
                time_n = res['orderCreateTransaction']['time']
                record_data = df_short_30(id=id_n,ACCOUNT_ID=acid_n,time=dt.now(),instrument=instrument,price=price,units=units_30 )
                session.add(record_data)
                session.commit()

        else:
        # LONG BLOCK
            if CURRENT_WINDOW and float(price) < LONG_CONFIG["avg"]:
                print(price,LONG_CONFIG)
                print("[DEBUG] Skipping long execution")
                continue
            else:
                min_30_execution_long = True
                units_30 = CARRY_FORWARD_UNITS[0]*0.5
                print(
                    f"[INFO] executing {instrument} for {units_30} units at {dt.now().time()}"
                    )
                res = execute_market_order(api, instrument, units_30)
                # Convert the transaction response to a DataFrame
                LONG_CONFIG["executed"] += abs(units_30)
                print(type(CARRY_FORWARD_UNITS), type(units_30))
                CARRY_FORWARD_UNITS[0] -= abs(units_30)
                id_n = res['orderCreateTransaction']['id']
                acid_n = res['orderCreateTransaction']['accountID']
                time_n = res['orderCreateTransaction']['time']
                record_data = df_long_30(id=id_n,ACCOUNT_ID=acid_n,time=dt.now(),instrument=instrument,price=price,units=units_30 )
                session.add(record_data)
                session.commit()

    print("[INFO] 30 min execution completed")

    # #60 min window
    while (dt.now() - batch_start_time).seconds <  30* 60: 
        print("60 min hault")
        pass

    print("[INFO] Starting 60 min execution")
    for k in range(2):
        config = SHORT_CONFIG if k else LONG_CONFIG
        instrument = config["instrument"]
        price = fetch_price_of_instrument(api, instrument, k)
        if k:            
        # SHORT BLOCK
            if min_30_execution_short:
                if float(price) < SHORT_CONFIG["avg"]:
                # sell remaining 50%
                    unit_60 = CARRY_FORWARD_UNITS[1]*0.5*(-1)*0.5
                    print(
                        f"[INFO] executing {instrument} for {unit_60} units at {dt.now().time()}"
                    )
                    res = execute_market_order(api, instrument, unit_60)
                    SHORT_CONFIG["executed"] += abs(unit_60)
                    CARRY_FORWARD_UNITS[1] -= abs(unit_60)
                    # Convert the transaction response to a DataFrame
                    id_n = res['orderCreateTransaction']['id']
                    acid_n = res['orderCreateTransaction']['accountID']
                    time_n = res['orderCreateTransaction']['time']
                    record_data = df_short_60(id=id_n,ACCOUNT_ID=acid_n,time=dt.now(),instrument=instrument,price=price,units=unit_60 )
                    session.add(record_data)
                    session.commit()
                else:
                    # buy remnaing 50%
                    unit_60 = CARRY_FORWARD_UNITS[1]*0.5*0.5
                    print(
                        f"[INFO] executing {instrument} for {unit_60} units at {dt.now().time()}"
                    )
                    res = execute_market_order(api, instrument, unit_60)
                    SHORT_CONFIG["executed"] += abs(unit_60)
                    CARRY_FORWARD_UNITS[1] -= abs(unit_60)
                    # Convert the transaction response to a DataFrame
                    id_n = res['orderCreateTransaction']['id']
                    acid_n = res['orderCreateTransaction']['accountID']
                    time_n = res['orderCreateTransaction']['time']
                    record_data = df_short_60(id=id_n,ACCOUNT_ID=acid_n,time=dt.now(),instrument=instrument,price=price,units=unit_60 )
                    session.add(record_data)
                    session.commit()
            else:
                if float(price) > SHORT_CONFIG["avg"]:
                    while uncondional_units_short:
                        unit_s = 20_000*(-1)
                        res = execute_market_order(api, instrument, -1 * unit_s)
                        id_n = res['orderCreateTransaction']['id']
                        acid_n = res['orderCreateTransaction']['accountID']
                        time_n = res['orderCreateTransaction']['time']
                        record_data = df_short_60(id=id_n,ACCOUNT_ID=acid_n,time=dt.now(),instrument=instrument,price=price,units=unit_s)
                        session.add(record_data)
                        session.commit()
                        uncondional_units_short -= unit_s
                        pass
        else:
            # LONG BLOCK
            if min_30_execution_long:
                # go lond and buy 50%
                unit_60 = CARRY_FORWARD_UNITS[0]*0.5*0.5
                print(
                    f"[INFO] executing {instrument} for {unit_60} units at {dt.now().time()}"
                )
                res = execute_market_order(api, instrument, unit_60)
                LONG_CONFIG["executed"] += abs(unit_60)
                CARRY_FORWARD_UNITS[0] -= abs(unit_60)
                # Convert the transaction response to a DataFrame
                id_n = res['orderCreateTransaction']['id']
                acid_n = res['orderCreateTransaction']['accountID']
                time_n = res['orderCreateTransaction']['time']
                record_data = df_long_60(id=id_n,ACCOUNT_ID=acid_n,time=dt.now(),instrument=instrument,price=price,units=unit_60 )
                session.add(record_data)
                session.commit()
            else:
                if float(price) < LONG_CONFIG["avg"]:
                    while uncondional_units_long:
                        unit_s = 20_000
                        res = execute_market_order(api, instrument, unit_s)
                        id_n = res['orderCreateTransaction']['id']
                        acid_n = res['orderCreateTransaction']['accountID']
                        time_n = res['orderCreateTransaction']['time']
                        record_data = df_long_60(id=id_n,ACCOUNT_ID=acid_n,time=dt.now(),instrument=instrument,price=price,units=unit_s )
                        session.add(record_data)
                        session.commit()
                        uncondional_units_long -= unit_s
                        pass

# Run the main function if this script is run as the main module
if __name__ == "__main__":
    main()
