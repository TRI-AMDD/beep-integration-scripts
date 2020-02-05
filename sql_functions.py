# Copyright 2018 Toyota Research Institute. All rights reserved.
import pypyodbc
import pandas
import numpy as np
from typing import Tuple, List, Any
import logging


def db_connect(cfg: Any, db: str) -> Tuple[Any, Any]:
    """
    Wrapper to connect to the databases
    """
    connection = pypyodbc.connect(
        'Driver={};'.format(cfg.driver) \
        + 'Server={};Database={};uid={};pwd={}'.format(
            cfg.server, db, cfg.user, cfg.password))
    cursor = connection.cursor()
    return connection, cursor


def find_test_names(c: Any) -> List[str]:
    """
    Get the names for all of the tests that have
    been run and return as list
    """
    sql_cmd = """SELECT test_name FROM TestList_Table;"""
    c.execute(sql_cmd)
    tempvals = c.fetchall()
    return list(set(map(lambda x: x[0], tempvals)))


def find_test_ids(c: Any, test_name: str) -> List[int]:
    """
    Get the test ids for all of the test names and return as list
    """
    sql_cmd = """SELECT Test_ID
                 FROM TestList_Table
                 WHERE
                      test_name = ?
                 ORDER BY First_Start_DateTime;"""
    params =[test_name]
    c.execute(sql_cmd, params)
    temp = c.fetchall()
    return list(map(lambda x: int(x[0]), temp))


def find_channel_id(c: Any, test_id: int) -> List[int]:
    """
    Get the channels that a test id was run on and return as list,
    some test ids will have multiple channels
    """
    sql_cmd = "SELECT Channel_ID FROM Resume_Table WHERE test_id = ?;"
    params = [test_id]
    c.execute(sql_cmd, params)
    temp = c.fetchall()
    return list(map(lambda x: int(x[0]), temp))


def find_start_stop(cfg: Any, c: Any, test_id: int,
                    chan_id: int) \
                    -> Tuple[List, List, List, List, List]:
    """
    Find out when the test started and when it stopped, along
    with which databases the results are stored in. Due to lack of
    documentation and functional clarity we double check
    the last database to see if there is newer data (past the
    last end datetime) Note that the event time stamps from the
    result databases are 10000000 * epoch_time
    """
    sql_cmd = """SELECT IV_Ch_ID, First_Start_DateTime,
                        Last_End_DateTime, Databases
                 FROM TestIVChList_Table
                 WHERE
                      test_id = ? AND IV_Ch_ID = ?
                 ORDER BY First_Start_DateTime, IV_Ch_ID;"""
    inserts = [test_id, chan_id]
    c.execute(sql_cmd, inserts)
    temp = c.fetchall()
    iv, starts, stops, databases = zip(*temp)
    list_iv, list_starts, list_stops, list_databases = list(iv), list(
        starts), list(stops), list(databases)

    min_db_num = min(list(int(db[12:]) for db in databases[0].split(',')[:-1]))  # This is to get around a corrupted db
    if min_db_num >= cfg.MIN_DATABASE_NUMBER:
        sql_cmd = """WITH
                lt AS (
                SELECT
                    Test_ID,
                    Channel_ID,
                    MAX(Date_Time) AS Latest_Event_Time
                FROM
                    dbo.Event_Table
                GROUP BY
                    Test_ID,
                    Channel_ID)
    
                SELECT
                    lt.Test_ID,
                    lt.Channel_ID,
                    lt.Latest_Event_Time,
                    et.Event_ID,
                    et.Event_Type,
                    et.Event_Desc
                FROM
                    dbo.Event_Table et
                    INNER JOIN lt
                    ON et.Test_ID = lt.Test_ID
                    AND et.Channel_ID = lt.Channel_ID
                WHERE
                    et.Date_Time = lt.Latest_Event_Time
                    AND et.Test_ID=?
                    AND et.Channel_ID=?;"""
        inserts = [test_id, chan_id]
        temp2 = []
        db_result_last = -2
        while temp2 == []:
            try:
                connection, cur = db_connect(cfg, databases[0].split(',')[db_result_last])
                cur.execute(sql_cmd, inserts)
                temp2 = cur.fetchall()
                db_result_last = db_result_last -1
            except IndexError:
                logging.warning('Warning! Unable to find any events for test_id:' +
                 str(test_id) + ' chan_id:' + str(chan_id))
                temp2 = [(test_id, chan_id, 0, 0, 'null', 'null')]
                break
        connection.close()
        test_id, chan_id, last_event, event_id, event_type, event_desc = zip(
            *temp2)
        list_last_event = list(last_event)
    else:
        list_last_event = [0]

    return list_iv, list_starts, list_stops, list_databases, list_last_event


def find_steps(connection: Any, channel_id: int, min_time: float,
               max_time: float) -> pandas.DataFrame:
    """
    Get the time stamps for the steps and the cycle number
    """
    sql_cmd = """SELECT date_time, New_Step_ID, New_Cycle_ID
                 FROM Event_Table
                 WHERE
                      (Channel_ID = ?
                      AND date_time >= ?
                      AND date_time < ?);"""
    params = [channel_id, min_time, max_time]
    step_frame = pandas.read_sql(
        sql_cmd, connection, params=params, index_col=['date_time'])
    logging.info('Done with step query')
    step_frame.columns = ['Step_Index', 'Cycle_Index']

    step_frame.drop_duplicates(inplace=True)
    assert isinstance(step_frame, pandas.DataFrame)
    return step_frame


def find_raw_data(connection: Any, channel_id: int, min_time: int,
                  max_time: int) -> pandas.DataFrame:
    """
    Get all of the channel information for a given time window and channel.
    This function does most of the heavy lifting to actually retrieve the data
    be cautious changing this function
    """
    frames = []
    aliases = {
        22: 'Current',
        21: 'Voltage',
        23: 'Charge_Capacity',
        24: 'Discharge_Capacity',
        25: 'Charge_Energy',
        26: 'Discharge_Energy',
        27: 'dV/dt',
        30: 'Internal_Resistance'
    }
    sql_cmd = """SELECT data_type, date_time, data_value
                 FROM Channel_RawData_Table
                 WHERE
                      (channel_id = ?
                      AND date_time >= ?
                      AND date_time < ?);"""
    params = [channel_id, min_time, max_time]
    total_data = pandas.read_sql(sql_cmd, connection, params=params)
    logging.info('Done with raw query')
    if total_data.empty:
        return total_data
    data_groups = total_data.groupby(['data_type'])

    for key, name in aliases.items():
        if key in data_groups.groups.keys():
            df = data_groups.get_group(key).copy()
        else:
            blank_data = {
                'data_type': pandas.Series(key, index=[0]),
                'date_time': pandas.Series(min_time, index=[0]),
                'data_value': pandas.Series(np.NaN, index=[0])
            }
            df = pandas.DataFrame(blank_data)
        df.drop('data_type', axis=1, inplace=True)
        df.sort_values(by=['date_time'], inplace=True)
        df.set_index(keys=['date_time'], drop=True, inplace=True)
        df.columns = [name]
        df = df[~df.index.duplicated(keep='first')]
        frames.append(df)
    joined_frame = pandas.concat(frames, axis=1, join='outer')
    return joined_frame


def find_auxiliary_data(connection: Any, channel_id: int, min_time: int,
                        max_time: int) -> pandas.DataFrame:
    """
    The auxiliary data lives in a different table. This function queries the
    data for a channel and returns a dataframe with the aux voltage and
    temperature as columns and date time as an index
    """
    frames = []
    aliases = {0: 'Aux_Voltage', 1: 'Temperature'}
    sql_cmd = """SELECT data_type, date_time, data_value
                 FROM Auxiliary_Table
                 WHERE
                      (AuxCh_ID = ?
                      AND date_time >= ?
                      AND date_time < ?);"""
    params = [channel_id, min_time, max_time]
    total_data = pandas.read_sql(sql_cmd, connection, params=params)
    logging.info('Done with aux query')
    if total_data.empty:
        return total_data

    data_groups = total_data.groupby(['data_type'])

    for key, name in aliases.items():
        if key in data_groups.groups.keys():
            df = data_groups.get_group(key).copy()
        else:
            blank_data = {
                'data_type': pandas.Series(key, index=[0]),
                'date_time': pandas.Series(min_time, index=[0]),
                'data_value': pandas.Series(np.NaN, index=[0])
            }
            df = pandas.DataFrame(blank_data)
        df.drop('data_type', axis=1, inplace=True)
        df.sort_values(by=['date_time'], inplace=True)
        df.set_index(keys=['date_time'], drop=True, inplace=True)
        df.columns = [name]
        df = df[~df.index.duplicated(keep='first')]
        frames.append(df)
    joined_frame = pandas.concat(frames, axis=1, join='outer')
    return joined_frame


def find_meta_data(connection: Any, test_id: int,
                   iv_ch_id: int) -> pandas.DataFrame:
    """
    The start time, stop time, databases used and schedule file name are
    stored here. This is regarded as the primary meta data for the test
    """
    sql_cmd = """SELECT *
                 FROM TestIVChList_Table
                 WHERE
                      (test_id=? AND iv_ch_id=?);"""
    params = [test_id, iv_ch_id]
    total_data = pandas.read_sql(sql_cmd, connection, params=params)
    return total_data
