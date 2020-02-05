# Copyright 2018 Toyota Research Institute. All rights reserved.
import pypyodbc
import logging
import pandas
import numpy as np
import sql_functions
from typing import Tuple, List, Any


class ArbinTime:
    """
    This class converts between the arbin timestamp values (ints)
    from the result databases and the seconds since epoch.
    Each timestamp value is
    epoch_time * 10000000
    (100 microseconds is the last digit)
    """

    def __init__(self) -> None:
        self.conversion_to_arbin_timestamp = 10000000

    def query(self, time: float) -> int:
        query_time = int(time * self.conversion_to_arbin_timestamp)
        return query_time

    def to_epoch(self, timestamp: int) -> float:
        epoch_time = timestamp / self.conversion_to_arbin_timestamp
        return epoch_time


def fill_times(row):
    """
    For each step the date time value is set to be the date time
    value of the first preceding row that has a step index
    This function then subtracts the date time for each row
    and returns that value, which should be the step time
    """
    return row['DateTime'] - row['Step_Time']


def pull_and_join(cfg: Any, test_id: int, channel: int, starts: List,
                  stops: List,
                  dbs: List) -> Tuple[pandas.DataFrame, float, float]:
    """
    This is the primary data manipulation function. It calls the sql query
    functions and joins the returned data frames. Based on the step and cycle
    date time entries it fills in step time and test time. It also fills
    in values in columns that do not have a value for that time stamp.
    """
    db_frames = []
    listed_windows = list(zip(starts, stops, dbs))
    arbin_time = ArbinTime()
    for window_index, window in enumerate(listed_windows):
        start = window[0]
        stop = window[1]
        db_offset = 0
        set_test_start_flag = True
        for db_index, db in enumerate(window[2].split(',')[:-1]):
            logging.info('Getting data from:' + db)
            for i in range(cfg.ATTEMPTS):
                try:
                    connection, cursor = sql_functions.db_connect(cfg, db)
                    steps_frame = sql_functions.find_steps(
                        connection, channel, arbin_time.query(start),
                        arbin_time.query(stop))
                    raw_frame = sql_functions.find_raw_data(
                        connection, channel, arbin_time.query(start),
                        arbin_time.query(stop))
                    aux_frame = sql_functions.find_auxiliary_data(
                        connection, channel, arbin_time.query(start),
                        arbin_time.query(stop))
                except pypyodbc.OperationalError:
                    logging.warning('Database read error')
                    continue
                except UnboundLocalError:
                    logging.warning('Unknown database read error')
                    continue
                else:
                    connection.close()
                break
            logging.info('Done getting info from: ' + db)

            if raw_frame.empty or steps_frame.empty:
                db_offset = db_offset + 1  # to deal with empty data frame and set start time correctly
                continue

            if not aux_frame.empty:
                aux_frame = aux_interpolate(raw_frame.index, aux_frame)
            else:
                blank_data = {
                    'date_time': pandas.Series(raw_frame.index[0], index=[0]),
                    'Temperature': pandas.Series(np.NaN, index=[0]),
                    'Aux_Voltage': pandas.Series(np.NaN, index=[0])
                }
                aux_frame = pandas.DataFrame(blank_data)

            set_frame = pandas.concat(
                [raw_frame, steps_frame, aux_frame], axis=1, join='outer')
            set_frame.reset_index(inplace=True)

            # if db_index == 0:
            #     start_time = steps_frame.index[0]
            if db_index == (0 + db_offset) and window_index == 0 and set_test_start_flag:
                start_time = steps_frame.index[0]
                set_test_start_flag = False

            set_frame['Test_Time'] = arbin_time.to_epoch(
                set_frame.date_time - start_time)
            set_frame['Step_Time'] = arbin_time.to_epoch(set_frame.date_time)
            set_frame.date_time = arbin_time.to_epoch(set_frame.date_time)
            set_frame.loc[set_frame.Step_Index.isnull(), 'Step_Time'] = np.NaN
            set_frame['AC_Impedance'] = 0
            set_frame['Is_FC_Data'] = 0
            set_frame['ACI_Phase_Angle'] = 0
            set_frame.rename(columns={'date_time': 'DateTime'}, inplace=True)
            cols = [
                'Test_Time', 'DateTime', 'Step_Time', 'Step_Index',
                'Cycle_Index', 'Current', 'Voltage', 'Charge_Capacity',
                'Discharge_Capacity', 'Charge_Energy', 'Discharge_Energy',
                'dV/dt', 'Internal_Resistance', 'Temperature', 'Aux_Voltage'
            ]
            db_frames.append(set_frame[cols])

    query_last_time = max(stops)

    if not db_frames:
        logging.warning(
            'No data for test id:' + str(test_id) + ' channel:' + str(channel))
        return pandas.DataFrame(
            columns=['DateTime', 'Cycle_Index']), query_last_time, 0

    full_test_frame = pandas.concat(db_frames, ignore_index=True)
    full_test_frame.fillna(method='ffill', inplace=True)
    full_test_frame = full_test_frame[np.isfinite(
        full_test_frame['Step_Index'])]
    full_test_frame.Step_Time = full_test_frame.apply(fill_times, axis=1)
    full_test_frame = full_test_frame[full_test_frame.Step_Time != 0]
    # delete the rows that were inserted by steps frame
    full_test_frame.reset_index(drop=True, inplace=True)
    full_test_frame.index.name = 'Data_Point'
    full_test_frame = full_test_frame.round({'Step_Time': 4})
    full_test_frame['Step_Index'] = full_test_frame.Step_Index.astype('int')
    full_test_frame['Cycle_Index'] = full_test_frame.Cycle_Index.astype('int')
    return full_test_frame, query_last_time, len(full_test_frame.index)


def pull_meta_data(cfg: Any, test_name_channel_test_id: int,
                   test_name_channel_chan_id: int) -> pandas.DataFrame:
    for i in range(cfg.ATTEMPTS):
        try:
            connection, c = sql_functions.db_connect(cfg, "ArbinMasterData")
            meta_data_frame = sql_functions.find_meta_data(
                connection, test_name_channel_test_id,
                test_name_channel_chan_id)
        except pymssql.OperationalError:
            logging.warning('Database read error')
            continue
        except UnboundLocalError:
            logging.warning('Unknown database read error')
            continue
        else:
            connection.close()
        break

    return meta_data_frame


def aux_interpolate(date_time: pandas.Series,
                    aux_frame: pandas.DataFrame) -> pandas.DataFrame:
    """
    Re-sample the aux data values to the same time indices
     as the main data file and return so that the outer join to the rest
     of the data and fill of NaN values does not produce duplicate values
    """
    interp_temp = np.interp(date_time, aux_frame.index, aux_frame.Temperature)
    interp_aux_volt = np.interp(date_time, aux_frame.index,
                                aux_frame.Aux_Voltage)
    interp_temp = pandas.DataFrame(
        data=interp_temp, index=date_time, columns=['Temperature'])
    interp_aux_volt = pandas.DataFrame(
        data=interp_aux_volt, index=date_time, columns=['Aux_Voltage'])
    interpolated_aux = pandas.concat(
        [interp_temp, interp_aux_volt], axis=1, join='inner')
    return interpolated_aux
