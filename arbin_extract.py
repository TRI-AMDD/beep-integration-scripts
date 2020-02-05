#!/usr/bin/env python3
# Copyright 2018 Toyota Research Institute. All rights reserved.
import os
import sql_functions
import data_join
import logging
import pandas
import datetime
import config
from typing import List, Tuple, Any


class NameTestChannel:
    """
    Class structure to store the meta information for each of the files
    that we are going to export
    """
    def __init__(self, test: str, test_id: int, channel: int) -> None:
        self.test = test
        self.test_id = test_id
        self.channel = channel


def list_test_channels(cfg: Any, c: Any) -> List[NameTestChannel]:
    """Find all of the tests in the database and their channels
    Exclusion logic removes test_names if they are on the excluded list
    and test_name_chs if they are on the excluded list
    """
    test_names = sql_functions.find_test_names(c)
    test_names = list(set(test_names) - set(cfg.excluded_tests))
    excluded_test_chs = [test_chs for test_chs in list(set(cfg.excluded_tests))
                         if cfg.channel_delimiter in test_chs]
    test_names.sort(reverse=True)
    test_name_chs = []
    for test in test_names:
        test_ids = sql_functions.find_test_ids(c, test)
        test_id = test_ids[-1]  # Possibly multiple test ids for a test name
        channel_ids = sql_functions.find_channel_id(c, test_id)
        for channel in channel_ids:
            ntc = NameTestChannel(test, test_id, channel)
            output_name = ntc.test + cfg.channel_delimiter + str(ntc.channel + 1)
            if output_name in excluded_test_chs:
                continue
            else:
                test_name_chs.append(ntc)
    return test_name_chs


def new_data(cfg: Any,
             test_id: int,
             channel: int,
             c: Any,
             test_final_time: float = -1) -> Tuple[bool, List, List, List]:
    """
    Contains logic for deciding when a test has started and when it has stopped
    The first start time is listed in the ArbinMasterData database but the stop
    time is unreliable. To solve this the event table for each relevant result
    database is queried and the final event for that channel and test is
    returned.
    This function decides which of those times to use for the actual data query
    """
    list_ivs, list_starts, list_stops, list_dbs, list_last_event = \
        sql_functions.find_start_stop(cfg, c, test_id, channel)
    arbin_time = data_join.ArbinTime()
    if max(list_stops) > 0:
        if arbin_time.to_epoch(max(list_last_event)) > max(list_stops):
            logging.info(
                'last event:' + str(arbin_time.to_epoch(max(list_last_event))) +
                ' stop: ' + str(max(list_stops)))
            list_stops[list_stops.index(max(list_stops))] = \
                arbin_time.to_epoch(max(list_last_event))
            logging.info('Data found after last stop on test id:' +
                         str(test_id) + ' chan id: ' + str(channel))
    else:
        list_stops[list_stops.index(max(list_stops))] = \
            arbin_time.to_epoch(max(list_last_event))

    if max(list_stops) <= test_final_time:
        fresh_data = False
    else:
        fresh_data = True

    return fresh_data, list_starts, list_stops, list_dbs


def main() -> None:
    logging.basicConfig(
        format='%(asctime)s %(message)s',
        filename=os.path.join(cfg.path_to_completed_list, 'Conversion.log'),
        level=logging.DEBUG)
    logging.info('Connecting to database')
    conn, c = sql_functions.db_connect(cfg, "ArbinMasterData")
    logging.info('Connected')

    test_name_chs = list_test_channels(cfg, c)
    logging.info(
        'Number of test name-channels in database:' + str(len(test_name_chs)))

    try:
        converted_tests = pandas.read_pickle(cfg.path_to_completed_list)
    except FileNotFoundError:
        converted_tests = pandas.DataFrame(columns=[
            'converted_test_and_ch', 'test_last_time', 'record_length'
        ])
    logging.info('Number of test name-channels converted:' +
                 str(len(converted_tests.index)))

    for test_name_channel in test_name_chs:
        name = test_name_channel.test + cfg.channel_delimiter + str(
            test_name_channel.channel + 1)  # +1 The Liveware Problem
        if name in converted_tests.converted_test_and_ch.unique():
            test_final_time = converted_tests.test_last_time[converted_tests[
                'converted_test_and_ch'] == name].max()
            test_length = converted_tests.record_length[converted_tests[
                'converted_test_and_ch'] == name].max()
            fresh_data, starts, stops, dbs = new_data(
                cfg, test_name_channel.test_id, test_name_channel.channel, c,
                test_final_time)

            min_db_num = min(list(int(db[12:]) for db in dbs[0].split(',')[:-1]))  #This is to get around a corrupted db
            if fresh_data and min_db_num >= cfg.MIN_DATABASE_NUMBER:
                logging.info('Updating: ' + name + ' with test_id:' + str(test_name_channel.test_id))

                full_test_frame, query_final_time, query_test_length = \
                    data_join.pull_and_join(cfg, test_name_channel.test_id,
                                            test_name_channel.channel,
                                            starts, stops, dbs)

                meta_data_frame = data_join.pull_meta_data(
                    cfg, test_name_channel.test_id, test_name_channel.channel)
            else:
                logging.info('No new data: ' + name)
                continue
        else:
            logging.info('New test: ' + name + ' with test_id:' + str(test_name_channel.test_id))
            test_length = 0
            fresh_data, starts, stops, dbs = new_data(
                cfg, test_name_channel.test_id, test_name_channel.channel, c)
            print(fresh_data, starts, stops, dbs)

            full_test_frame, query_final_time, query_test_length = \
                data_join.pull_and_join(cfg, test_name_channel.test_id,
                                        test_name_channel.channel,
                                        starts, stops, dbs)
            meta_data_frame = data_join.pull_meta_data(
                cfg, test_name_channel.test_id, test_name_channel.channel)
            print(query_final_time, query_test_length)

            new_converted_row = pandas.DataFrame(
                [[name, query_final_time, query_test_length]],
                columns=[
                    'converted_test_and_ch', 'test_last_time', 'record_length'
                ])
            converted_tests = converted_tests.append(
                new_converted_row, ignore_index=True)

        full_test_frame.to_csv(
            path_or_buf=os.path.join(cfg.data_folder, name + '.csv'))
        meta_data_frame.to_csv(
            path_or_buf=os.path.join(cfg.data_folder,
                                     name + '_Metadata' + '.csv'))

        converted_tests.loc[converted_tests['converted_test_and_ch'] == name,
                            'test_last_time'] = query_final_time
        converted_tests.loc[converted_tests['converted_test_and_ch'] == name,
                            'record_length'] = query_test_length
        converted_tests.to_pickle(cfg.path_to_completed_list)
        readable_datetime = datetime.datetime.fromtimestamp(
            float(query_final_time)).strftime('%Y-%m-%d %H:%M:%S')
        logging.info('Test: ' + name + ' Last data time:' + readable_datetime)
        logging.info('Finished with test: ' + name + ' Old length:' + str(
            test_length) + ' New length:' + str(query_test_length))

    conn.close()


if __name__ == "__main__":
    cfg = config.ConfigWindows()
    main()
