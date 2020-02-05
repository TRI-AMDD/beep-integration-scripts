# Copyright 2018 Toyota Research Institute. All rights reserved.
import pytest
import sql_functions
import arbin_extract
import data_join
from typing import List
import pandas
import os
import difflib
import config

cfg = config.ConfigUnix()


def test_sql_connection_get_names():
    connection, cursor = sql_functions.db_connect(cfg, "ArbinMasterData")
    test_names = sql_functions.find_test_names(cursor)
    connection.close()
    assert isinstance(test_names, List)
    assert len(test_names) > 0


def test_start_stop_times(capsys):
    connection, cursor = sql_functions.db_connect(cfg, "ArbinMasterData")
    list_ivs, list_starts, list_stops, list_dbs, list_last_event = \
        sql_functions.find_start_stop(cfg, cursor, 2, 9)
    connection.close()
    with capsys.disabled():
        print('')
    assert isinstance(list_ivs, List)
    assert isinstance(list_dbs, List)
    assert len(list_starts) == len(list_stops)
    for idx, start in enumerate(list_starts):
        assert start < list_stops[idx]


def test_list_test_channels(capsys):
    connection, cursor = sql_functions.db_connect(cfg, "ArbinMasterData")
    test_name_chs = arbin_extract.list_test_channels(cfg, cursor)
    connection.close()
    example_excluded_test_ch = arbin_extract.NameTestChannel(
        test='2018-02-16_newcomputerdiagnostic',
        test_id=1,
        channel=2)
    assert example_excluded_test_ch not in test_name_chs
    assert isinstance(test_name_chs, List)
    for test_name_channel in test_name_chs:
        assert isinstance(test_name_channel.test, str)
        assert isinstance(test_name_channel.test_id, int)
        assert isinstance(test_name_channel.channel, int)


def test_single_test_channel():
    data_folder = cfg.data_folder
    conn, c = sql_functions.db_connect(cfg, "ArbinMasterData")
    test_name_channel = arbin_extract.NameTestChannel('script_test', 2, 43)

    fresh_data, starts, stops, dbs = arbin_extract.new_data(
        cfg, test_name_channel.test_id, test_name_channel.channel, c)
    name = test_name_channel.test + cfg.channel_delimiter + str(
        test_name_channel.channel + 1)
    full_test_frame, query_final_time, query_test_length = \
        data_join.pull_and_join(cfg, test_name_channel.test_id,
                                test_name_channel.channel,
                                starts, stops, dbs)
    meta_data_frame = data_join.pull_meta_data(cfg, test_name_channel.test_id,
                                               test_name_channel.channel)
    full_test_frame.to_csv(
        path_or_buf=os.path.join(data_folder, name + '.csv'))
    meta_data_frame.to_csv(
        path_or_buf=os.path.join(data_folder, name + '_Metadata' + '.csv'))

    conn.close()


def test_validate_single_test_channel():
    test_name_channel = arbin_extract.NameTestChannel('script_test', 2, 43)
    data_folder = cfg.data_folder
    name = test_name_channel.test + cfg.channel_delimiter + str(
        test_name_channel.channel + 1)
    full_test_frame = pandas.read_csv(os.path.join(data_folder, name + '.csv'))
    valid_frame = pandas.read_csv(
        os.path.join(cfg.path_to_validation_xlsx, 'validation_test_CH44' + '.csv'))

    script_discharge_capacity = [
        '{:.6f}'.format(x)
        for x in full_test_frame['Discharge_Capacity'].tolist()
    ]
    arbin_discharge_capacity = [
        '{:.6f}'.format(x)
        for x in valid_frame['Discharge_Capacity(Ah)'].tolist()
    ]
    diff_file = open(
        os.path.join(data_folder, 'diff_discharge_capacity.txt'), 'w')
    for line in difflib.unified_diff(
            script_discharge_capacity,
            arbin_discharge_capacity,
            fromfile='script',
            tofile='arbin',
            lineterm=''):
        diff_file.write(line)
        diff_file.write('\n')
    diff_file.close()

    script = ['{:.4f}'.format(x) for x in full_test_frame['Voltage'].tolist()]
    arbin = ['{:.4f}'.format(x) for x in valid_frame['Voltage(V)'].tolist()]
    diff_file = open(os.path.join(data_folder, 'diff_voltage.txt'), 'w')
    for line in difflib.unified_diff(
            script, arbin, fromfile='script', tofile='arbin', lineterm=''):
        diff_file.write(line)
        diff_file.write('\n')
    diff_file.close()

    script = ['{:.4f}'.format(x) for x in full_test_frame['Current'].tolist()]
    arbin = ['{:.4f}'.format(x) for x in valid_frame['Current(A)'].tolist()]
    diff_file = open(os.path.join(data_folder, 'diff_current.txt'), 'w')
    for line in difflib.unified_diff(
            script, arbin, fromfile='script', tofile='arbin', lineterm=''):
        diff_file.write(line)
        diff_file.write('\n')
    diff_file.close()

    # script = ['{:.4f}'.format(x) for x in full_test_frame['Temperature'].tolist()]
    # arbin = ['{:.4f}'.format(x) for x in valid_frame['Temperature(C)'].tolist()]
    # diff_file = open(os.path.join(data_folder, 'diff_temperature.txt'), 'w')
    # for line in difflib.unified_diff(
    #         script, arbin, fromfile='script', tofile='arbin', lineterm=''):
    #     diff_file.write(line)
    #     diff_file.write('\n')
    # diff_file.close()

    script = [
        '{:.1f}'.format(x) for x in full_test_frame['Test_Time'].tolist()
    ]
    arbin = ['{:.1f}'.format(x) for x in valid_frame['Test_Time(s)'].tolist()]
    diff_file = open(os.path.join(data_folder, 'diff_test_time.txt'), 'w')
    for line in difflib.unified_diff(
            script, arbin, fromfile='script', tofile='arbin', lineterm=''):
        diff_file.write(line)
        diff_file.write('\n')
    diff_file.close()

    script = [
        '{:.1f}'.format(x) for x in full_test_frame['Step_Time'].tolist()
    ]
    arbin = ['{:.1f}'.format(x) for x in valid_frame['Step_Time(s)'].tolist()]
    diff_file = open(os.path.join(data_folder, 'diff_step_time.txt'), 'w')
    for line in difflib.unified_diff(
            script, arbin, fromfile='script', tofile='arbin', lineterm=''):
        diff_file.write(line)
        diff_file.write('\n')
    diff_file.close()
