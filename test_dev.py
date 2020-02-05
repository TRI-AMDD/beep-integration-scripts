# Copyright 2018 Toyota Research Institute. All rights reserved.
import pandas
import os


def create_arbin_validation_base():
    dirname = os.path.dirname(__file__)
    path_to_validation_xlsx = os.path.join(dirname, 'validation_data')
    name = 'test_ch44'
    file_list = []
    frames = []
    for file in os.listdir(path_to_validation_xlsx):
        if file.endswith(".xlsx"):
            file_list.append(os.path.join(path_to_validation_xlsx, file))
    file_list.sort()
    for file in file_list:
        vald_frame_partial = pandas.read_excel(file, sheet_name=1)
        frames.append(vald_frame_partial)
    vald_frame = pandas.concat(frames, ignore_index=True)
    vald_frame.to_csv(
        path_or_buf=os.path.join(path_to_validation_xlsx, name + '.csv'))
