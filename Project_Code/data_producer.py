#!/usr/bin/env python 
# -*- coding: utf-8 -*-

# @Author  : Aaron
# @File    : data_producer.py
# @Software: PyCharm


import os
import shutil
import codecs
import random
import time

day_seconds = 24 * 60 * 60
time_format = "%Y-%m-%d-%H-%M-%S"
log_num_min = 10000
log_num_max = 100000


def get_data_file_list(list_size):
    dst_dir = "./data_files"
    data_file_list = []
    for i in range(list_size):
        dst_file = os.path.join(dst_dir, str(i) + ".txt")
        data_file_list.append(dst_file)
    return data_file_list


def produce_data(file_num, begin_date, days):
    log_sum=0
    dst_dir = "./data_files"
    if os.path.exists(dst_dir):
        shutil.rmtree(dst_dir)
    os.mkdir(dst_dir)

    data_file_list = get_data_file_list(file_num)
    begin_seconds = timef_to_seconds(begin_date)
    range_seconds = days * day_seconds - 1

    for data_file in data_file_list:
        with codecs.open(data_file, encoding="utf-8", mode="w") as f:
            log_num = random.randint(log_num_min, log_num_max)
            log_sum+=log_num
            for cnt in range(log_num):
                ip = produce_ip()
                seconds = begin_seconds + random.randint(1, range_seconds - 1)
                timef_str = seconds_to_timef(seconds)

                f.write(str(ip))
                f.write("\t")
                f.write(str(timef_str))
                f.write("\n")

    return log_sum


def timef_to_seconds(timef_str):
    time_struct = time.strptime(timef_str, time_format)
    seconds = time.mktime(time_struct)
    return seconds


def seconds_to_timef(seconds):
    time_struct = time.localtime(seconds)
    timef_str = time.strftime(time_format, time_struct)
    return timef_str


def produce_ip():
    ip_str = ""
    range_num = 10
    for i in range(4):
        cur_num = random.randint(1, range_num)
        ip_str = ip_str + str(cur_num) + "."
    return ip_str[:-1]


def iteration_output(iteration_var):
    for item in iteration_var:
        print(item)


if __name__ == "__main__":
    produce_data(100, "2019-05-01-00-00-00", 3)
    pass
