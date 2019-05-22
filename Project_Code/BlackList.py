# -*- coding: utf-8 -*-
"""
@File  : BlackList.py
@Author: SangYu
@Date  : 2019/5/18 11:39
@Desc  : 获取黑名单
"""
from data_producer import *
from mapper import *
from reducer import *
import time


def get_black_list(threshold: int, number: int):
    """
    获得黑名单，在时间阈值threshold内访问了number次
    :param threshold: 时间阈值
    :param number: 访问次数
    """
    #produce_data(100, "2019-05-01-00-00-00", 0.01)
    start = time.time()
    black_list_first_mapper()
    print("first_map end!")
    black_list_first_reducer()
    print("first_reduce end!")
    black_list_second_mapper()
    print("second_map end!")
    black_list_second_reducer(threshold)
    print("second_reduce end!")
    black_list_third_mapper()
    print("third_map end!")
    black_list_third_reducer()
    print("third_reduce end!")
    print("耗时%s..." % (time.time() - start))
    data = read_input("process_files/black_list_third_reduce.txt")
    print("黑名单数据")
    for line in data:
        line_split = line.split("\t")
        count = int(line_split[0])
        if count >= number:
            print(line)
        else:
            break

def get_black_list_test(threshold: int, number: int):
    """
    获得黑名单，在时间阈值threshold内访问了number次
    :param threshold: 时间阈值
    :param number: 访问次数
    """
    start=time.time()
    log_sum=produce_data(200, "2019-05-01-00-00-00", 0.01)
    end = time.time()
    print_to_file("随机日志数据生成耗时%s秒..." % (end - start))
    print_to_file("数据：\n"
          "\t共200个日志文件，一共%s条数据"%log_sum)
    start = time.time()
    black_list_first_mapper()
    print_to_file("first_map end!")
    black_list_first_reducer()
    print_to_file("first_reduce end!")
    black_list_second_mapper_test()
    print_to_file("second_map end!")
    black_list_second_reducer_test(threshold, number)
    print_to_file("second_reduce end!")
    black_list_third_mapper()
    print_to_file("third_map end!")
    black_list_third_reducer()
    print_to_file("third_reduce end!")
    print_to_file("程序耗时%s秒..." % (time.time() - start))
    data = read_input("process_files/black_list_third_reduce.txt")
    print("黑名单数据")
    for line in data:
        line_split = line.split("\t")
        count = int(line_split[0])
        if count >= number:
            print_to_file(line)
        else:
            break

def print_to_file(string):
    print(string)
    with codecs.open("Black_List.txt", mode="a", encoding="utf-8") as f:
        f.write(string)
        f.write("\n")

def verify_rst(dst_ip:str, start_time:str, end_time:str):
    file="process_files/black_list_first_map.txt"
    start_time_seconds=timef_to_seconds(start_time)
    end_time_seconds=timef_to_seconds(end_time)
    with open(file, mode="r", encoding="utf-8") as f:
        lines=f.readlines()
    cnt=0
    for line in lines:
        line_struct=line.split("\t")
        ip=line_struct[1]
        access_time=line_struct[0]
        access_time_seconds=timef_to_seconds(access_time)
        if ip==dst_ip:
            if access_time_seconds<=end_time_seconds and access_time_seconds>=start_time_seconds:
                cnt+=1
    print(cnt)


if __name__ == '__main__':
    if not os.path.exists("./process_files"):
        os.mkdir("./process_files")
    get_black_list_test(10,15)
    #get_black_list(10, 5)
    # 10	9.1.2.6 2019-05-01-00-06-11 2019-05-01-00-06-31
    #verify_rst("9.1.2.6", "2019-05-01-00-06-11", "2019-05-01-00-06-31")