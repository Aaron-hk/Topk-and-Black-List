# -*- coding: utf-8 -*-
"""
@File  : mapper.py
@Author: SangYu
@Date  : 2019/5/15 18:27
@Desc  : 第一次map
"""
import os


def read_input(file):
    """
    读取文件
    :param file: 文件对象
    :return: 以迭代器方式每次返回文件对象的行
    """
    for line in open(file, "r", encoding="utf-8"):
        yield line.rstrip()


def topk_first_mapper():
    """
    第一次map，将原始数据ip    access_time --> ip  1   access_time
    """
    # 读取日志文件
    f_map = open("process_files/topk_first_map.txt", "w", encoding="utf-8")
    for file in os.listdir("data_files"):
        data = read_input(os.path.join("data_files", file))
        for line in data:
            line_split = line.split()
            ip = line_split[0]
            access_time = line_split[-1]
            # ip    access_time --> ip  1   access_time
            f_map.write("%s\t%d\t%s\n" % (ip, 1, access_time))
    f_map.close()


def topk_second_mapper():
    """
    第二次map:使用IP访问次数作为排序键
    """
    # 读取第一次reduce的结果
    f_map = open("process_files/topk_second_map.txt", "w", encoding="utf-8")
    data = read_input("process_files/topk_first_reduce.txt")
    for line in data:
        line_split = line.split("\t")
        f_map.write("%s\t%s\t%s\n" % (line_split[1], line_split[0], line_split[2]))
    f_map.close()


def black_list_first_mapper():
    """
    对读入的文档进行时间排序，ip access_time --> access_time ip  1
    """
    # 打开记录文件
    f_map = open("process_files/black_list_first_map.txt", "w", encoding="utf-8")
    # 读取日志文件
    for file in os.listdir("data_files"):
        data = read_input(os.path.join("data_files", file))
        for line in data:
            line_split = line.split()
            ip = line_split[0]
            access_time = line_split[-1]
            f_map.write("%s\t%s\t%d\n" % (access_time, ip, 1))
    f_map.close()


def black_list_second_mapper():
    """
    将排序后的日志记录转化为热点时间列表：
    access_time ip  1   --> ip  [{ts:access_time,te:access_time,count:1}]
    """
    # 打开记录文件
    f_map = open("process_files/black_list_second_map.txt", "w", encoding="utf-8")
    # 读取第一次reduce的结果
    data = read_input("process_files/black_list_first_reduce.txt")
    for line in data:
        line_split = line.split("\t")
        access_time = line_split[0]
        ip = line_split[1]
        count = line_split[2]
        f_map.write("%s\t%s %s %s\n" % (ip, access_time, access_time, count))
    f_map.close()


def black_list_second_mapper_test():
    """
    将排序后的日志记录转化为热点时间列表：
    access_time ip  1   --> ip  [{ts:access_time,te:access_time,count:1}]
    """
    # 打开记录文件
    f_map = open("process_files/black_list_second_map.txt", "w", encoding="utf-8")
    # 读取第一次reduce的结果
    data = read_input("process_files/black_list_first_reduce.txt")
    for line in data:
        line_split = line.split("\t")
        access_time = line_split[0]
        ip = line_split[1]
        count = line_split[2]
        f_map.write("%s\t%s %s %s\n" % (ip, access_time, access_time, count))
    f_map.close()

def black_list_third_mapper():
    """
    将热点时间按IP访问次数为key进行分解
    """
    # 打开记录文件
    f_map = open("process_files/black_list_third_map.txt", "w", encoding="utf-8")
    # 读取第二次reduce的结果
    data = read_input("process_files/black_list_second_reduce.txt")
    for line in data:
        line_split = line.split("\t")
        ip = line_split[0]
        hot_time_list = line_split[1].split(",")[:-1]
        for hot_time in hot_time_list:
            hot_time_split = hot_time.split()
            ts = hot_time_split[0]
            te = hot_time_split[1]
            count = hot_time_split[2]
            f_map.write("%s\t%s %s %s\n" % (count, ip, ts, te))
    f_map.close()


if __name__ == '__main__':
    # topk_first_mapper()
    # topk_second_mapper()
    # black_list_first_mapper()
    black_list_third_mapper()
    pass