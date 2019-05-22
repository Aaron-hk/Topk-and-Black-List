# -*- coding: utf-8 -*-
"""
@File  : GetTopK.py
@Author: SangYu
@Date  : 2019/5/16 9:27
@Desc  : 生成数据，并获取数据的topk
"""
from data_producer import *
from mapper import *
from reducer import *
import time


def get_topk(k: int):
    start=time.time()
    log_sum=produce_data(200, "2019-05-01-00-00-00", 0.1)
    end= time.time()
    print("随机日志数据生成耗时%s秒..." % (end - start))
    print("数据：\n"
          "\t共200个日志文件，一共%s条数据\n"
          "访问日志的时间区间为:2019-05-01 00:00:00 至 02:24:00"%log_sum)
    start = time.time()
    topk_first_mapper()
    topk_first_reducer()
    topk_second_mapper()
    topk_second_reducer()
    print("TopK任务耗时%s...秒" % (time.time() - start))
    data = read_input("process_files/second_reduce.txt")
    print("前%d个数据" % k)
    while k > 0:
        print(data.__next__())
        k -= 1


if __name__ == '__main__':
    if not os.path.exists("./process_files"):
        os.mkdir("./process_files")
    get_topk(9)