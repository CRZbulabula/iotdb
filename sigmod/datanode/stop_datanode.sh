#!/bin/bash

# 搜索名为 datanode 的进程，并杀死这些进程
kill -9 $(ps aux | grep datanode | grep -v grep | awk '{print $2}')
