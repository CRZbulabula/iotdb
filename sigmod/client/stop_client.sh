#!/bin/bash

# 搜索名为 benchmark 的进程，并杀死这些进程
kill -9 $(ps aux | grep java | grep -v grep | awk '{print $2}')
