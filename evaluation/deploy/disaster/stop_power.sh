#!/bin/bash

kill -9 $(ps aux | grep "python /home/ubuntu/power.py" | grep -v grep | awk '{print $2}')
