#!/bin/bash

# Search and kill datanode processes
kill -9 $(ps aux | grep datanode | grep -v grep | awk '{print $2}')
