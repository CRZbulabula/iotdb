#!/bin/bash

kill -9 $(ps aux | grep auto_set_ttl | grep -v grep | awk '{print $2}')
