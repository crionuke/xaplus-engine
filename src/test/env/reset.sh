#!/bin/bash

docker-compose kill

rm -rf pg1
rm -rf pg2
rm -rf tlog

docker-compose up -d