#!/bin/zsh

RESULT=1

while (( $RESULT != 0 ))
do
    docker compose stop && docker compose rm -f
    docker compose up -d
    ~/anaconda3/envs/TFM/bin/python main.py
    RESULT=$?
done