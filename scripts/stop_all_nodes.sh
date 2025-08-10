#!/bin/bash

for i in {1..5}
do
    SESSION_NAME="node${i}"
    tmux send-keys -t "$SESSION_NAME" C-c
    sleep 5
    tmux send-keys -t "$SESSION_NAME" "exit" C-m
    sleep 1
    tmux send-keys -t "$SESSION_NAME" "exit" C-m
    sleep 1
    tmux send-keys -t "$SESSION_NAME" "exit" C-m
done