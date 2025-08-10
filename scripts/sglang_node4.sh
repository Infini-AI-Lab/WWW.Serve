#!/bin/bash

SESSION_NAME="sglang_node4"

ALLOC_CMD="salloc -N 1 -n 4 --gres=gpu:1 -p RTX3090"

SSH_TARGET="node2"

CUDA_PATH="/data1/public/cuda/cuda-12.4/bin"

CONDA_ENV="sglang"

MODEL_PATH="/home/hywang/Reasoning/Decentralized-Agents/models/DeepSeek-R1-Distill-Qwen-7B"

PORT=30003


tmux new-session -d -s "$SESSION_NAME"


tmux send-keys -t "$SESSION_NAME" "$ALLOC_CMD" C-m
tmux send-keys -t "$SESSION_NAME" "ssh $SSH_TARGET" C-m
tmux send-keys -t "$SESSION_NAME" "export PATH=$CUDA_PATH:\$PATH" C-m
tmux send-keys -t "$SESSION_NAME" "conda activate $CONDA_ENV" C-m
tmux send-keys -t "$SESSION_NAME" "python3 -m sglang.launch_server --model-path $MODEL_PATH --host 0.0.0.0 --port $PORT --enable-metrics" C-m


# tmux attach -t "$SESSION_NAME"
