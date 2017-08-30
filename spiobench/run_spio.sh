#! /bin/sh

create_bias=75
rw_bias=50
append_bias=50
min_io=512
max_io=4096
sleepy=1
burst=10
fsync_f=5
num_files=100
num_dirs=10
min_fsize=1024
max_fsize=16777216
num_ops=1000
min_time=30
seed=42
threads=0
target_dir="/mnt/m8"

date
./spio $create_bias $rw_bias $append_bias $min_io $max_io $sleepy $burst $fsync_f $num_files $num_dirs $min_fsize $max_fsize $num_ops $min_time $seed $target_dir $threads
date
