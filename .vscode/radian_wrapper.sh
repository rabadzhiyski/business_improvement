#!/bin/bash
export LD_PRELOAD=/home/plamen/anaconda3/lib/libstdc++.so.6
exec /home/plamen/anaconda3/bin/radian "$@"
