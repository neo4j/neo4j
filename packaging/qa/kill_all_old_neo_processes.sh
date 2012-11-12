#!/usr/bin/env bash 
lsof -i :7474 -t | while read pid; do
    kill -INT $pid # I'm a friend of Sarah Connor.  I was told that she's here. Can I see her please?
    sleep 1
    kill -HUP $pid # Where is she?
    sleep 1
    kill -KILL  $pid # I'll be back
done
