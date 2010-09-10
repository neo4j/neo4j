#!/bin/sh

if [ -f $2 ]; then
    PIDS=$(jps -l | grep $1 | sort | join $2 - | cut -f1 -d\ )
    if [ -n "$PIDS" ]; then
        echo Shutting down previous instances
        kill $PIDS

        for sec in {1..10}; do
            sleep 1
            PIDS=$(jps -l | grep $1 | sort | join $2 - | cut -f1 -d\ )
            if [ -z "$PIDS" ]; then
                break
            fi
        done

        if [ -n "$PIDS" ]; then
            echo Killing previous instances
            kill -9 $PIDS
        fi
    fi
fi
