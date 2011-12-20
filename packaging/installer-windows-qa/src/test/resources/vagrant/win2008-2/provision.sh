#!/bin/bash

# Disable the firewall
netsh firewall set opmode disable

# Set static ip
netsh interface ipv4 set address name="Local Area Connection" source=static address=33.33.33.11 mask=255.255.255.0
