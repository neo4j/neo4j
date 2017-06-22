#!/bin/bash

# This is the main entry point and generates everything necessary.

rm -rf servers
rm -rf ca

./gen-ca.sh

./gen-server.sh 0
./gen-server.sh 1
./gen-server.sh 2
./gen-server.sh 3
./gen-server.sh 4
./gen-server.sh 5

# The first parameter is the certificate serial number and the second is the keyId / directory name.
./gen-revocation.sh 01 0
./gen-revocation.sh 02 1
./gen-revocation.sh 03 2
./gen-revocation.sh 04 3
./gen-revocation.sh 05 4
./gen-revocation.sh 06 5
