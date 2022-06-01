#!/usr/bin/env bash

# Clean
rm ./*.key

# Generate RSA
openssl genrsa -out rsa.key 1024
openssl pkcs8 -topk8 -inform PEM -outform PEM -nocrypt -in rsa.key -out rsa.pkcs8.key

# Generate DSA
openssl dsaparam -genkey -noout -out dsa.key 1024
openssl pkcs8 -topk8 -inform PEM -outform PEM -nocrypt -in dsa.key -out dsa.pkcs8.key

# Generate EC
openssl ecparam -name prime256v1 -genkey -noout -out ec.key
openssl pkcs8 -topk8 -inform PEM -outform PEM -nocrypt -in ec.key -out ec.pkcs8.key