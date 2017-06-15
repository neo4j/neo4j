#!/bin/bash

subj="/C=SE/ST=Malmo/L=Malmo/O=neo4j/OU=dev/CN=Company_CA"

# Generates a 2048 RSA private key in PKCS#1 format
openssl genrsa -out ca.key1 2048

# Transforms it into PKCS#8 format as commonly required
openssl pkcs8 -topk8 -nocrypt -in ca.key1 -out ca.key
rm ca.key1

# Creates a certificate signing request (CSR) for the key
openssl req -new -key ca.key -out ca.csr -subj $subj

# Signs the CA CSR by root
openssl x509 -req -in ca.csr -extfile ca.ext -CA root.crt -CAkey root.key -CAcreateserial -out ca.crt -days 36500 -sha256
rm ca.csr
