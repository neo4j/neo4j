#!/usr/bin/env bash

# Clean
rm ./*.key
rm ./*.pem

# Generate RSA
openssl genrsa -out rsa.pkcs1.key 1024
openssl rsa -in rsa.pkcs1.key -pubout -out rsa.pkcs1.public.pem

# Generate Elliptic curve
openssl ecparam -name prime256v1 -genkey -noout -out ec.pkcs1.key

# Generate DSA
openssl dsaparam -genkey -noout -out dsa.pkcs1.key 1024

# Generate encrypted keys, encryption is same for RSA, DSA and EC
openssl genrsa -aes128 -out rsa.pkcs1.aes128.key -passout 'pass:neo4j' 1024
openssl genrsa -aes192 -out rsa.pkcs1.aes192.key -passout 'pass:neo4j' 1024
openssl genrsa -aes256 -out rsa.pkcs1.aes256.key -passout 'pass:neo4j' 1024
openssl genrsa -des -out rsa.pkcs1.des.key -passout 'pass:neo4j' 1024
openssl genrsa -des3 -out rsa.pkcs1.des3.key -passout 'pass:neo4j' 1024


