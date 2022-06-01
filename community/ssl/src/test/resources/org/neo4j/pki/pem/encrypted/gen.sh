#!/usr/bin/env bash

# Clean
rm ./*.key
rm ./*.pem

# Generate key
openssl genrsa -out keypair.key 1024

# Extract public key
openssl rsa -in keypair.key -pubout -out public.pem

# Extract private un-encrypted key
openssl pkcs8 -topk8 -inform PEM -outform PEM -nocrypt -in keypair.key -out pkcs8.key

# Generate encrypted keys for testing
# PBES 1
openssl pkcs8 -topk8 -in pkcs8.key -v1 PBE-MD5-DES      -out PBEWithMD5AndDES.key      -passout 'pass:neo4j'
openssl pkcs8 -topk8 -in pkcs8.key -v1 PBE-SHA1-3DES    -out PBEWithSHA1AndDESede.key  -passout 'pass:neo4j'
openssl pkcs8 -topk8 -in pkcs8.key -v1 PBE-SHA1-RC2-40  -out PBEWithSHA1AndRC2_40.key  -passout 'pass:neo4j'
openssl pkcs8 -topk8 -in pkcs8.key -v1 PBE-SHA1-RC2-128 -out PBEWithSHA1AndRC2_128.key -passout 'pass:neo4j'
openssl pkcs8 -topk8 -in pkcs8.key -v1 PBE-SHA1-RC4-40  -out PBEWithSHA1AndRC4_40.key  -passout 'pass:neo4j'
openssl pkcs8 -topk8 -in pkcs8.key -v1 PBE-SHA1-RC4-128 -out PBEWithSHA1AndRC4_128.key -passout 'pass:neo4j'
openssl pkcs8 -topk8 -in pkcs8.key -v1 PBE-SHA1-2DES    -out PBEWithSHA1And2DES.key    -passout 'pass:neo4j'

# PBES 2
openssl pkcs8 -topk8 -in pkcs8.key -v2 aes-128-cbc -v2prf hmacWithSHA1   -out PBEWithHmacSHA1AndAES_128.key   -passout 'pass:neo4j'
openssl pkcs8 -topk8 -in pkcs8.key -v2 aes-128-cbc -v2prf hmacWithSHA224 -out PBEWithHmacSHA224AndAES_128.key -passout 'pass:neo4j'
openssl pkcs8 -topk8 -in pkcs8.key -v2 aes-128-cbc -v2prf hmacWithSHA256 -out PBEWithHmacSHA256AndAES_128.key -passout 'pass:neo4j'
openssl pkcs8 -topk8 -in pkcs8.key -v2 aes-128-cbc -v2prf hmacWithSHA384 -out PBEWithHmacSHA384AndAES_128.key -passout 'pass:neo4j'
openssl pkcs8 -topk8 -in pkcs8.key -v2 aes-128-cbc -v2prf hmacWithSHA512 -out PBEWithHmacSHA512AndAES_128.key -passout 'pass:neo4j'
openssl pkcs8 -topk8 -in pkcs8.key -v2 aes-256-cbc -v2prf hmacWithSHA1   -out PBEWithHmacSHA1AndAES_256.key   -passout 'pass:neo4j'
openssl pkcs8 -topk8 -in pkcs8.key -v2 aes-256-cbc -v2prf hmacWithSHA224 -out PBEWithHmacSHA224AndAES_256.key -passout 'pass:neo4j'
openssl pkcs8 -topk8 -in pkcs8.key -v2 aes-256-cbc -v2prf hmacWithSHA256 -out PBEWithHmacSHA256AndAES_256.key -passout 'pass:neo4j'
openssl pkcs8 -topk8 -in pkcs8.key -v2 aes-256-cbc -v2prf hmacWithSHA384 -out PBEWithHmacSHA384AndAES_256.key -passout 'pass:neo4j'
openssl pkcs8 -topk8 -in pkcs8.key -v2 aes-256-cbc -v2prf hmacWithSHA512 -out PBEWithHmacSHA512AndAES_256.key -passout 'pass:neo4j'


# Unsupported
openssl pkcs8 -topk8 -in pkcs8.key -v2 des3 -v2prf hmacWithSHA512 -out PBEWithHmacSHA1AndDESede.key -passout 'pass:neo4j'
openssl pkcs8 -topk8 -in pkcs8.key -v1 PBE-SHA1-RC2-64 -out PBEWithSHA1AndRC2_64.key -passout 'pass:neo4j'
openssl pkcs8 -topk8 -in pkcs8.key -v1 PBE-MD5-RC2-64  -out PBEWithMD5AndRC2_64.key  -passout 'pass:neo4j'
openssl pkcs8 -topk8 -in pkcs8.key -v1 PBE-SHA1-DES    -out PBEWithSHA1AndDES.key    -passout 'pass:neo4j'
