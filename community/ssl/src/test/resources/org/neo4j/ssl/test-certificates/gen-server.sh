#!/bin/bash

# The purpose of this script is to generate keys and various signed artifacts for a server.

rm -rf servers/$1
mkdir -p servers/$1

subj="/C=SE/O=Example/OU=Example Cluster/CN=Server ${1}"

#openssl req -new -out servers/$1/$1.csr -keyout servers/$1/private.key1 -nodes -config server.conf -subj "${subj}"
openssl req -x509 -sha1 -nodes -days 2000 -newkey rsa:2048 \
    -config server.conf -keyout servers/$1/private.key1 -out servers/$1/selfsigned.crt -subj "$subj"

openssl pkcs8 -topk8 -nocrypt -in servers/$1/private.key1 -out servers/$1/private.key
rm servers/$1/private.key1

openssl req -new -key servers/$1/private.key -out servers/$1/casigned.csr -config server.conf -subj "${subj}"
#openssl req -noout -text -in servers/$1/casigned.csr

openssl ca -batch -config cluster.conf -in servers/$1/casigned.csr -out servers/$1/casigned.cert -extensions server_ext
#openssl x509 -noout -text -in servers/$1/casigned.crt

# Fix-up for broken parsers which can't handle headers
awk '/-----/{i++}i' servers/$1/casigned.cert > servers/$1/casigned.crt
