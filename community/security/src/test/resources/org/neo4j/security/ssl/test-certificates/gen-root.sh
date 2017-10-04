#!/bin/bash

subj="/C=SE/ST=Malmo/L=Malmo/O=neo4j/OU=Root/CN=Root"

openssl req -x509 -sha256 -nodes -days 36500 -newkey rsa:2048 -keyout root.key1 -out root.crt -subj $subj
openssl pkcs8 -topk8 -nocrypt -in root.key1 -out root.key
rm root.key1
