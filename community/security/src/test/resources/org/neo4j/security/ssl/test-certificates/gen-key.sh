#!/bin/bash

subj="/C=SE/ST=Malmo/L=Malmo/O=neo4j/OU=dev/CN=${1}"

mkdir $1

openssl req -x509 -sha256 -nodes -days 36500 -newkey rsa:2048 -keyout $1/private.key1 -out $1/selfsigned.crt -subj $subj
openssl pkcs8 -topk8 -nocrypt -in $1/private.key1 -out $1/private.key
rm $1/private.key1

openssl req -new -key $1/private.key -out $1/public.csr -subj $subj
openssl x509 -req -in $1/public.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out $1/casigned.crt -days 36500 -sha256
rm $1/public.csr

cat ca.crt >> $1/casigned.crt

openssl req -new -key $1/private.key -out $1/public.csr -subj $subj
openssl x509 -req -in $1/public.csr -CA root.crt -CAkey root.key -CAcreateserial -out $1/rootsigned.crt -days 36500 -sha256
rm $1/public.csr
