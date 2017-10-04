#!/bin/bash

# The purpose of this script is to setup a Root CA and an intermediate Cluster CA.

##### ROOT #####

# Root base setup
mkdir -p ca/root/private
mkdir ca/root/db

touch ca/root/db/root.db
touch ca/root/db/root.db.attr

echo 01 > ca/root/db/root.crt.srl
echo 01 > ca/root/db/root.crl.srl

# Key Generation and Certificate Signing Request (CSR) for Root
openssl req -new -config root.conf -out ca/root.csr -keyout ca/root/private/root.key

# Self-signing of Root Certificate for ~100 Years
openssl ca -batch -selfsign -config root.conf -in ca/root.csr -out ca/root.cert -extensions root_ca_ext -days 36500

# Fix-up for broken parsers which can't handle headers
awk '/-----/{i++}i' ca/root.cert > ca/root.crt

##### CLUSTER #####

# Cluster base setup
mkdir -p ca/cluster/private
mkdir ca/cluster/db

touch ca/cluster/db/cluster.db
touch ca/cluster/db/cluster.db.attr

echo 01 > ca/cluster/db/cluster.crt.srl
echo 01 > ca/cluster/db/cluster.crl.srl

# Key Generation and Certificate Signing Request (CSR) for Cluster
openssl req -new -config cluster.conf -out ca/cluster.csr -keyout ca/cluster/private/cluster.key

# Root-signing of Cluster Certificate for ~10 Years
openssl ca -batch -config root.conf -in ca/cluster.csr -out ca/cluster.cert -extensions signing_ca_ext -days 3650

# Fix-up for broken parsers which can't handle headers
awk '/-----/{i++}i' ca/cluster.cert > ca/cluster.crt
