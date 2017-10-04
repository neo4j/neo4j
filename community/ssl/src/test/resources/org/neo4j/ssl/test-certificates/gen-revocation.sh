#!/bin/bash

# Generating revocations is a bit messy because they are usually cumulative.
# After the generation of each CRL for 1 server we restore the database.

openssl ca -config cluster.conf -revoke ca/cluster/$1.pem -crl_reason unspecified
openssl ca -gencrl -config cluster.conf -out servers/$2/revoked.crl

# Restore back to previous state.
mv ca/cluster/db/cluster.db.old ca/cluster/db/cluster.db
