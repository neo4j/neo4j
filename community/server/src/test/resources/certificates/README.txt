this directory contains a mini CA based on openssl's CA.sh script
for info how to create chained certificates: http://www.ibm.com/developerworks/library/j-certgen/

notes:
* CA certificate is not password protected, also private keys for certificates are not protected.
* TestKeyStoreFactory.java uses chained_key.der and combined.pem
* to generate chained_key.der and combined.pem the following steps have been taken:

1) setup CA using openssl's CA.sh script, see URL above
2) create a chained certificate as described in URL above
3) convert key from chained certificate to DER format:
openssl pkcs8 -in chained_key.pem -nocrypt -out chained_key.der -outform DER
4) copy certificate from intermediate cert and chained cert to combined.pem

certificate directly signed by CA:
direct_cert.pem
direct_req.pem

intermediate CA certificate signed by CA:
intermediate_cert.pem
intermediate_req.pem

chained certificate signed by intermediate CA:
chained_cert.pem
chained_req.pem

intermediate_cert.pem and chained_cert.pem are copied to combined.pem
