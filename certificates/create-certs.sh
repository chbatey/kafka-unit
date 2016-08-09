#!/bin/bash

PASSWORD=test1234
CERTIFICATE_VALIDITY_DAYS=365

CA_KEY_FILE=tmp/ca.key
CA_CERT_FILE=tmp/ca.crt
CA_ALIAS=CARoot

SERVER_KEYSTORE_FILE=out/server.keystore.jks
SERVER_TRUSTSTORE_FILE=out/server.truststore.jks
SERVER_CSR_FILE=tmp/server.csr
SERVER_CERT_FILE=tmp/server.crt
SERVER_FQDN=localhost

CLIENT_KEYSTORE_FILE=out/client.keystore.jks
CLIENT_TRUSTSTORE_FILE=out/client.truststore.jks
CLIENT_CSR_FILE=tmp/client.csr
CLIENT_CERT_FILE=tmp/client.crt
CLIENT_FQDN=localhost

set -e

mkdir out
mkdir tmp

echo "********************************************************************"
echo "Use '$PASSWORD' as the password in every password prompt below."
echo "Other than the password, leave all fields with their default values."
echo "Answer 'yes' when prompted about trusting certificates."
echo "********************************************************************"
echo

echo "Creating server and client key stores"
keytool -keystore $SERVER_KEYSTORE_FILE -alias $SERVER_FQDN -validity $CERTIFICATE_VALIDITY_DAYS -keyalg RSA -genkey
keytool -keystore $CLIENT_KEYSTORE_FILE -alias $CLIENT_FQDN -validity $CERTIFICATE_VALIDITY_DAYS -keyalg RSA -genkey

echo "Creating CA certificate"
openssl req -new -x509 -keyout $CA_KEY_FILE -out $CA_CERT_FILE -days $CERTIFICATE_VALIDITY_DAYS

echo "Importing CA certificate into the server and client trust stores"
keytool -keystore $SERVER_TRUSTSTORE_FILE -alias $CA_ALIAS -import -file $CA_CERT_FILE
keytool -keystore $CLIENT_TRUSTSTORE_FILE -alias $CA_ALIAS -import -file $CA_CERT_FILE

echo "Creating CSR for server"
keytool -keystore $SERVER_KEYSTORE_FILE -alias $SERVER_FQDN -certreq -file $SERVER_CSR_FILE

echo "Signing server CSR"
openssl x509 -req -CA $CA_CERT_FILE -CAkey $CA_KEY_FILE -in $SERVER_CSR_FILE -out $SERVER_CERT_FILE -days $CERTIFICATE_VALIDITY_DAYS -CAcreateserial -passin pass:$PASSWORD

echo "Importing CA and server certificates into the server key store"
keytool -keystore $SERVER_KEYSTORE_FILE -alias $CA_ALIAS -import -file $CA_CERT_FILE
keytool -keystore $SERVER_KEYSTORE_FILE -alias $SERVER_FQDN -import -file $SERVER_CERT_FILE

echo "Creating client CSR"
keytool -keystore $CLIENT_KEYSTORE_FILE -alias $CLIENT_FQDN -certreq -file $CLIENT_CSR_FILE

echo "Signing client CSR"
openssl x509 -req -CA $CA_CERT_FILE -CAkey $CA_KEY_FILE -in $CLIENT_CSR_FILE -out $CLIENT_CERT_FILE -days $CERTIFICATE_VALIDITY_DAYS -CAcreateserial -passin pass:$PASSWORD

echo "Importing CA and client certificates into the client key store"
keytool -keystore $CLIENT_KEYSTORE_FILE -alias $CA_ALIAS -import -file $CA_CERT_FILE
keytool -keystore $CLIENT_KEYSTORE_FILE -alias $CLIENT_FQDN -import -file $CLIENT_CERT_FILE

rm tmp/*
rmdir tmp
