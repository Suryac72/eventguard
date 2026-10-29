
#!/bin/sh
set -eu

if [ -n "${KAFKA_CA_CERT_BASE64:-}" ]; then
    echo "$KAFKA_CA_CERT_BASE64" | base64 -d > /tmp/eventguard-ca.pem

    keytool -importcert \
        -noprompt \
        -alias aiven-ca \
        -file /tmp/eventguard-ca.pem \
        -keystore /tmp/kafka-truststore.p12 \
        -storetype PKCS12 \
        -storepass "${KAFKA_SSL_TRUSTSTORE_PASSWORD:?Set KAFKA_SSL_TRUSTSTORE_PASSWORD}" 

    export KAFKA_SSL_TRUSTSTORE_LOCATION=/tmp/kafka-truststore.p12
    export KAFKA_SSL_TRUSTSTORE_TYPE=PKCS12
fi

exec java -jar /app/eventguard.jar
