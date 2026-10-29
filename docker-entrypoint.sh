
#!/bin/sh
set -eu

if [ -n "${KAFKA_CA_CERT_BASE64:-}" ]; then
    : "${KAFKA_SSL_TRUSTSTORE_PASSWORD:?Truststore password is required}"

    CA_FILE="/tmp/eventguard-ca.pem"
    TRUSTSTORE="/tmp/kafka-truststore.p12"

    printf '%s' "$KAFKA_CA_CERT_BASE64" | base64 -d > "$CA_FILE"

    keytool -importcert \
        -noprompt \
        -alias aiven-kafka-ca \
        -file "$CA_FILE" \
        -keystore "$TRUSTSTORE" \
        -storetype PKCS12 \
        -storepass "$KAFKA_SSL_TRUSTSTORE_PASSWORD"

    export KAFKA_SSL_TRUSTSTORE_LOCATION="$TRUSTSTORE"
    export KAFKA_SSL_TRUSTSTORE_TYPE=PKCS12

    rm -f "$CA_FILE"
fi

exec java -jar /app/eventguard.jar
