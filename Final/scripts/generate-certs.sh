#!/bin/bash


mkdir -p ./certs ./kafka-1st-1 ./kafka-1st-2 ./kafka-1st-3 ./kafka-controller-1st-1 ./kafka-controller-1st-2 ./kafka-controller-1st-3 ./kafka-2nd-1 ./kafka-2nd-2 ./kafka-controller-2nd-3 ./kafka-controller-2nd-1 ./kafka-controller-2nd-2 ./kafka-2nd-3 ./kafka-creds

# Файл конфигурации для корневого сертификата (Root CA)
cat > ./certs/ca.cnf << EOF2
[ policy_match ]
countryName = match
stateOrProvinceName = match
organizationName = match
organizationalUnitName = optional
commonName = supplied
emailAddress = optional

[ req ]
prompt = no
distinguished_name = dn
default_md = sha256
default_bits = 4096
x509_extensions = v3_ca

[ dn ]
countryName = RU
organizationName = Yandex
organizationalUnitName = Practice
localityName = Moscow
commonName = yandex-practice-kafka-ca

[ v3_ca ]
subjectKeyIdentifier = hash
basicConstraints = critical,CA:true
authorityKeyIdentifier = keyid:always,issuer:always
keyUsage = critical,keyCertSign,cRLSign
EOF2

# Создадим корневой сертификат (Root CA)
openssl req -new -nodes -x509 \
  -days 3650 \
  -newkey rsa:2048 \
  -keyout ./certs/ca.key \
  -out ./certs/ca.crt \
  -config ./certs/ca.cnf

#  Создадим файл для хранения сертификата безопасности
cat ./certs/ca.crt ./certs/ca.key > ./certs/ca.pem

# Файлы конфигурации для каждого брокера основного кластера
cat > ./kafka-1st-1/kafka-1.cnf << EOF2
[req]
prompt = no
distinguished_name = dn
default_md = sha256
default_bits = 4096
req_extensions = v3_req

[ dn ]
countryName = RU
organizationName = Yandex
organizationalUnitName = Practice
localityName = Moscow
commonName = kafka-1

[ v3_ca ]
subjectKeyIdentifier = hash
basicConstraints = critical,CA:true
authorityKeyIdentifier = keyid:always,issuer:always
keyUsage = critical,keyCertSign,cRLSign

[ v3_req ]
subjectKeyIdentifier = hash
basicConstraints = CA:FALSE
nsComment = "OpenSSL Generated Certificate"
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = localhost
DNS.2 = kafka-1st-1
DNS.3 = kafka-1st-2
DNS.4 = kafka-1st-3
DNS.5 = kafka-controller-1
DNS.6 = kafka-controller-2
DNS.7 = kafka-controller-3
DNS.8 = kafka-2nd-1
DNS.9 = kafka-2nd-2
DNS.10 = kafka-2nd-3
DNS.11 = kafka-controller-2nd-1
DNS.12 = kafka-controller-2nd-2
DNS.13 = kafka-controller-2nd-3
DNS.14 = schema-registry-1st
DNS.15 = schema-registry-2nd
DNS.16 = kafka-ui
IP.1 = 127.0.0.1
EOF2

cat > ./kafka-1st-2/kafka-2.cnf << EOF2
[req]
prompt = no
distinguished_name = dn
default_md = sha256
default_bits = 4096
req_extensions = v3_req

[ dn ]
countryName = RU
organizationName = Yandex
organizationalUnitName = Practice
localityName = Moscow
commonName = kafka-2

[ v3_ca ]
subjectKeyIdentifier = hash
basicConstraints = critical,CA:true
authorityKeyIdentifier = keyid:always,issuer:always
keyUsage = critical,keyCertSign,cRLSign

[ v3_req ]
subjectKeyIdentifier = hash
basicConstraints = CA:FALSE
nsComment = "OpenSSL Generated Certificate"
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = localhost
DNS.2 = kafka-1st-1
DNS.3 = kafka-1st-2
DNS.4 = kafka-1st-3
DNS.5 = kafka-controller-1
DNS.6 = kafka-controller-2
DNS.7 = kafka-controller-3
DNS.8 = kafka-2nd-1
DNS.9 = kafka-2nd-2
DNS.10 = kafka-2nd-3
DNS.11 = kafka-controller-2nd-1
DNS.12 = kafka-controller-2nd-2
DNS.13 = kafka-controller-2nd-3
DNS.14 = schema-registry-1st
DNS.15 = schema-registry-2nd
DNS.16 = kafka-ui
IP.1 = 127.0.0.1
EOF2

cat > ./kafka-1st-3/kafka-3.cnf << EOF2
[req]
prompt = no
distinguished_name = dn
default_md = sha256
default_bits = 4096
req_extensions = v3_req

[ dn ]
countryName = RU
organizationName = Yandex
organizationalUnitName = Practice
localityName = Moscow
commonName = kafka-3

[ v3_ca ]
subjectKeyIdentifier = hash
basicConstraints = critical,CA:true
authorityKeyIdentifier = keyid:always,issuer:always
keyUsage = critical,keyCertSign,cRLSign

[ v3_req ]
subjectKeyIdentifier = hash
basicConstraints = CA:FALSE
nsComment = "OpenSSL Generated Certificate"
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = localhost
DNS.2 = kafka-1st-1
DNS.3 = kafka-1st-2
DNS.4 = kafka-1st-3
DNS.5 = kafka-controller-1
DNS.6 = kafka-controller-2
DNS.7 = kafka-controller-3
DNS.8 = kafka-2nd-1
DNS.9 = kafka-2nd-2
DNS.10 = kafka-2nd-3
DNS.11 = kafka-controller-2nd-1
DNS.12 = kafka-controller-2nd-2
DNS.13 = kafka-controller-2nd-3
DNS.14 = schema-registry-1st
DNS.15 = schema-registry-2nd
DNS.16 = kafka-ui
IP.1 = 127.0.0.1
EOF2

# Файлы конфигурации для каждого котроллера основного кластера
cat > ./kafka-controller-1st-1/kafka-1.cnf << EOF2
[req]
prompt = no
distinguished_name = dn
default_md = sha256
default_bits = 4096
req_extensions = v3_req

[ dn ]
countryName = RU
organizationName = Yandex
organizationalUnitName = Practice
localityName = Moscow
commonName = kafka-controller-1

[ v3_ca ]
subjectKeyIdentifier = hash
basicConstraints = critical,CA:true
authorityKeyIdentifier = keyid:always,issuer:always
keyUsage = critical,keyCertSign,cRLSign

[ v3_req ]
subjectKeyIdentifier = hash
basicConstraints = CA:FALSE
nsComment = "OpenSSL Generated Certificate"
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = localhost
DNS.2 = kafka-1st-1
DNS.3 = kafka-1st-2
DNS.4 = kafka-1st-3
DNS.5 = kafka-controller-1
DNS.6 = kafka-controller-2
DNS.7 = kafka-controller-3
DNS.8 = kafka-2nd-1
DNS.9 = kafka-2nd-2
DNS.10 = kafka-2nd-3
DNS.11 = kafka-controller-2nd-1
DNS.12 = kafka-controller-2nd-2
DNS.13 = kafka-controller-2nd-3
DNS.14 = schema-registry-1st
DNS.15 = schema-registry-2nd
DNS.16 = kafka-ui
IP.1 = 127.0.0.1
EOF2

cat > ./kafka-controller-1st-2/kafka-2.cnf << EOF2
[req]
prompt = no
distinguished_name = dn
default_md = sha256
default_bits = 4096
req_extensions = v3_req

[ dn ]
countryName = RU
organizationName = Yandex
organizationalUnitName = Practice
localityName = Moscow
commonName = kafka-controller-2

[ v3_ca ]
subjectKeyIdentifier = hash
basicConstraints = critical,CA:true
authorityKeyIdentifier = keyid:always,issuer:always
keyUsage = critical,keyCertSign,cRLSign

[ v3_req ]
subjectKeyIdentifier = hash
basicConstraints = CA:FALSE
nsComment = "OpenSSL Generated Certificate"
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = localhost
DNS.2 = kafka-1st-1
DNS.3 = kafka-1st-2
DNS.4 = kafka-1st-3
DNS.5 = kafka-controller-1
DNS.6 = kafka-controller-2
DNS.7 = kafka-controller-3
DNS.8 = kafka-2nd-1
DNS.9 = kafka-2nd-2
DNS.10 = kafka-2nd-3
DNS.11 = kafka-controller-2nd-1
DNS.12 = kafka-controller-2nd-2
DNS.13 = kafka-controller-2nd-3
DNS.14 = schema-registry-1st
DNS.15 = schema-registry-2nd
DNS.16 = kafka-ui
IP.1 = 127.0.0.1
EOF2

cat > ./kafka-controller-1st-3/kafka-3.cnf << EOF2
[req]
prompt = no
distinguished_name = dn
default_md = sha256
default_bits = 4096
req_extensions = v3_req

[ dn ]
countryName = RU
organizationName = Yandex
organizationalUnitName = Practice
localityName = Moscow
commonName = kafka-controller-3

[ v3_ca ]
subjectKeyIdentifier = hash
basicConstraints = critical,CA:true
authorityKeyIdentifier = keyid:always,issuer:always
keyUsage = critical,keyCertSign,cRLSign

[ v3_req ]
subjectKeyIdentifier = hash
basicConstraints = CA:FALSE
nsComment = "OpenSSL Generated Certificate"
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = localhost
DNS.2 = kafka-1st-1
DNS.3 = kafka-1st-2
DNS.4 = kafka-1st-3
DNS.5 = kafka-controller-1
DNS.6 = kafka-controller-2
DNS.7 = kafka-controller-3
DNS.8 = kafka-2nd-1
DNS.9 = kafka-2nd-2
DNS.10 = kafka-2nd-3
DNS.11 = kafka-controller-2nd-1
DNS.12 = kafka-controller-2nd-2
DNS.13 = kafka-controller-2nd-3
DNS.14 = schema-registry-1st
DNS.15 = schema-registry-2nd
DNS.16 = kafka-ui
IP.1 = 127.0.0.1
EOF2


# Файлы конфигурации для каждого брокера второго кластера
cat > ./kafka-2nd-1/kafka-1.cnf << EOF2
[req]
prompt = no
distinguished_name = dn
default_md = sha256
default_bits = 4096
req_extensions = v3_req

[ dn ]
countryName = RU
organizationName = Yandex
organizationalUnitName = Practice
localityName = Moscow
commonName = kafka-secondary-1

[ v3_ca ]
subjectKeyIdentifier = hash
basicConstraints = critical,CA:true
authorityKeyIdentifier = keyid:always,issuer:always
keyUsage = critical,keyCertSign,cRLSign

[ v3_req ]
subjectKeyIdentifier = hash
basicConstraints = CA:FALSE
nsComment = "OpenSSL Generated Certificate"
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = localhost
DNS.2 = kafka-1st-1
DNS.3 = kafka-1st-2
DNS.4 = kafka-1st-3
DNS.5 = kafka-controller-1
DNS.6 = kafka-controller-2
DNS.7 = kafka-controller-3
DNS.8 = kafka-2nd-1
DNS.9 = kafka-2nd-2
DNS.10 = kafka-2nd-3
DNS.11 = kafka-controller-2nd-1
DNS.12 = kafka-controller-2nd-2
DNS.13 = kafka-controller-2nd-3
DNS.14 = schema-registry-1st
DNS.15 = schema-registry-2nd
DNS.16 = kafka-ui
IP.1 = 127.0.0.1
EOF2

cat > ./kafka-2nd-2/kafka-2.cnf << EOF2
[req]
prompt = no
distinguished_name = dn
default_md = sha256
default_bits = 4096
req_extensions = v3_req

[ dn ]
countryName = RU
organizationName = Yandex
organizationalUnitName = Practice
localityName = Moscow
commonName = kafka-secondary-2

[ v3_ca ]
subjectKeyIdentifier = hash
basicConstraints = critical,CA:true
authorityKeyIdentifier = keyid:always,issuer:always
keyUsage = critical,keyCertSign,cRLSign

[ v3_req ]
subjectKeyIdentifier = hash
basicConstraints = CA:FALSE
nsComment = "OpenSSL Generated Certificate"
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = localhost
DNS.2 = kafka-1st-1
DNS.3 = kafka-1st-2
DNS.4 = kafka-1st-3
DNS.5 = kafka-controller-1
DNS.6 = kafka-controller-2
DNS.7 = kafka-controller-3
DNS.8 = kafka-2nd-1
DNS.9 = kafka-2nd-2
DNS.10 = kafka-2nd-3
DNS.11 = kafka-controller-2nd-1
DNS.12 = kafka-controller-2nd-2
DNS.13 = kafka-controller-2nd-3
DNS.14 = schema-registry-1st
DNS.15 = schema-registry-2nd
DNS.16 = kafka-ui
IP.1 = 127.0.0.1
EOF2

cat > ./kafka-2nd-3/kafka-3.cnf << EOF2
[req]
prompt = no
distinguished_name = dn
default_md = sha256
default_bits = 4096
req_extensions = v3_req

[ dn ]
countryName = RU
organizationName = Yandex
organizationalUnitName = Practice
localityName = Moscow
commonName = kafka-secondary-3

[ v3_ca ]
subjectKeyIdentifier = hash
basicConstraints = critical,CA:true
authorityKeyIdentifier = keyid:always,issuer:always
keyUsage = critical,keyCertSign,cRLSign

[ v3_req ]
subjectKeyIdentifier = hash
basicConstraints = CA:FALSE
nsComment = "OpenSSL Generated Certificate"
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = localhost
DNS.2 = kafka-1st-1
DNS.3 = kafka-1st-2
DNS.4 = kafka-1st-3
DNS.5 = kafka-controller-1
DNS.6 = kafka-controller-2
DNS.7 = kafka-controller-3
DNS.8 = kafka-2nd-1
DNS.9 = kafka-2nd-2
DNS.10 = kafka-2nd-3
DNS.11 = kafka-controller-2nd-1
DNS.12 = kafka-controller-2nd-2
DNS.13 = kafka-controller-2nd-3
DNS.14 = schema-registry-1st
DNS.15 = schema-registry-2nd
DNS.16 = kafka-ui
IP.1 = 127.0.0.1
EOF2

# Файлы конфигурации для каждого котроллера второго кластера
cat > ./kafka-controller-2nd-1/kafka-1.cnf << EOF2
[req]
prompt = no
distinguished_name = dn
default_md = sha256
default_bits = 4096
req_extensions = v3_req

[ dn ]
countryName = RU
organizationName = Yandex
organizationalUnitName = Practice
localityName = Moscow
commonName = kafka-controller-2nd-1

[ v3_ca ]
subjectKeyIdentifier = hash
basicConstraints = critical,CA:true
authorityKeyIdentifier = keyid:always,issuer:always
keyUsage = critical,keyCertSign,cRLSign

[ v3_req ]
subjectKeyIdentifier = hash
basicConstraints = CA:FALSE
nsComment = "OpenSSL Generated Certificate"
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = localhost
DNS.2 = kafka-1st-1
DNS.3 = kafka-1st-2
DNS.4 = kafka-1st-3
DNS.5 = kafka-controller-1
DNS.6 = kafka-controller-2
DNS.7 = kafka-controller-3
DNS.8 = kafka-2nd-1
DNS.9 = kafka-2nd-2
DNS.10 = kafka-2nd-3
DNS.11 = kafka-controller-2nd-1
DNS.12 = kafka-controller-2nd-2
DNS.13 = kafka-controller-2nd-3
DNS.14 = schema-registry-1st
DNS.15 = schema-registry-2nd
DNS.16 = kafka-ui
IP.1 = 127.0.0.1
EOF2

cat > ./kafka-controller-2nd-2/kafka-2.cnf << EOF2
[req]
prompt = no
distinguished_name = dn
default_md = sha256
default_bits = 4096
req_extensions = v3_req

[ dn ]
countryName = RU
organizationName = Yandex
organizationalUnitName = Practice
localityName = Moscow
commonName = kafka-controller-2nd-2

[ v3_ca ]
subjectKeyIdentifier = hash
basicConstraints = critical,CA:true
authorityKeyIdentifier = keyid:always,issuer:always
keyUsage = critical,keyCertSign,cRLSign

[ v3_req ]
subjectKeyIdentifier = hash
basicConstraints = CA:FALSE
nsComment = "OpenSSL Generated Certificate"
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = localhost
DNS.2 = kafka-1st-1
DNS.3 = kafka-1st-2
DNS.4 = kafka-1st-3
DNS.5 = kafka-controller-1
DNS.6 = kafka-controller-2
DNS.7 = kafka-controller-3
DNS.8 = kafka-2nd-1
DNS.9 = kafka-2nd-2
DNS.10 = kafka-2nd-3
DNS.11 = kafka-controller-2nd-1
DNS.12 = kafka-controller-2nd-2
DNS.13 = kafka-controller-2nd-3
DNS.14 = schema-registry-1st
DNS.15 = schema-registry-2nd
DNS.16 = kafka-ui
IP.1 = 127.0.0.1
EOF2

cat > ./kafka-controller-2nd-3/kafka-3.cnf << EOF2
[req]
prompt = no
distinguished_name = dn
default_md = sha256
default_bits = 4096
req_extensions = v3_req

[ dn ]
countryName = RU
organizationName = Yandex
organizationalUnitName = Practice
localityName = Moscow
commonName = kafka-controller-2nd-3

[ v3_ca ]
subjectKeyIdentifier = hash
basicConstraints = critical,CA:true
authorityKeyIdentifier = keyid:always,issuer:always
keyUsage = critical,keyCertSign,cRLSign

[ v3_req ]
subjectKeyIdentifier = hash
basicConstraints = CA:FALSE
nsComment = "OpenSSL Generated Certificate"
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = localhost
DNS.2 = kafka-1st-1
DNS.3 = kafka-1st-2
DNS.4 = kafka-1st-3
DNS.5 = kafka-controller-1
DNS.6 = kafka-controller-2
DNS.7 = kafka-controller-3
DNS.8 = kafka-2nd-1
DNS.9 = kafka-2nd-2
DNS.10 = kafka-2nd-3
DNS.11 = kafka-controller-2nd-1
DNS.12 = kafka-controller-2nd-2
DNS.13 = kafka-controller-2nd-3
DNS.14 = schema-registry-1st
DNS.15 = schema-registry-2nd
DNS.16 = kafka-ui
IP.1 = 127.0.0.1
EOF2

cat > ./client.properties << EOF2
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="admin" \
  password="admin-secret";
ssl.truststore.location=/etc/kafka/secrets/kafka.truststore.jks
ssl.truststore.password=final-work-pass

EOF2

# Генерируем сертификаты для каждого брокера основного кластера
for i in 1 2 3; do
  echo "Генерация сертификатов для kafka-$i"

  # Создаем приватный ключ и запрос на сертификат (CSR)
  openssl req -new \
      -newkey rsa:2048 \
      -keyout ./kafka-1st-$i/kafka-$i.key \
      -out ./kafka-1st-$i/kafka-$i.csr \
      -config ./kafka-1st-$i/kafka-$i.cnf \
      -nodes


  # Создаем сертификат брокера, подписанный CA
  openssl x509 -req \
    -days 3650 \
    -in ./kafka-1st-$i/kafka-$i.csr \
    -CA ./certs/ca.crt \
    -CAkey ./certs/ca.key \
    -CAcreateserial \
    -out ./kafka-1st-$i/kafka-$i.crt \
    -extfile ./kafka-1st-$i/kafka-$i.cnf \
    -extensions v3_req

  # Создаем PEM файл с цепочкой сертификатов
  cat ./kafka-1st-$i/kafka-$i.crt ./certs/ca.pem > ./kafka-1st-$i/kafka-$i-chain.pem

  # Создаем PKCS12 хранилище с цепочкой сертификатов
  openssl pkcs12 -export \
    -in ./kafka-1st-$i/kafka-$i-chain.pem \
    -inkey ./kafka-1st-$i/kafka-$i.key \
    -name kafka-$i \
    -out ./kafka-1st-$i/kafka-$i.p12 \
    -password pass:final-work-pass

  # Создаем keystore для Kafka
  keytool -importkeystore \
    -deststorepass final-work-pass \
    -destkeystore ./kafka-1st-$i/kafka.keystore.pkcs12 \
    -srckeystore ./kafka-1st-$i/kafka-$i.p12 \
    -deststoretype PKCS12  \
    -srcstoretype PKCS12 \
    -noprompt \
    -srcstorepass final-work-pass

  # Создаем truststore для Kafka с полной цепочкой сертификатов
  keytool -import \
    -file ./certs/ca.crt \
    -alias ca \
    -keystore ./kafka-1st-$i/kafka.truststore.jks \
    -storepass final-work-pass \
    -noprompt

  mkdir -p ../ca/kafka-1st-$i
  cp ./kafka-1st-$i/kafka.keystore.pkcs12 ../ca/kafka-1st-$i/
  cp ./kafka-1st-$i/kafka.truststore.jks ../ca/kafka-1st-$i/
  cp ./client.properties ../ca/kafka-1st-$i/
  cp ./kafka_server_jaas.conf ../ca/kafka-1st-$i/
  cp ./certs/ca.crt ../ca/kafka-1st-$i/
  cp ./certs/ca.pem ../ca/kafka-1st-$i/
  
done

# Генерируем сертификаты для каждого контроллера основного кластера
for i in 1 2 3; do
  echo "Генерация сертификатов для kafka-$i"

  # Создаем приватный ключ и запрос на сертификат (CSR)
  openssl req -new \
      -newkey rsa:2048 \
      -keyout ./kafka-controller-1st-$i/kafka-$i.key \
      -out ./kafka-controller-1st-$i/kafka-$i.csr \
      -config ./kafka-controller-1st-$i/kafka-$i.cnf \
      -nodes


  # Создаем сертификат контроллера, подписанный CA
  openssl x509 -req \
    -days 3650 \
    -in ./kafka-controller-1st-$i/kafka-$i.csr \
    -CA ./certs/ca.crt \
    -CAkey ./certs/ca.key \
    -CAcreateserial \
    -out ./kafka-controller-1st-$i/kafka-$i.crt \
    -extfile ./kafka-controller-1st-$i/kafka-$i.cnf \
    -extensions v3_req

  # Создаем PEM файл с цепочкой сертификатов
  cat ./kafka-controller-1st-$i/kafka-$i.crt ./certs/ca.pem > ./kafka-controller-1st-$i/kafka-$i-chain.pem

  # Создаем PKCS12 хранилище с цепочкой сертификатов
  openssl pkcs12 -export \
    -in ./kafka-controller-1st-$i/kafka-$i-chain.pem \
    -inkey ./kafka-controller-1st-$i/kafka-$i.key \
    -name kafka-$i \
    -out ./kafka-controller-1st-$i/kafka-$i.p12 \
    -password pass:final-work-pass

  # Создаем keystore для Kafka
  keytool -importkeystore \
    -deststorepass final-work-pass \
    -destkeystore ./kafka-controller-1st-$i/kafka.keystore.pkcs12 \
    -srckeystore ./kafka-controller-1st-$i/kafka-$i.p12 \
    -deststoretype PKCS12  \
    -srcstoretype PKCS12 \
    -noprompt \
    -srcstorepass final-work-pass

  # Создаем truststore для Kafka с полной цепочкой сертификатов
  keytool -import \
    -file ./certs/ca.crt \
    -alias ca \
    -keystore ./kafka-controller-1st-$i/kafka.truststore.jks \
    -storepass final-work-pass \
    -noprompt

  mkdir -p ../ca/kafka-controller-1st-$i
  cp ./kafka-controller-1st-$i/kafka.keystore.pkcs12 ../ca/kafka-controller-1st-$i/
  cp ./kafka-controller-1st-$i/kafka.truststore.jks ../ca/kafka-controller-1st-$i/
  cp ./kafka_server_jaas.conf ../ca/kafka-controller-1st-$i/
  cp ./certs/ca.crt ../ca/kafka-controller-1st-$i/
  cp ./certs/ca.pem ../ca/kafka-controller-1st-$i/
  
done

# Генерируем сертификаты для каждого брокера второго кластера
for i in 1 2 3; do
  echo "Генерация сертификатов для kafka-2nd-$i"

  # Создаем приватный ключ и запрос на сертификат (CSR)
  openssl req -new \
      -newkey rsa:2048 \
      -keyout ./kafka-2nd-$i/kafka-$i.key \
      -out ./kafka-2nd-$i/kafka-$i.csr \
      -config ./kafka-2nd-$i/kafka-$i.cnf \
      -nodes


  # Создаем сертификат брокера, подписанный CA
  openssl x509 -req \
    -days 3650 \
    -in ./kafka-2nd-$i/kafka-$i.csr \
    -CA ./certs/ca.crt \
    -CAkey ./certs/ca.key \
    -CAcreateserial \
    -out ./kafka-2nd-$i/kafka-$i.crt \
    -extfile ./kafka-2nd-$i/kafka-$i.cnf \
    -extensions v3_req

  # Создаем PEM файл с цепочкой сертификатов
  cat ./kafka-2nd-$i/kafka-$i.crt ./certs/ca.pem > ./kafka-2nd-$i/kafka-$i-chain.pem

  # Создаем PKCS12 хранилище с цепочкой сертификатов
  openssl pkcs12 -export \
    -in ./kafka-2nd-$i/kafka-$i-chain.pem \
    -inkey ./kafka-2nd-$i/kafka-$i.key \
    -name kafka-$i \
    -out ./kafka-2nd-$i/kafka-$i.p12 \
    -password pass:final-work-pass

  # Создаем keystore для Kafka
  keytool -importkeystore \
    -deststorepass final-work-pass \
    -destkeystore ./kafka-2nd-$i/kafka.keystore.pkcs12 \
    -srckeystore ./kafka-2nd-$i/kafka-$i.p12 \
    -deststoretype PKCS12  \
    -srcstoretype PKCS12 \
    -noprompt \
    -srcstorepass final-work-pass

  # Создаем truststore для Kafka с полной цепочкой сертификатов
  keytool -import \
    -file ./certs/ca.crt \
    -alias ca \
    -keystore ./kafka-2nd-$i/kafka.truststore.jks \
    -storepass final-work-pass \
    -noprompt

  mkdir -p ../ca/kafka-2nd-$i
  cp ./kafka-2nd-$i/kafka.keystore.pkcs12 ../ca/kafka-2nd-$i/
  cp ./kafka-2nd-$i/kafka.truststore.jks ../ca/kafka-2nd-$i/
  cp ./client.properties ../ca/kafka-2nd-$i/
  cp ./kafka_server_jaas.conf ../ca/kafka-2nd-$i/
  cp ./certs/ca.crt ../ca/kafka-2nd-$i/
  cp ./certs/ca.pem ../ca/kafka-2nd-$i/
done

# Генерируем сертификаты для каждого контроллера второго кластера
for i in 1 2 3; do
  echo "Генерация сертификатов для kafka-$i"

  # Создаем приватный ключ и запрос на сертификат (CSR)
  openssl req -new \
      -newkey rsa:2048 \
      -keyout ./kafka-controller-2nd-$i/kafka-$i.key \
      -out ./kafka-controller-2nd-$i/kafka-$i.csr \
      -config ./kafka-controller-2nd-$i/kafka-$i.cnf \
      -nodes


  # Создаем сертификат контроллера, подписанный CA
  openssl x509 -req \
    -days 3650 \
    -in ./kafka-controller-2nd-$i/kafka-$i.csr \
    -CA ./certs/ca.crt \
    -CAkey ./certs/ca.key \
    -CAcreateserial \
    -out ./kafka-controller-2nd-$i/kafka-$i.crt \
    -extfile ./kafka-controller-2nd-$i/kafka-$i.cnf \
    -extensions v3_req

  # Создаем PEM файл с цепочкой сертификатов
  cat ./kafka-controller-2nd-$i/kafka-$i.crt ./certs/ca.pem > ./kafka-controller-2nd-$i/kafka-$i-chain.pem

  # Создаем PKCS12 хранилище с цепочкой сертификатов
  openssl pkcs12 -export \
    -in ./kafka-controller-2nd-$i/kafka-$i-chain.pem \
    -inkey ./kafka-controller-2nd-$i/kafka-$i.key \
    -name kafka-$i \
    -out ./kafka-controller-2nd-$i/kafka-$i.p12 \
    -password pass:final-work-pass

  # Создаем keystore для Kafka
  keytool -importkeystore \
    -deststorepass final-work-pass \
    -destkeystore ./kafka-controller-2nd-$i/kafka.keystore.pkcs12 \
    -srckeystore ./kafka-controller-2nd-$i/kafka-$i.p12 \
    -deststoretype PKCS12  \
    -srcstoretype PKCS12 \
    -noprompt \
    -srcstorepass final-work-pass

  # Создаем truststore для Kafka с полной цепочкой сертификатов
  keytool -import \
    -file ./certs/ca.crt \
    -alias ca \
    -keystore ./kafka-controller-2nd-$i/kafka.truststore.jks \
    -storepass final-work-pass \
    -noprompt

  mkdir -p ../ca/kafka-controller-2nd-$i
  cp ./kafka-controller-2nd-$i/kafka.keystore.pkcs12 ../ca/kafka-controller-2nd-$i/
  cp ./kafka-controller-2nd-$i/kafka.truststore.jks ../ca/kafka-controller-2nd-$i/
  cp ./kafka_server_jaas.conf ../ca/kafka-controller-2nd-$i/
  cp ./certs/ca.crt ../ca/kafka-controller-2nd-$i/
  cp ./certs/ca.pem ../ca/kafka-controller-2nd-$i/
  
done

mkdir -p ../ca/certs
cp ./certs/ca.crt ../ca/certs/

echo "Сертификаты успешно созданы для обоих кластеров"