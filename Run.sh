#! /bin/sh

# wget https://github.com/ydb-platform/ydb-jdbc-driver/releases/download/v2.2.1/ydb-jdbc-driver-shaded-2.2.1.jar
# wget https://mycop1.website.yandexcloud.net/temp/ydb-decimal-test-1.0-SNAPSHOT.jar

# ydb yql -s 'DROP TABLE dectest1'

# YDB_CONNECTION_STRING=grpcs://lb.etnphmvaf3ue1ub1srr6.ydb.mdb.yandexcloud.net:2135/ru-central1/b1g8skpblkos03malf3s/etnphmvaf3ue1ub1srr6
# YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=/Users/mzinal/Downloads/mzinal_sa.json

YDB_CONNECTION_STRING=grpcs://zeit-1:2135/Domain0/zodak
export YDB_CONNECTION_STRING

YDB_USER='ydb-admin1@ldap'
export YDB_USER

YDB_PASSWORD='P@$$w0rd+'
export YDB_PASSWORD

YDB_SSL_ROOT_CERTIFICATES_FILE=/home/ubuntu/oldinstall/TLS/ca.crt
export YDB_SSL_ROOT_CERTIFICATES_FILE

java -classpath ydb-jdbc-driver-shaded-2.2.1.jar:ydb-decimal-test-1.0-SNAPSHOT.jar tech.ydb.samples.dectest.YdbDecimalTest

# ydb yql -s 'SELECT * FROM dectest1'
# ydb sql -s 'SELECT * FROM dectest1'
