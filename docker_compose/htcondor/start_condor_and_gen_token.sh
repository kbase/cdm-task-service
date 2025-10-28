#!/bin/sh

# Add the submituser to the docker group
usermod -aG docker submituser

# Stage the cts token & s3 secret
mkdir /cts
echo $CTS_EXCECUTOR_TOKEN_FOR_FILE > /cts/cts_token
echo $CTS_EXECUTOR_S3_ACCESS_SECRET_FOR_FILE > /cts/s3_secret
chown submituser /cts/*
chmod 600 /cts/*

mkdir /condor_workdir
chmod 777 /condor_workdir

chmod 755 /etc/condor/tokens.d/

# Kick off a background job to fetch token after startup delay waiting for condor
(
  sleep 5
  su -s /bin/sh submituser -c "condor_token_fetch > /home/submituser/mini.token"
  cp /home/submituser/mini.token /etc/condor/tokens.d/
  chmod 600 /etc/condor/tokens.d/mini.token
) &

# Replace shell with Condor’s normal startup so it gets signals correctly
exec /start.sh
