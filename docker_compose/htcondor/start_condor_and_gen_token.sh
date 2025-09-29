#!/bin/sh

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
