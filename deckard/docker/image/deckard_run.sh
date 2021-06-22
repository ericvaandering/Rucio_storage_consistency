#!/bin/sh

/usr/bin/env 

if [ -f /opt/rucio/etc/rucio.cfg ]; then
    echo "rucio.cfg already mounted."
else
    echo "rucio.cfg not found. will generate one."
    j2 /tmp/rucio.cfg.j2 | sed '/^\s*$/d' > /opt/rucio/etc/rucio.cfg
fi

if [ ! -z "$RUCIO_PRINT_CFG" ]; then
    echo "=================== /opt/rucio/etc/rucio.cfg ============================"
    cat /opt/rucio/etc/rucio.cfg
    echo ""
fi

if [ ! -d /var/cache/consistency-dump ]; then
    echo "Scanner output directory /var/cache/consistency-dump not mounted"
    exit 1
fi

# Setup Jobber
cp deckard_jobber.yaml /root/.jobber
cd /deckard/Rucio_storage_consistency
git pull   

# Start Jobber
echo "Starting Jobber"
/usr/local/libexec/jobbermaster &

sleep 5

echo
echo "============= Jobber log file ============="

tail -f /var/log/jobber-runs

###
# if [ "$RUCIO_DAEMON" == "deckard" ]
# then
#   echo "starting sleep for $RUCIO_DAEMON"
#   /usr/bin/sleep infinity
# fi
### 

