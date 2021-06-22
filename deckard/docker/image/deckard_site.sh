#!/bin/bash

cd /deckard/Rucio_storage_consistency/deckard

export PYTHON=python3 

./site_CC.sh \
  /config/config.yaml \
  /opt/rucio/etc/rucio.cfg \
  $1 \
  /var/cache/consistency-temp \
  /var/cache/consistency-dump \
  /opt/proxy/x509up                   


