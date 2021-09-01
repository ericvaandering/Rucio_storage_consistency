#!/bin/bash

cd /deckard/Rucio_storage_consistency/deckard
python3 deckard.py -c /config/config.yaml -r $1

