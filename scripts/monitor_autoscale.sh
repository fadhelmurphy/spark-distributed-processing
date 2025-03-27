#!/bin/bash
MAX_WORKERS=5  # Batas maksimum Worker
while true; do
    active_workers=$(curl -s http://VM-MASTER-IP:8080/json | jq '.workers | length')
    if [ "$active_workers" -lt "$MAX_WORKERS" ]; then
        usage=$(ssh vm1 "mpstat | awk '/all/ {print 100 - $NF}'")
        if [ "$usage" -gt 80 ]; then
            echo "Menambahkan Worker baru..."
            ssh new-vm "bash start-spark-worker.sh"
        fi
    fi
    sleep 10
done
