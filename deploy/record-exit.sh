#!/bin/sh
set -eu
if [ "${SERVICE_RESULT:-success}" != success ] && [ ! -e /var/lib/fleet-monitor/PAUSED.json ]; then
  umask 077
  printf '{"at":"%s","reason":"systemd_%s","resume":"Inspect the cause before manually removing this marker and restarting"}\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${SERVICE_RESULT:-unknown}" > /var/lib/fleet-monitor/PAUSED.json
fi
