#!/usr/bin/env bash
set -euo pipefail

root=/opt/raft-benchmark
case "${1:-}" in
  driver)
    [[ "$#" -eq 1 ]]
    ;;
  node)
    [[ "$#" -eq 4 && "$2" =~ ^[0-9]+$ && "$3" =~ ^(standard|lease)$ && "$4" =~ ^(default|lab)$ ]]
    ;;
  *)
    echo 'Usage: bootstrap.sh driver | node ID standard|lease default|lab' >&2
    exit 2
    ;;
esac

if [[ "$1" == node ]] && systemctl is-active --quiet raft-benchmark.service; then
  systemctl stop raft-benchmark.service
fi

mkdir -p "$root/vendor"
for wheel in "$root"/wheels/*.whl; do
  /usr/bin/python3 -m zipfile -e "$wheel" "$root/vendor"
done
PYTHONPATH="$root/vendor" /usr/bin/python3 -c 'import grpc, google.protobuf'

if [[ "$1" == node ]]; then
  install -m 0644 "$root/cloud/raft-benchmark.service" /etc/systemd/system/raft-benchmark.service
  printf 'NODE_ID=%s\nRAFT_MODE=%s\nRAFT_PROFILE=%s\n' "$2" "$3" "$4" > "$root/node.env"
  systemctl daemon-reload
  systemctl restart raft-benchmark.service
fi
