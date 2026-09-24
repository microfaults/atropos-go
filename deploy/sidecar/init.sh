#!/bin/sh
# atropos-init: transparent capture for the atroposd sidecar (design §5.2).
#
# Redirects the app's outbound TCP to the sidecar's egress port and the
# app's inbound ports to the sidecar's ingress port, then writes the
# capture marker the sidecar requires before it registers. Idempotent.
#
# Environment (defaults in parentheses):
#   ATROPOS_PROXY_UID               uid the sidecar runs as; excluded (1337)
#   ATROPOSD_EGRESS_PORT            sidecar egress port (16001)
#   ATROPOSD_INGRESS_PORT           sidecar ingress port (16006)
#   ATROPOSD_INBOUND_PORTS          app ports to capture at ingress, comma-separated ("" = none)
#   ATROPOSD_EXCLUDE_OUTBOUND_PORTS destination ports never redirected (4317,4318: the OTel collector)
#   ATROPOSD_EXCLUDE_CIDRS          destination CIDRs never redirected ("")
#   ATROPOSD_CAPTURE_MARKER         marker path (/var/run/atropos/capture.ok)
set -eu

PROXY_UID="${ATROPOS_PROXY_UID:-1337}"
EGRESS_PORT="${ATROPOSD_EGRESS_PORT:-16001}"
INGRESS_PORT="${ATROPOSD_INGRESS_PORT:-16006}"
INBOUND_PORTS="${ATROPOSD_INBOUND_PORTS:-}"
EXCLUDE_PORTS="${ATROPOSD_EXCLUDE_OUTBOUND_PORTS:-4317,4318}"
EXCLUDE_CIDRS="${ATROPOSD_EXCLUDE_CIDRS:-}"
MARKER="${ATROPOSD_CAPTURE_MARKER:-/var/run/atropos/capture.ok}"

ipt() { iptables -w 5 -t nat "$@"; }

# Fresh chains (idempotent on container restart).
for chain in ATROPOS_OUTPUT ATROPOS_INBOUND; do
  ipt -N "$chain" 2>/dev/null || ipt -F "$chain"
done
ipt -C OUTPUT -p tcp -j ATROPOS_OUTPUT 2>/dev/null || ipt -A OUTPUT -p tcp -j ATROPOS_OUTPUT
ipt -C PREROUTING -p tcp -j ATROPOS_INBOUND 2>/dev/null || ipt -A PREROUTING -p tcp -j ATROPOS_INBOUND

# Outbound: never redirect the sidecar's own connections, loopback traffic
# (the sidecar's hop to the app, explicit-mode ports, health probes), the
# excluded ports/CIDRs; redirect everything else to the egress listener.
ipt -A ATROPOS_OUTPUT -m owner --uid-owner "$PROXY_UID" -j RETURN
ipt -A ATROPOS_OUTPUT -o lo -j RETURN
ipt -A ATROPOS_OUTPUT -d 127.0.0.0/8 -j RETURN
old_ifs="$IFS"; IFS=','
for p in $EXCLUDE_PORTS; do
  p="$(echo "$p" | tr -d ' ')"; [ -n "$p" ] && ipt -A ATROPOS_OUTPUT -p tcp --dport "$p" -j RETURN
done
for c in $EXCLUDE_CIDRS; do
  c="$(echo "$c" | tr -d ' ')"; [ -n "$c" ] && ipt -A ATROPOS_OUTPUT -d "$c" -j RETURN
done
IFS="$old_ifs"
ipt -A ATROPOS_OUTPUT -p tcp -j REDIRECT --to-ports "$EGRESS_PORT"

# Inbound: only the declared app ports, to the ingress listener.
old_ifs="$IFS"; IFS=','
for p in $INBOUND_PORTS; do
  p="$(echo "$p" | tr -d ' ')"; [ -n "$p" ] && ipt -A ATROPOS_INBOUND -p tcp --dport "$p" -j REDIRECT --to-ports "$INGRESS_PORT"
done
IFS="$old_ifs"

mkdir -p "$(dirname "$MARKER")"
{
  echo "egress_port=$EGRESS_PORT"
  echo "ingress_port=$INGRESS_PORT"
  echo "inbound_ports=$INBOUND_PORTS"
  echo "exclude_ports=$EXCLUDE_PORTS"
  echo "exclude_cidrs=$EXCLUDE_CIDRS"
  echo "proxy_uid=$PROXY_UID"
  echo "installed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > "$MARKER"
chmod 0644 "$MARKER"
echo "atropos-init: capture installed; marker $MARKER"
iptables -w 5 -t nat -S ATROPOS_OUTPUT
iptables -w 5 -t nat -S ATROPOS_INBOUND
