#!/usr/bin/env bash
set -euo pipefail

SERVER="root@85.239.43.247"
LOCAL_DIR="/Users/maksimkiselev/Desktop/Under_price_final"

# Push specific data/config files to server when you intentionally update them
scp "$LOCAL_DIR/sources.json" "$SERVER:/opt/under_price/" || true
scp "$LOCAL_DIR/data.json" "$SERVER:/opt/under_price/" || true
scp "$LOCAL_DIR/.env" "$SERVER:/opt/under_price/" || true
scp "$LOCAL_DIR/config/google_service_account.json" "$SERVER:/opt/under_price/config/" || true
rsync -az "$LOCAL_DIR/sessions/" "$SERVER:/opt/under_price/sessions/"
