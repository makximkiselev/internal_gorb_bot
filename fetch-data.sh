#!/usr/bin/env bash
set -euo pipefail

SERVER="root@85.239.43.247"
LOCAL_DIR="/Users/maksimkiselev/Desktop/Under_price_final"

rsync -az "$SERVER:/opt/under_price/data/" "$LOCAL_DIR/data/"
rsync -az "$SERVER:/opt/under_price/handlers/parsing/data/" "$LOCAL_DIR/handlers/parsing/data/"
