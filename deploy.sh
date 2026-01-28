#!/usr/bin/env bash
set -euo pipefail

SERVER="root@85.239.43.247"
APP_DIR="/opt/under_price"
SERVICE="underprice-bot"

ssh "$SERVER" "cd $APP_DIR && git pull && systemctl restart $SERVICE"
