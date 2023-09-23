#!/usr/bin/env bash

mc alias set \
  local "http://127.0.0.1:9000" root password
mc admin user add \
  local portfolio-dev dev-password
mc admin policy attach \
  local readwrite --user=portfolio-dev

mc alias set \
  dev "http://127.0.0.1:9000" portfolio-dev dev-password
mc mb \
  dev/portfolio-dev
