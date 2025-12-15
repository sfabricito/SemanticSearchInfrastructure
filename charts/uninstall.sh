#!/bin/bash

set -euo pipefail

helm list

# Ignore failures if releases are already gone
helm uninstall application || true
helm uninstall databases || true
