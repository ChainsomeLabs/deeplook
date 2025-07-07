#!/bin/bash

export RUST_BACKTRACE=1
export RUST_LOG=info

/opt/mysten/bin/deepbook-indexer --env "$ENV" --database-url "$DATABASE_URL"
