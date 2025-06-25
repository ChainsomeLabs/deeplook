#!/bin/bash

export RUST_BACKTRACE=1
export RUST_LOG=info

/opt/mysten/bin/deepbook-server --database-url "$DATABASE_URL" --rpc-url "$RPC_URL"
