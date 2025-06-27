# DeepLook

This project provides a robust and efficient system for accessing **historical and real-time trading data** from the [DeepBook V3](https://github.com/MystenLabs/deepbookv3) decentralized orderbook on Sui. Built with a focus on performance, reliability, and observability, it delivers OHLCV data, live orderbooks, and trade-level summaries via a simple HTTP API.

---

## Features

- 📊 **OHLCV Aggregation**  
  Aggregated candlestick data (Open, High, Low, Close, Volume) at 1-minute resolution using TimescaleDB.

- 🧾 **Trade-Level Summaries**  
  Exposes normalized historical trades (excluding tick-by-tick) per pool with optional time filters.

- 📚 **Historical Snapshots**  
  Live and historical orderbook snapshots per trading pair with timestamped updates.

- 🧩 **API Extensibility**  
  Fully compatible with all endpoints from `deepbookv3`, with custom endpoints added.

- ⚙️ **Monitoring and Metrics**  
  Includes a full observability stack:
  - [Prometheus](https://prometheus.io/) for metrics
  - [Grafana](https://grafana.com/) dashboards for visual monitoring

---

## Architecture

- **Backend Framework**: Built on top of `deepbookv3` fork.
- **Database**: PostgreSQL + TimescaleDB for high-performance time-series aggregation.
- **Deployment**: Dockerized services with NGINX reverse proxy and HTTPS enabled.
- **Monitoring Stack**: Prometheus + Grafana with custom metrics for ingestion and uptime tracking.

---

## API Endpoints

All endpoints return JSON and are publicly accessible via HTTPS.

### `/get_pools`
Returns metadata for all available pools.  
[Example](https://api.sui.carmine.finance/get_pools)

### `/ohlcv/<pool_name>?start_time=<unix_sec>&end_time=<unix_sec>`
Returns OHLCV candlestick data for the specified time range.  
[Example](https://api.sui.carmine.finance/ohlcv/XBTC_USDC?start_time=1750370400&end_time=1750888800)

### `/orderbook/<pool_name>`
Returns the current orderbook snapshot and the timestamp of the last update.  
[Example](https://api.sui.carmine.finance/orderbook/TYPUS_SUI)

### `/order_fills/<pool_name>?start_time=<unix_sec>&end_time=<unix_sec>`
Returns all trade-level order fills within the specified time window.  
[Example](https://api.sui.carmine.finance/order_fills/SUI_USDC?start_time=1750866244&end_time=1750886244)

---

## Monitoring

- API and indexer prometheus metrics are gathered
- There is a Grafana dashboard for API and for indexer

Dashboards are available [here](http://deeplook.carmine.finance:3000/dashboards).

---

## Data Storage

- Aggregated OHLCV and trade data is stored in Postgresql with **TimescaleDB** extension for fast querying.

---

## Setup & Deployment

### Local development

The project has a Makefile that streamlines local development.

Create `.env` file for local development in the root directory with the following values:
```sh
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=deeplook
POSTGRES_HOST=localhost
DATABASE_URL=postgres://postgres:postgres@localhost/deeplook
REMOTE_STORE_URL=https://checkpoints.mainnet.sui.io
RPC_URL=https://fullnode.mainnet.sui.io:443
ENV=mainnet
```
**Do not use these values in production**

Create Postgresql database locally using docker container.
```sh
make postgres
```
It is currently required to install Timescaledb manually. Access the container:
```sh
docker exec -it deeplook-db /bin/bash
```
And following [Install TimescaleDB on Linux tutorial](https://docs.tigerdata.com/self-hosted/latest/install/installation-linux/#install-timescaledb-on-linux).

Then create db with
```sh
make createdb
```

Run database migrations
```sh
make migrateup
```

Run indexer (currently it runs from checkpoint 150000000 with `skip-watermark`, feel free to adjust this in the Makefile):
```sh
make indexer
```

Run API
```sh
make api
```

### Production

It is advised to build *docker images* from `docker` folder and use those in production.

You need to have `postgresql` database with `timescaledb` extension, then provide ENV variables, specified above, with correct values and run the containers.
