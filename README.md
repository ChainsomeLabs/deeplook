# API

The API is built on top of the [deepbookv3](https://github.com/MystenLabs/deepbookv3). The API has all the endpoints of _deepbookv3_ and some more.

## Endpoints

These are the most interesting endpoints:

_/get_pools_

Returns all the pools. [example](https://api.sui.carmine.finance/get_pools)

_/ohlcv/&lt;pool_name&gt;?start_time=&lt;start_timestamp_in_secs&gt;&end_time=&lt;end_timestamp_in_secs&gt;_

Returns Open, High, Low, Close, Volume. [example](https://api.sui.carmine.finance/ohlcv/XBTC_USDC?start_time=1750370400&end_time=1750888800)

_/orderbook/&lt;pool_name&gt;_

Returns current orders with the timestamp of the last update. [example](https://api.sui.carmine.finance/orderbook/TYPUS_SUI)
