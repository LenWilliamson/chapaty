# Chapaty
Chapaty is the open source software to backtast trading strategies on different markets. Your trading results on [chapaty-ai](https://www.chapaty-ai.com) are computed by this library. To use this library you need to provide your own data and follow the conventions for the historical data below.

## Data Providers and Markets
Currently, the following data providers and markets are supported. The historical market data reaches back until 2006 for FX markets.

| Data Provider | Markets | Data |
| --- | --- | --- |
| **CME** | <ul><li>**FX Futures:** 6A, 6B, 6BTC, 6C, 6E, 6J, 6N</li><li>**Commodities:** CL, NG, GC, ZC, ZS, ZW</li></ul> | OHLC data on 1m time frame |
| **Binance** | <ul><li>BTCUSDT</li><li>ETHUSDT</li></ul> | OHLCV data on 1m time frames and tick data |

## Directory Layout
All the data is stored inside a Google Cloud Storage bucket. There are two buckets. We use one bucket to store the historical market data. We have a second bucket to cache the computed results to decrease the computation time to evaluate a bot on historical market data.

### Historical Market Data
We use the following directory layout. It is for illustrative purposes, as Google Cloud Storage does not have folders or subdirectories. The data is stored as a `.csv` inside the `"bucket"/{provider}/{aggTrades | ohlc-{ts} | ...}` directory. The files follow a strict naming convention.
```bash
.
├── {provider}              # binance / cme / ...   
    ├── ohlc/
    │   ├── {market}-1m-{year}.csv
    │   ├── btcusdt-1m-2023-01.csv
    │   ├── ...
    ├── ohlcv/
    │   ├── {market}-1m-{year}.csv
    │   ├── btcusdt-1m-2023-01.csv
    │   ├── ...
    ├── tick/               
        ├── {market}-tick-{year}.csv
        ├── btcusdt-tick-2023-01.csv
        ├── ...
```
### Cached Data
The cached data is stored in a separate bucket, following this directory layout.
```bash
.
├── {strategy}                              # ppp / magneto / news / ...
    ├── {market}                            # 6e / btcusdt / ...
        ├── {year}                          # 2023 / 2022 / ...
            ├── {time_interval}             # none / TimeFrame::to_string()
                ├── {time_frame}            # { 1w | 1d | 12h | 8h | 6h | 4h | 3h | 2h | 1h | 30m | 15m | 5m }
                    ├── ohlc-{ts}.csv       # Output of processed `.csv` files
                    ├── ohlcv-{ts}.csv      # Output of processed `.csv` files
                    ├── tick.csv            # Output of processed `.csv` files
                    ├── tpo-{ts}.csv        # Output of processed `.csv` files
                    ├── vol-tick.csv        # Contains volume profile, which is computed by tick data
                    ├── vol-aggTrades.csv   # Contains volume profile, which is computed by aggTrades data
                    ├── ...
```