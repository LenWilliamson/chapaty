<p align="center">
    <img src="chapaty-ai.svg" alt="Image Description" width="500">
</p>

# chapaty v0.1.0
Chapaty is the open source software to backtast trading strategies on different markets. Your trading results on [chapaty-ai](https://www.chapaty-ai.com) are computed by this library. To use this library you need to provide your own data and follow the conventions for the historical data below.
## Data Providers and Markets
Currently the following data providers and markets are supported. The historical market data reaches back until 2006 for some markets.
Data Provider|Markets|Data
--- | --- | ---
CME | <p> FX Futures: 6A, 6B, 6BTC, 6C, 6E, 6J, 6N <p> Commodities: CL, NG, GC, ZC, ZS, ZW<p> | OHLC on different time frames
Binance | <p> btcusdt <p> ETHUSDT <p> | OHLCV on different time frames, Tick data, Aggregated trades data

## Profit and Loss Report
When Backtesting your bot, chapaty will generate a profit and loss report of the following form.
#|CalendarWeek|Date|Strategy|Market|TradeDirection|Entry|TakeProfit|StopLoss|ExpectedWinTik|ExpectedLossTik|ExpectedWinDollar|ExpectedLossDollar|Crv|EntryTimestamp|TargetTimestamp|StopLossTimestamp|ExitPrice|Status|PlTik|PlDollar
--- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- 
0|2|2022-01-10|PPP|EurUsd|Long|1.1549|1.162|0.1537|142.0|-20024.0|887.45|-125150.0|0.0071|2022-01-10 14:00:00|Timeout|Timeout|1.1575|Winner|52.0|325.0
1|2|2022-01-11|PPP|EurUsd|Long|1.15735|1.1585|0.15315|23.0|-20084.0|143.75|-125525.0|0.0011|2022-01-11 11:00:00|2022-01-11 11:00:00|Timeout|1.1585|Winner|23.0|143.75

## Performance Report
Additionally to the profit and loss report, chapaty generates a performance report of the following form.
Year|Strategy|Market|NetProfit|AvgWinnByTrade|MaxDrawDownAbs|MaxDrawDownRel|PercentageProfitability|RatioAvgWinByAvgLoss|AvgWin|AvgLoss|ProfitFactor
--- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- 
2022|Ppp|EurUsd|13475.0|83.18|5581.25|-0.41|0.77|0.46|308.46|-677.91|1.54

## Trade Breakdown Report
Year|Strategy|TotalWin|TotalLoss|CleanWin|TimeoutWin|CleanLoss|TimeoutLoss|TotalNumberWinnerTrades|TotalNumberLoserTrades|TotalNumberTrades|NumberWinnerTrades|NumberLoserTrades|NumberTimeoutWinnerTrades|NumberTimeoutLoserTrades|NumberTimeoutTrades|NumberNoEntry
--- | --- | --- | --- | --- | --- | --- |  --- | --- | --- | --- | --- | --- | --- | --- | --- | --- 
2022|Ppp|38557.5|-25082.5|32876.25|5681.25|0.0|-25082.5|125|37|162|107|0|18|37|55|89


## Opening Data in Excel
The reports are simple `.csv` files, where:
* Columns are separated with a comma `,`
* Floating point numbers use a decimal point `.`

If you want to open a report in Excel and you don't get a nice formatted file, try the following steps:
1. Open the `.csv` file in a simple text editior
2. Replace all `,` with `;`
3. Replace all `.` with `,`
4. Save the file
5. Reopen the file in Excel agein

## Directory Layout
All the data is stored inside a Google Cloud Storage bucket. There are two buckets. We use one bucket to store the historical market data. We have a second bucket to cache the computed results to decrease the computation time to evaluate a bot on historical market data.

### Historical Market Data
We use the following directory layout. It is for illustrative purposes, as Google Cloud Storage does not have folders or subdirectories. The data is stored as a `.csv` inside the `"bucket"/{provider}/{aggTrades | ohlc-{ts} | ...}` directory. The files follow a strict naming convention.
```bash
.
├── {provider}              # binance / cme / ...
    ├── aggTrades/         
    │   ├── {market}-aggTrades-{year}[-{month?}-{day?}].csv
    │   ├── btcusdt-aggTrades-2023-01.csv
    │   ├── ...        
    ├── ohlc/               # For ts in { 1w | 1d | 12h | 8h | 6h | 4h | 3h | 2h | 1h | 30m | 15m | 5m | 1m | 30s | 15s | 1s }
    │   ├── {market}-{ts}-{year}[-{month?}-{day?}].csv
    │   ├── btcusdt-1m-2023-01.csv
    │   ├── ...
    ├── ohlcv/              # For ts in { 1w | 1d | 12h | 8h | 6h | 4h | 3h | 2h | 1h | 30m | 15m | 5m | 1m | 30s | 15s | 1s }
    │   ├── {market}-{ts}-{year}[-{month?}-{day?}].csv
    │   ├── btcusdt-1m-2023-01.csv
    │   ├── ...
    ├── tick/               
        ├── {market}-tick-{year}[-{month?}-{day?}].csv
        ├── btcusdt-tick-2023-01.csv
        ├── ...
```
### Cached Data
The cached data is stored in a separate bucket, following this directory layout.
```bash
.
├── {strategy}                              # ppp / magneto / postNews / ...
    ├── {market}                            # 6e / btcusdt / ...
        ├── {year}                          # 2023 / 2022 / ...
            ├── {time_interval}             # none / TimeFrame::to_string()
                ├── {time_frame}            # { 1w | 1d | 12h | 8h | 6h | 4h | 3h | 2h | 1h | 30m | 15m | 5m }
                    ├── aggTrades.csv       # Output of processed `.csv` files
                    ├── ohlc-{ts}.csv       # Output of processed `.csv` files
                    ├── ohlcv-{ts}.csv      # Output of processed `.csv` files
                    ├── tick.csv            # Output of processed `.csv` files
                    ├── tpo-{ts}.csv        # Output of processed `.csv` files
                    ├── vol-tick.csv        # Contains volume profile, which is computed by tick data
                    ├── vol-aggTrades.csv   # Contains volume profile, which is computed by aggTrades data
                    ├── ...
```
### Test Data
We maintain an additional bucket for test data. The directory layout is identical to the above ones, with one special directory `ohter`.
```bash
.
├── {...directory layout}
│
├── other                   # Special only directory
    ├── test_file1.csv      # Test files for unit tests
    ├── test_file2.csv      # ...
    ├── ...
    ├── gcp/                # Contains test files for the `gcp` module
    ├── market_profile/     # Contains test files for the `market_profile` module
    ├── ppp/                # Contains test files for the `ppp` module
    └── ...                 # etc.
```

## Downloading from GCP
To download multiple files from the Google Cloud Platform do the following steps.
1. Open the gsutil command line tool
2. Paste the command e.g. `gsutil cp -r gs://bucket/folder .`
3. Run `$ zip -r folder.zip folder/` to zip your files
4. In the gsutil commoand line tool menu click the botton with the three dots on the top right corner
5. Select Download
6. Append the file `folder.zip` name you want to download to the file path

## Renaming files
Use the `rename` command:
1. To lowercase: `rename -f 'y/A-Z/a-z/' *.csv`
2. Replace `_` with `-`: `rename 's/_/-/g' *.csv`
3. Replace `6btc-1m-01-01-2021-31-12-2021.csv` to `6btc-1m-2021-31-12-2021.csv` by `rename 's/^(\w+-\w+-)(\d{2}-\d{2}-)(\d{4}-\d{2}-\d{2}-\d{4})\.csv$/$1$3.csv/' *.csv` and then `6btc-1m-2021-31-12-2021.csv` to `6btc-1m-2021.csv` by `rename 's/^(.{12}).*\.csv$/$1.csv/' *.csv` (replace 12 in `(.{12})` with 10 if you want to keep first ten letters)