# chapaty v0.0.0
This crate lets you backtest arbitrary trading strategies and generates a simple P&L report of the following form:
CW|Date|Strategy|Market|Trade_Direction|Entry|Target|Stop_Loss|Expected_Win_Tik|Expected_Loss_Tik|Expected_Win_Dollar|Expected_Loss_Dollar|CRV|Entry_Timestamp|Target_Timestamp|Stop_Loss_Timestamp|Exit_Price|Status|PL_Tik|PL_Dollar
--- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- 
44|2022-11-02|PPP|6E|Short|1.0033|0.99675|2.0059|131.0000000000011|-20051.999999999996|818.7500000000069|-125324.99999999997|0.0065330141631757994|2022-11-02 19:00:00|2022-11-02 20:00:00|Timeout|0.99675|Winner|131.0000000000011|818.7500000000069
36|2022-09-06|PPP|6E|Long|0.9993500000000001|1.0012999999999999|-0.0049000000000000155|0.0019499999999997852|-1.00425|0.0019499999999997852|-1.00425|0.0019417475728153199|2022-09-06 13:00:00|Timeout|Timeout|0.99765|Timeout|-0.0017000000000000348|-0.0017000000000000348

This report is a simple `.csv` file, where:
* Columns are separated with a comma `,`
* Floating point numbers use a decimal point `.`

If you want to open this report in Excel and you don't get a nice formatted file, try the following steps:
1. Open the `.csv` file in a simple text editior
2. Replace all `,` with `;`
3. Replace all `.` with `,`
4. Save the file
5. Reopen the file in Excel agein

# Table of Contents
1. [Directory Layout](#directoryLayout)
2. [Finding Files](#findingFiles)
4. [Downloading from GCP](#downloadFromGCP)
5. [Improvement Proposals](#improvementProposals)
   1. [Major](#major)
   2. [Minor](#minor)

# Directory Layout
We use the following directory layout. It is for illustrative purposes, as Google Cloud Storage does not have folders or subdirectories. The raw data is stored inside the `data` directory. To be precise, it is stored as a `.csv` inside the `"bucket"/data/{producer}/{market}/{year}/{aggTrades | ohlc-{ts} | ...}` directory. Once we run our consumer to process the raw data the `cw/` directory inside `"bucket"/data/...` and the respective directories inisde `"bucket"/strategy/...` get populated. The bot runs after the consumer processed all files.

```bash
.
├── data
│   ├── {producer}                     # binance / ninja / test / ...
│       ├── {market}                   # 6e / btcusdt / ...
│           ├── {year}                 # 2023 / 2022 / ...
│               ├── aggTrades/         # Contains raw data files
│               │   ├── raw_data1.csv
│               │   ├── raw_data2.csv
│               │   ├── ...
│               │   ├── cw/            # Contains raw data files split by calendar week
│               ├── ohlc-{ts}/         # Contains raw data files for ts = {1h | 30m | 1m | ...}
│               │   ├── raw_data1.csv
│               │   ├── raw_data2.csv
│               │   ├── ...
│               │   ├── cw/            # ...
│               ├── ohlcv-{ts}/        # Contains raw data files for ts = {1h | 30m | 1m | ...}
│               │   ├── raw_data1.csv
│               │   ├── raw_data2.csv
│               │   ├── ...
│               │   ├── cw/            # ...
│               ├── tick/              # Contains raw data files
│               │   ├── raw_data1.csv
│               │   ├── raw_data2.csv
│               │   ├── ...
│               │   ├── cw/            # ...
│               └── ...                # etc.
├── strategy
    ├── {bot}                          # ppp / magneto / gap / ...
        ├── {market}                   # 6e / btcusdt / ...
            ├── {year}                 # 2023 / 2022 / ...
                ├── {granularity}      # cw / day
                    ├── aggTrades/     # Output of processed `.csv` files by the consumer
                    ├── ohlc-{ts}/     # Output of processed `.csv` files by the consumer
                    ├── ohlcv-{ts}/    # Output of processed `.csv` files by the consumer
                    ├── tick/          # Output of processed `.csv` files by the consumer
                    ├── ...
                    ├── vol/           # Contains volume profile, which is computed by the consumer
                    ├── pl/            # Contains P&L computed by the bot, which runs after the consumer processed all files
```

We maintain a special directory `ohter` inside `"bucket"/data/test/` for testing purposes. This directory is not used in production. We omitted this directory in the above directory layout.

```bash
.
├── data
    ├── test                        # {producer} = test
        ├── other                   # Special only directory for testing purposes
            ├── test_file1.csv      # Test files for unit tests
            ├── test_file2.csv      # ...
            ├── ...
            ├── gcp/                # Contains test files for the `gcp` module
            ├── market_profile/     # Contains test files for the `market_profile` module
            ├── ppp/                # Contains test files for the `ppp` module
            └── ...                 # etc.
```
# Finding Files
The consumer and bot need to know where to find the respective files in our hierachy. Upon start we configure our `Finder` on the variable parameters
* `producer` (i.e. binance, ninja, test, ...)
* `market` (i.e. 6e, btcusdt, ...)
* `year` (i.e. 2023, 2022, ...)
* `bot` (i.e. ppp, magneto, gap, ...)
* `granularity` (currently only: weekly or daily)

Hence, to get the path to a directory we only say if we want to look inside `data` or `strategy` and what leaf directory we want to find (`ohlc-1m`, `tick`, etc.).

# Downloading from GCP
To download multiple files from the Google Cloud Platform do the following steps.
1. Open the gsutil command line tool
2. Paste the command you copied to your clipboard
3. Run `$ zip -r vol.zip vol/` to zip your files
4. In the gsutil commoand line tool menu click the botton with the three dots on the top right corner
5. Select Download
6. Append the file name you want to download to the file path

# Improvement Proposals
## Major
In this section we collect simple improvment proposals:
* TBD


## Minor
In this section we collect simple improvment proposals:
* TBD
