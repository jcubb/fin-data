# Financial Data Update Tool

A Python tool for updating and managing financial data, specifically focused on S&P 500 stocks and SPDR ETF factor data.

## Features

- **S&P 500 Data Management**: Scrapes current S&P 500 company list with market cap and weights
- **Historical Data Updates**: Updates existing pickle files with new financial data from Yahoo Finance
- **Sector & Industry Analysis**: Retrieves sector and industry information for stocks
- **ETF Factor Data**: Manages SPDR ETF factor data for analysis
- **Data Persistence**: Maintains historical snapshots using pickle files

## Requirements

- Python 3.7+
- pandas
- yfinance
- beautifulsoup4
- requests
- openpyxl (for Excel file reading)

## Installation

1. Clone this repository:
```bash
git clone https://github.com/yourusername/fin-data.git
cd fin-data
```

2. Install required packages:
```bash
pip install -r requirements.txt
```

## Usage

Run the main script:
```bash
python fin_data_update.py
```

The script will:
1. Update S&P 500 ticker list and market data
2. Update historical returns data
3. Collect sector and industry information
4. Update SPDR factor ETF data

## Data Files

- `etf_tickers.csv`: List of ETF tickers for analysis
- `spdr_data.xlsx`: SPDR ETF information and metadata
- 
- Expect all of the following to be present in `data_db_root`. 
- `spdrfactors.pickle`: all the Historical SPDR ETF returns
- `sprtns.pickle`: all the historical individual equity returns
- `spsect.pickle`: all the individual equity sectors/industries, frozen when first found
- `sp500_history.pickle`: dictionary of SP500 snapshots, with sector, industry, weight
**HOWEVER** now the code will create them if they are not already present
- Generated pickle files store historical data and are saved to the configured data directory

## Configuration

Update the `data_db_root` variable in the script to point to your desired data storage location:
```python
data_db_root = 'C:\\Users\\gcubb\\OneDrive\\Python\\data-hub'
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Disclaimer

This tool is for educational and research purposes only. Always verify financial data from official sources before making investment decisions.