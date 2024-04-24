import json
import csv

input_file = "./stockdata.json"
output_file = "./stockdata.csv"

with open(input_file) as f:
    dane_json = json.load(f)

with open(output_file, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)

    general_header_names = ['symbol', 'name', 'sector', 'industry', 'exchange', 'exchangeShortName', 'volAvg', 'mktCap', 'companyName', 'cik', 'isin', 'cusip', 'website', 'ceo', 'fullTimeEmployees', 'address', 'city', 'zip', 'image']
    stock_header_names = ['date', 'open', 'high', 'low', 'close', 'adjClose', 'volume', 'unadjustedVolume', 'change', 'changePercent', 'vwap', 'label', 'changeOverTime']
    stock_data_column = 'stockData'
    all_header_names = general_header_names + stock_header_names
    writer.writerow(all_header_names)

    # for every element in json table
    for row in dane_json:
        print("#", end='')
        if stock_data_column in row:
            # for every element of inner table
            for element in row[stock_data_column]:
                new_row = [str(row[header]).replace(',', '.') if isinstance(row[header], str) else row[header] for header in general_header_names]
                new_row.extend(element.values())
                writer.writerow(new_row)
        else:
            print('warning: ' + stock_data_column + ' column not exists in json object: ')
            print(row)
            print('skipping')

print("Conversion completed")