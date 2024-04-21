import json
import csv

input_file = "./data.json"
output_file = "./data.csv"

# Wczytaj dane z pliku JSON
with open(input_file) as f:
    dane_json = json.load(f)

# Otwórz plik CSV do zapisu
with open(output_file, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)

    general_header_names = ['symbol', 'name', 'sector', 'industry']
    stock_header_names = ['date', 'open', 'high', 'low', 'close', 'adjClose', 'volume', 'unadjustedVolume', 'change', 'changePercent', 'vwap', 'label', 'changeOverTime']
    all_header_names = general_header_names + stock_header_names
    writer.writerow(all_header_names)

    # Przetwarzanie danych JSON i zapis do pliku CSV
    for row in dane_json:
        # Sprawdź czy wiersz zawiera zagnieżdżoną tablicę
        if 'stockData' in row:
            # Dla każdego elementu zagnieżdżonej tablicy
            for element in row['stockData']:
                new_row = [row[header] for header in general_header_names]
                new_row.extend(element.values())
                writer.writerow(new_row)
        else:
            raise Exception("stockData not found") 

print("Conversion completed")