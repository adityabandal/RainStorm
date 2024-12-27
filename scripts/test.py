import csv
from collections import defaultdict

def aggregate_column(csv_file_path, target_string):
    aggregated_data = {}
    aggregated_data = defaultdict(int)
    x = 0
    with open(csv_file_path, mode='r') as file:
        reader = csv.reader(file)
        for row in reader:
            if row[6] == target_string:
                aggregated_data[row[8]] += 1
    
    return aggregated_data

# Example usage
csv_file_path = '../files/remote/local/TrafficSigns_10000.csv'
target_string = 'Punched Telespar'
result = aggregate_column(csv_file_path, target_string)
total = 0
for key, value in result.items():
    print(f'{key}: {value}')
    total += value
print(f'Total: {total}')
