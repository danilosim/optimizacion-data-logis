import csv
from collections import defaultdict

with open("./csvs/mexico.csv", "r") as f:
    reader = csv.DictReader(f)
    trip_list = list(reader)

unidades = {}

for trip in trip_list:
    if not unidades.get(trip['TIPOUNIDADID']):
        unidades[trip['TIPOUNIDADID']] = {
            'tipo': trip['TIPOUNIDAD'],
            'count': 1,
        }
    else:
        unidades[trip['TIPOUNIDADID']]['count'] += 1

for id, unidad in unidades.items():
    print(f'{id}: {unidad["tipo"]} - count {unidad["count"]}')
