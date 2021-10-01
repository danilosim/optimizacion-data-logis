import requests
import csv
import datetime
import psycopg2


CONN = conn = psycopg2.connect(
    database='assistcargo', user='*', password='*', host='localhost')

API_KEY = '*'
DISTANCE_URL = 'http://router.project-osrm.org/table/v1/driving/{locations}?annotations=distance&sources=0'
url = "https://maps.googleapis.com/maps/api/distancematrix/json?origins={origins}&destinations={destinations}&key={api_key}"


def calcular_distancias(origin, destination):
    full_string = f"{origin['long']},{origin['lat']};{destination['long']},{destination['lat']}"
    try:
        formated_url = DISTANCE_URL.format(locations=full_string)
        formated_url = url.format(
            origins=f"{origin['lat']},{origin['long']}", destinations=f"{destination['lat']},{destination['long']}", api_key=API_KEY)
        result = requests.get(formated_url)
        result_json = result.json()
        # print(result_json)
        distance = round(
            result_json['rows'][0]['elements'][0]['distance']['value'] / 1000, 2)
        time = int(
            result_json['rows'][0]['elements'][0]['duration']['value'] / 60) * 2
        return distance, time

    except Exception as e:
        # print(e)
        return 0, 0


def get_distance(origin, destination, origin_data, destination_data):
    distancia = None
    global CONN
    try:
        cur = CONN.cursor()

        cur.execute(
            """SELECT distancia, tiempo
            FROM distancias_mexico
            WHERE ubicacion_id_origen = (%s) AND ubicacion_id_destino = (%s);
            """,
            (min(origin, destination), max(origin, destination))
        )
        distancia = cur.fetchone()
    except Exception:
        return 0

    if distancia:
        return float(distancia[0]), float(distancia[1])
    else:
        distancia, tiempo = calcular_distancias(origin_data, destination_data)
        if distancia == 0 or tiempo == 0:
            return 0, 0
        return distancia, tiempo


with open("./optimizer_results_thorton/result_original.csv", "r", encoding="ISO-8859-1") as f:
    reader = csv.DictReader(f)
    original_trip_list = list(reader)

original_trip_list = sorted(original_trip_list, key=lambda x: datetime.datetime.strptime(
    x['real_start'], "%Y-%m-%d %H:%M:%S"))

total_kilometers = 0
total_time = 0
unidades_locations = {}

for trip in original_trip_list:
    if (
        trip['unidad'] not in unidades_locations
        or trip['origin_id'] == unidades_locations[trip['unidad']]['ubicacion']
        or datetime.datetime.strptime(trip['real_start'], "%Y-%m-%d %H:%M:%S").date() != unidades_locations[trip['unidad']]['date']
    ):
        unidades_locations[trip['unidad']] = {
            'ubicacion': trip['dest_id'],
            'data': {
                'lat': trip['dest_lat'],
                'long': trip['dest_long'],
            },
            'date': datetime.datetime.strptime(trip['real_start'], "%Y-%m-%d %H:%M:%S").date(),
        }
    else:
        distance, time_distance = get_distance(
            unidades_locations[trip['unidad']]['ubicacion'],
            trip['dest_id'],
            unidades_locations[trip['unidad']]['data'],
            {
                'lat': trip['dest_lat'],
                'long': trip['dest_long'],
            }
        )
        total_kilometers += float(distance)
        total_time += float(time_distance)
        unidades_locations[trip['unidad']] = {
            'ubicacion': trip['dest_id'],
            'data': {
                'lat': trip['dest_lat'],
                'long': trip['dest_long'],
            },
            'date': datetime.datetime.strptime(trip['real_start'], "%Y-%m-%d %H:%M:%S").date(),
        }

print('\nKilometros con camiones vacíos original')
print(int(total_kilometers))
print('\nTiempo con camiones vacíos original')
print(int(total_time / 60))

with open("./optimizer_results_thorton/result.csv", "r", encoding="ISO-8859-1") as f:
    reader = csv.DictReader(f)
    optimizer_trip_list = list(reader)

optimizer_trip_list = sorted(optimizer_trip_list, key=lambda x: datetime.datetime.strptime(
    x['real_start'], "%Y-%m-%d %H:%M:%S"))

total_kilometers = 0
total_time = 0
unidades_locations = {}

for trip in optimizer_trip_list:
    if trip['unidad'] not in unidades_locations:
        unidades_locations[trip['unidad']] = {
            'ubicacion': trip['dest_id'],
            'data': {
                'lat': trip['dest_lat'],
                'long': trip['dest_long'],
            }
        }
    elif int(trip['carga']) == 0:
        total_kilometers += float(trip['calculated_kms'])
        total_time += float(trip['calculated_minutes'])
        unidades_locations[trip['unidad']] = {
            'ubicacion': trip['dest_id'],
            'data': {
                'lat': trip['dest_lat'],
                'long': trip['dest_long'],
            }
        }
    else:
        # distance = get_distance(
        #     unidades_locations[trip['unidad']]['ubicacion'],
        #     trip['dest_id'],
        #     unidades_locations[trip['unidad']]['data'],
        #     {
        #         'lat': trip['dest_lat'],
        #         'long': trip['dest_long'],
        #     }
        # )
        # total_kilometers += distance
        unidades_locations[trip['unidad']] = {
            'ubicacion': trip['dest_id'],
            'data': {
                'lat': trip['dest_lat'],
                'long': trip['dest_long'],
            }
        }

print('\n\nKilometros con camiones vacíos optimizador')
print(int(total_kilometers))
print('\nTiempo con camiones vacíos optimizador')
print(int(total_time / 60))
