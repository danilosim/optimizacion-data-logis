import argparse
import asyncio
from collections import defaultdict
import csv
from datetime import timedelta, datetime
from itertools import combinations, cycle
from multiprocessing import Process, Value, Queue
import psycopg2
import re
import requests
import sys
from time import time
import traceback

API_KEY = '*'
# DISTANCE_URL_0 = 'http://router.project-osrm.org/table/v1/driving/{locations}?annotations=distance,duration&sources=0'
DISTANCE_URL_1 = 'https://odd-baboon-63.loca.lt/table/v1/driving/{locations}?annotations=distance,duration&sources=0'
DATE_RANGE_START = datetime(2021, 2, 1, 0)
DATE_RANGE_END = datetime(2021, 2, 15, 0)
HOUR_REGEX = r"\d{1,2}:\d{1,2}:\d{1,2}\s[ap].m."

total_database_inserts_counter = 0

def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

def check_valid_coords(lat, long):
    if not (lat and long):
        return False

    try:
        lat = float(lat)
        long = float(long)
    except Exception:
        return False

    if lat == 0 and long == 0:
        return False

    if lat > 90 or lat < -90:
        return False

    if long > 180 or long < -180:
        return False

    return True

def store_distance_results(origin, destinations, distance_results, time_results, queue):
    distance_results = [round(i / 1000, 2) if i else 0 for i in distance_results ]
    time_results = [i // 60 if i else 0 for i in time_results]

    for idx, destination in enumerate(destinations):
        queue.put((min([origin, destination]), max([origin, destination]), distance_results[idx+1], time_results[idx+1]))

def query_distances(origin, destinations, info_dict, queue):
    origin_string = f"{info_dict[origin]['longitude']},{info_dict[origin]['latitude']}"
    destination_strings = ';'.join([f"{info_dict[i]['longitude']},{info_dict[i]['latitude']}" for i in destinations])
    full_string = f"{origin_string};{destination_strings}"
    try:
        formated_url = DISTANCE_URL_1.format(locations=full_string)
        result = None
        result = requests.get(formated_url)
        if result.ok:
            result_json = result.json()

            store_distance_results(origin, destinations, result_json['distances'][0], result_json['durations'][0], queue)

    except Exception:
        traceback.print_exc()
        if result:
            print(result.status_code)
            print(result.raw)
        sys.exit(1)

def calculate_remaining_distances(location_set=None):
    print('Querying locations...\n')

    with psycopg2.connect(database='assistcargo', user='*', password='*', host='localhost') as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            if location_set:
                cursor.execute("SELECT * FROM ubicaciones_mexico WHERE ubicacion_id IN %s;", (tuple(location_set), ))
                locations = cursor.fetchall()
            else:
                cursor.execute("SELECT * FROM ubicaciones_mexico;")
                locations = cursor.fetchall()

    locations = {
        item[0]: {
            'latitude': str(round(item[3], 5)),
            'longitude': str(round(item[4], 5)),
        }
        for item in locations
    }

    location_ids = sorted(locations.keys())

    possible_comb = set(combinations(location_ids, 2))

    with psycopg2.connect(database='assistcargo', user='*', password='*', host='localhost') as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute("SELECT ubicacion_id_origen, ubicacion_id_destino FROM distancias_mexico")
            existing_comb = cursor.fetchall()

    remaining_combinations = possible_comb - set(existing_comb)

    distances_to_calculate = defaultdict(list)

    for item in remaining_combinations:
        distances_to_calculate[item[0]].append(item[1])

    return locations, dict(distances_to_calculate), len(remaining_combinations), len(possible_comb)

def insert_location_entry(cursor, location_id, address, locality, latitude, longitude):
    cursor.execute(
        "INSERT INTO ubicaciones_mexico (ubicacion_id, direccion, localidad, latitud, longitud) VALUES (%s, %s, %s, %s, %s);",
        (location_id, address, locality, latitude, longitude),
    )

def read_and_store_locations():
    print('Reading csv...')

    with open("./csvs/ubicaciones_mexico.csv", "r") as f:
        reader = csv.DictReader(f)
        locations = list(reader)

    # Store only if location has latitude and longitude
    locations = {
        location['UbicacionID']: {
            'address': location['Ubicacion'],
            'locality': location['Localidad'],
            'latitude': float(location['Latitud']),
            'longitude': float(location['Longitud'])
        } for location in locations
        if check_valid_coords(location.get('Latitud'), location.get('Longitud'))
    }

    print('Storing locations')

    try:
        with psycopg2.connect(database='assistcargo', user='*', password='*', host='localhost') as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                for location_id, data in locations.items():
                    insert_location_entry(
                        cursor,
                        location_id,
                        data['address'],
                        data['locality'],
                        data['latitude'],
                        data['longitude'],
                    )
    except Exception:
        traceback.print_exc()

    print('Done storing locations')

def calculate_distances(distances_to_calculate, locations, counter, queue):

    for location_id, destination_locations in distances_to_calculate.items():
        for dist_to_calculate in batch(destination_locations, 200):
            query_distances(location_id, dist_to_calculate, locations, queue)
            counter.value -= len(dist_to_calculate)

async def logging(total_process, total_possible_combinations, counter, queue, start_time):
    global total_database_inserts_counter
    while True:
        queue_size = total_process - counter.value - total_database_inserts_counter
        current_time = str(timedelta(seconds=time() - start_time))
        print(
            f"Remaining calculations: {counter.value}\n"
            f"Total Percentage done: {str(round((total_possible_combinations - counter.value) * 100 / total_possible_combinations, 2))}%\n"
            f"Run Percentage done: {str(round((total_process - counter.value) * 100 / total_process, 2))}%\n"
            f"Queue size: {queue_size}\n"
            f"Elapsed time: {current_time}\n"
        )
        await asyncio.sleep(30)
        if counter.value == 0 and queue.empty():
                break

def calculate_keys_for_workers(num_workers, distances_to_calculate):
    num_split = list(range(num_workers))
    distances_for_workers = [dict() for _ in num_split]
    number_pool = cycle(num_split)

    for key, value in distances_to_calculate.items():
        distances_for_workers[next(number_pool)][key] = value

    return distances_for_workers

async def database_store(remaining_counter, queue):
    global total_database_inserts_counter
    internal_counter = 0
    start_time = time()

    while True:
        with psycopg2.connect(database='assistcargo', user='*', password='*', host='localhost') as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                try:
                    item = queue.get(block=True, timeout=60)
                    total_database_inserts_counter += 1
                except Exception:
                    internal_counter = 10000
                    item = None
                    if remaining_counter.value == 0:
                        break
                if item:
                    cursor.execute(
                        "INSERT INTO distancias_mexico (ubicacion_id_origen, ubicacion_id_destino, distancia, tiempo) VALUES (%s, %s, %s, %s);",
                        item,
                    )
                    internal_counter += 1
                current_time = time()
                if internal_counter == 10000 or current_time - start_time >= 30:
                    await asyncio.sleep(0)
                    internal_counter = 0
                    start_time = time()


async def main(total_process, total_possible_combinations, remaining, queue, start_time):
    await asyncio.gather(
        logging(total_process, total_possible_combinations, remaining, queue, start_time),
        database_store(remaining, queue)
    )
    print("100% Done calculating distances")

def filter_trips_range(trips):
    """"
    Filter the trips that occur on the date range and discard others. Also discard trips that don't
    have a clear way of calculating start and end date.
    """

    filtered_trips = []
    for trip in trips:
        try:
            hora_inicio_re = re.fullmatch(HOUR_REGEX, trip['HR_COMPLETO'])
            hora_fin_re = re.fullmatch(HOUR_REGEX, trip['HF_COMPLETO'])

            if not (hora_inicio_re and hora_fin_re):
                continue

            hora_inicio = trip['HR_COMPLETO'].upper().replace(':', ' ').replace('.', '')
            hora_fin = trip['HF_COMPLETO'].upper().replace(':', ' ').replace('.', '')
            hora_inicio = datetime.strptime(hora_inicio, "%I %M %S %p")
            hora_fin = datetime.strptime(hora_fin, "%I %M %S %p")

            inicio = datetime(
                int(trip['TIEMPOINICIOREALID'][:4]),
                int(trip['TIEMPOINICIOREALID'][4:6]),
                int(trip['TIEMPOINICIOREALID'][6:]),
                hora_inicio.hour,
                hora_inicio.minute,
            )

            if inicio < DATE_RANGE_START or inicio >= DATE_RANGE_END:
                continue

            filtered_trips.append(trip)
        except Exception:
            traceback.print_exc()
            continue

    return filtered_trips

def read_trips():
    with open("./csvs/mexico.csv", "r", encoding = "ISO-8859-1") as f:
        reader = csv.DictReader(f)
        trip_list = list(reader)

    # trip_list = filter_trips_range(trip_list)

    locations = set()
    for trip in trip_list:
        if not (trip['TIEMPOINICIOREALID'] and trip['TIEMPOFINID'] and trip['HR_COMPLETO'] and trip['HF_COMPLETO']):
            continue

        locations.add(int(trip['UBICACIONORIGENID']))
        locations.add(int(trip['UBICACIONDESTINOID']))

    print(f"Working with {len(locations)} locations")
    return locations

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--locations', action='store_true', help='Read and store locations from a csv')
    parser.add_argument('-cd', '--custom_distances', action='store_true', help='Distances from a time frame')
    parser.add_argument('-d', '--distances', action='store_true', help='Calculate and store distances')
    parser.add_argument('-w', '--workers', type=int, help='Number of workers', action='store', default=1)
    args = parser.parse_args()

    if args.locations:
        read_and_store_locations()

    custom_locations = None

    if args.custom_distances:
        custom_locations = read_trips()

    if args.distances:
        print(f'Calculating distances with {args.workers} workers')
        locations, distances_to_calculate, total_process, total_possible_combinations = calculate_remaining_distances(custom_locations)

        remaining = Value('i', total_process)
        queue = Queue()

        distances_for_workers = calculate_keys_for_workers(args.workers, distances_to_calculate)

        workers = [Process(target=calculate_distances, args=(distances_for_workers[i], locations, remaining, queue)) for i in range(args.workers)]

        for worker in workers:
            worker.start()

        start_time = time()

        asyncio.run(main(total_process, total_possible_combinations, remaining, queue, start_time))
