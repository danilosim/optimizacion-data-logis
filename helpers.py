import csv
import datetime
from math import ceil
import os
import psycopg2
import traceback
import re
from collections import defaultdict

HOUR_REGEX = r"\d{1,2}:\d{1,2}:\d{1,2}\s[ap].m."
WORKING_HOURS_START = 420
WORKING_HOURS_END = 1080


def build_locations_set(trips, key):
    "Build set of locations that will be used in the optimization using the trips from the database."
    units_list = []
    if key:
        try:
            with open(f"optimizer_results/free_units_{key}.csv", "r", encoding="ISO-8859-1") as f:
                reader = csv.DictReader(f)
                units_list = list(reader)
        except Exception:
            pass

    locations = set([trip['origen'] for trip in trips.values()]) | set(
        [trip['destino'] for trip in trips.values()]) | set([u['location'] for u in units_list])
    ubicaciones = []
    try:
        conn = psycopg2.connect(
            database='assistcargo', user='*', password='*', host='localhost')
        cur = conn.cursor()

        cur.execute(
            """
            SELECT ubicacion_id
            FROM ubicaciones_mexico
            WHERE ubicacion_id = ANY(%s);
            """,
            (list(locations), )
        )
        ubicaciones_disponibles = cur.fetchall()
        ubicaciones = [a[0] for a in ubicaciones_disponibles]
    except Exception:
        traceback.print_exc()

    return ubicaciones


def filter_trips_range(trips, key=None):
    """"
    Filter the trips that occur on the date range and discard others. Also discard trips that don't
    have a clear way of calculating start and end date.
    """

    filtered_trips = {}
    for trip_id, data in trips.items():
        try:
            hora_inicio_re = re.fullmatch(HOUR_REGEX, data['hora_inicio'])
            hora_fin_re = re.fullmatch(HOUR_REGEX, data['hora_fin'])

            if not (hora_inicio_re and hora_fin_re):
                continue

            hora_inicio = data['hora_inicio'].upper().replace(
                ':', ' ').replace('.', '')
            hora_fin = data['hora_fin'].upper().replace(
                ':', ' ').replace('.', '')
            hora_inicio = datetime.datetime.strptime(
                hora_inicio, "%I %M %S %p")
            hora_fin = datetime.datetime.strptime(hora_fin, "%I %M %S %p")

            inicio = datetime.datetime(
                int(data['fecha_inicio'][:4]),
                int(data['fecha_inicio'][4:6]),
                int(data['fecha_inicio'][6:]),
                hora_inicio.hour,
                hora_inicio.minute,
            )
            fin = datetime.datetime(
                int(data['fecha_fin'][:4]),
                int(data['fecha_fin'][4:6]),
                int(data['fecha_fin'][6:]),
                hora_fin.hour,
                hora_fin.minute,
            )

            if inicio < DATE_RANGE_START or inicio >= DATE_RANGE_END:
                continue

            filtered_trips[trip_id] = {
                'origen': data['origen'],
                'destino': data['destino'],
                'inicio_datetime': inicio,
                'fin_datetime': fin,
                'unidad': data['unidad'],
                'tipo_unidad': data['tipo_unidad']
            }
        except Exception:
            traceback.print_exc()
            continue

    locations = build_locations_set(filtered_trips, key)

    filtered_trips = {
        k: v for k, v in filtered_trips.items()
        if v['origen'] in locations and v['destino'] in locations
    }

    return filtered_trips, locations


def calculate_start_end_datetime(trips):
    "Calculate the start and end datetimes in minutes from the start of the date range for one trip."
    trips_dict = {}
    for trip_id, data in trips.items():

        initial_timedelta = data['inicio_datetime'] - DATE_RANGE_START
        initial_minutes = initial_timedelta.days * \
            24 * 60 + initial_timedelta.seconds // 60

        end_timedelta = data['fin_datetime'] - DATE_RANGE_START
        end_minutes = end_timedelta.days * 24 * 60 + end_timedelta.seconds // 60

        trips_dict[trip_id] = {
            'origen': data['origen'],
            'destino': data['destino'],
            'inicio_datetime': data['inicio_datetime'],
            'fin_datetime': data['fin_datetime'],
            'inicio_minutes': initial_minutes,
            'fin_minutes': end_minutes,
            'unidad': data['unidad'],
            'tipo_unidad': data['tipo_unidad'],
        }

    return trips_dict


def read_units_locations(key):
    starts_data = {}
    units_list = []
    if key:
        try:
            with open(f"optimizer_results/free_units_{key}.csv", "r", encoding="ISO-8859-1") as f:
                reader = csv.DictReader(f)
                units_list = list(reader)
        except Exception:
            return {}

    for unit in units_list:
        free_time = datetime.datetime.strptime(
            unit['free_time'], "%Y-%m-%d %H:%M:%S")
        starting_delta = int((free_time.date() - DATE_RANGE_START.date()).days)
        initial_minutes = 0
        if free_time > DATE_RANGE_START:
            initial_timedelta = free_time - DATE_RANGE_START
            initial_minutes = initial_timedelta.days * \
                24 * 60 + initial_timedelta.seconds // 60
        starts_data[unit['unit_id']] = {
            'id': unit['location'],
            'minutes': initial_minutes,
            'start_range': max(WORKING_HOURS_START + (starting_delta * 1440), initial_minutes),
            'end_range': 9999999,
            'initial': True,
        }
    return starts_data


def build_locations(trips, distances, key=None):
    """
    Calculate locations dictionary with incremental ids. Also build pickup and deliveries tuples.
    The locations dictionary should look something like this:
        {
            1:{
                'id': ...,
                'minutes': ...,
            },
            2: {...},
            ...
        }
    The pickup and delivery list should look something like this:
        [
            (1, 2),
            (3, 4),
            ...
        ]
    """
    locations = {0: defaultdict(int)}
    counter = 1
    pickups = []

    sorted_trips = sorted(
        trips.values(), key=lambda trip: trip['inicio_minutes'])

    starts_data = read_units_locations(key)
    starts = []
    starts_definition = []
    demands = [0]

    for values in sorted_trips:
        starting_delta = int(
            (values['inicio_datetime'].date() - DATE_RANGE_START.date()).days)
        locations[counter] = {
            'id': values['origen'],
            'minutes': values['inicio_minutes'],
            'start_range': min(WORKING_HOURS_START + (starting_delta * 1440), values['inicio_minutes']),
            'end_range': max(WORKING_HOURS_END + (starting_delta * 1440), values['inicio_minutes']),
            'day_delta': starting_delta,
            'type': 'PICKUP'
        }
        destination_delta = ceil(distances[min(values['origen'], values['destino'])][max(
            values['origen'], values['destino'])] / 1440)
        destination_delta = (destination_delta + 1) * 2
        locations[counter + 1] = {
            'id': values['destino'],
            'minutes_start': values['inicio_minutes'],
            'minutes': values['fin_minutes'],
            'start_range': int(WORKING_HOURS_START + (starting_delta * 1440)),
            'end_range': int(WORKING_HOURS_END + (destination_delta * 1440)),
            'remove_ranges': [(WORKING_HOURS_END + (1440 * a) + 1, WORKING_HOURS_START + (a + 1) * 1440 - 1) for a in range(0, destination_delta)],
            'type': 'DELIVERY'
        }
        pickups.append((counter, counter + 1))
        demands.extend([1, -1])
        counter += 2

        if not starts_data.get(values['unidad']):
            starts_data[values['unidad']] = {
                'id': values['origen'],
                'minutes': values['inicio_minutes'],
                'start_range': min(WORKING_HOURS_START + (starting_delta * 1440), values['inicio_minutes']),
                'end_range': 9999999,
            }
        elif starts_data[values['unidad']].get('initial'):
            starts_data[values['unidad']]['initial'] = True
            starts_data[values['unidad']]['id'] = values['origen']

    for unidad, unidad_data in starts_data.items():
        locations[counter] = {
            'id': unidad_data['id'],
            'minutes': unidad_data['minutes'],
            'unidad_id': unidad,
            'start_range': unidad_data['start_range'],
            'end_range': unidad_data['end_range'],
            'type': 'START'
        }
        starts.append(counter)
        starts_definition.append(unidad)
        counter += 1
        demands.append(0)

    return locations, pickups, starts, demands, starts_definition


def get_location_distances(locations_set):
    """
    The distances dictionary should look something like this:
        {
            30000: {
                30001: ... # Distance in time.
                30002: ...
            },
            30001: {
                30002: ...
            },
            ...
        }
    """
    distancias = None

    try:
        conn = psycopg2.connect(
            database='assistcargo', user='*', password='*', host='localhost')
        cur = conn.cursor()

        cur.execute(
            """SELECT ubicacion_id_origen, ubicacion_id_destino, tiempo, distancia
            FROM distancias_mexico
            WHERE ubicacion_id_origen = ANY(%s) AND ubicacion_id_destino = ANY(%s);
            """,
            (locations_set, locations_set)
        )
        distancias = cur.fetchall()
    except Exception:
        traceback.print_exc()

    if distancias:
        # Armar diccionario con distancias reemplazando los tiempos de 0 por tiempos altos
        dist_dict = defaultdict(dict)
        km_dict = defaultdict(dict)
        for orig, dest, time, distancia in distancias:
            dist_dict[orig][dest] = int(time) * 1.6 if time != 0 else 99999
            km_dict[orig][dest] = float(distancia)
        return dict(dist_dict), dict(km_dict)
    else:
        return {}, {}


def remove_invalid_trips(trips, distances):
    valid_trips = {}

    for trip_id, values in trips.items():
        min_location = min(values['origen'], values['destino'])
        max_location = max(values['origen'], values['destino'])
        calculated_distance = distances[min_location][max_location]
        real_time = int(
            (values['fin_datetime'] - values['inicio_datetime']).total_seconds() // 60)

        if real_time >= calculated_distance * 0.5 and real_time < calculated_distance * 2:
            valid_trips[trip_id] = values

    return valid_trips


def get_trips_per_unit_type(trips):

    trips_per_unit_type = defaultdict(dict)

    for trip_id, trip_data in trips.items():
        trips_per_unit_type[trip_data['tipo_unidad']][trip_id] = trip_data

    return dict(trips_per_unit_type)


def get_lat_long(id):
    try:
        conn = psycopg2.connect(
            database='assistcargo', user='*', password='*', host='localhost')
        cur = conn.cursor()

        cur.execute(
            """
            SELECT latitud, longitud
            FROM ubicaciones_mexico
            WHERE ubicacion_id = %s;
            """,
            (id, )
        )
        data = cur.fetchall()
        return data[0][0], data[0][1]
    except Exception:
        traceback.print_exc()


def save_routes(trips, distances, kilometers, filename="trips.csv", tipo_unidad=None):
    trip_list = []

    for trip in trips.values():
        origin_lat, origin_long = get_lat_long(trip['origen'])
        dest_lat, dest_long = get_lat_long(trip['destino'])
        min_location = min(trip['origen'], trip['destino'])
        max_location = max(trip['origen'], trip['destino'])
        if min_location == max_location:
            continue
        trip_list.append({
            'origin_id': trip['origen'],
            'dest_id': trip['destino'],
            'origin_lat': origin_lat,
            'origin_long': origin_long,
            'dest_lat': dest_lat,
            'dest_long': dest_long,
            'unidad': trip['unidad'],
            'tipo_unidad': tipo_unidad or 'Not-defined',
            'real_start': trip['inicio_datetime'],
            'real_end': trip['fin_datetime'],
            'real_minutes': int((trip['fin_datetime'] - trip['inicio_datetime']).total_seconds() // 60),
            'calculated_minutes': distances[min_location][max_location],
            'calculated_kms': kilometers[min_location][max_location],
            'carga': 1 if trip.get('carga') else 0,
        })

    if os.path.isfile(filename):
        with open(filename, 'a', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, trip_list[0].keys())
            dict_writer.writerows(trip_list)
    else:
        with open(filename, 'w', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, trip_list[0].keys())
            dict_writer.writeheader()
            dict_writer.writerows(trip_list)


def read_trips(start, end, return_trips=False):
    global DATE_RANGE_START
    global DATE_RANGE_END

    DATE_RANGE_START = start
    DATE_RANGE_END = end

    with open("./csvs/mexico.csv", "r", encoding="ISO-8859-1") as f:
        reader = csv.DictReader(f)
        trip_list = list(reader)

    trips = {}
    for trip in trip_list:
        if not (trip['TIEMPOINICIOREALID'] and trip['TIEMPOFINID'] and trip['HR_COMPLETO'] and trip['HF_COMPLETO']):
            continue

        trips[trip['VIAJEID']] = {
            'origen': int(trip['UBICACIONORIGENID']),
            'destino': int(trip['UBICACIONDESTINOID']),
            'fecha_inicio': trip['TIEMPOINICIOREALID'],
            'fecha_fin': trip['TIEMPOFINID'],
            'hora_inicio': trip['HR_COMPLETO'],
            'hora_fin': trip['HF_COMPLETO'],
            'unidad': trip['UNIDADID'],
            'tipo_unidad_id': trip['TIPOUNIDADID'],
            'tipo_unidad': trip['TIPOUNIDAD']
        }

    trips_per_unit_type = get_trips_per_unit_type(trips)

    if return_trips:
        trips, locations_set = filter_trips_range(trips)

        trips = calculate_start_end_datetime(trips)

        distances, kilometers = get_location_distances(locations_set)

        trips = remove_invalid_trips(trips, distances)

        locations, pickups, starts, demands, starts_definition = build_locations(
            trips, distances)

        print("Amount of trucks originally used:")
        print(len({d['unidad'] for d in trips.values()}))
        print("Amount of trips to optimize:")
        print(len(trips))

        return trips, distances, kilometers
    # import ipdb
    for key in trips_per_unit_type:
        # ipdb.set_trace()
        trips, locations_set = filter_trips_range(
            trips_per_unit_type[key], key)

        trips = calculate_start_end_datetime(trips)

        distances, kilometers = get_location_distances(locations_set)

        if not distances:
            continue

        trips = remove_invalid_trips(trips, distances)

        if not trips:
            continue

        locations, pickups, starts, demands, starts_definition = build_locations(
            trips, distances, key)

        print(f"Amount of {key} trucks originally used:")
        print(len({d['unidad'] for d in trips.values()}))
        print("Amount of trips to optimize:")
        print(len(trips))

        # if key == 'Thorton':
        #     save_routes(trips, distances, kilometers,
        #                 f'optimizer_results/result_original.csv', key)

        yield locations, pickups, starts, demands, distances, kilometers, starts_definition, key, (len({d['unidad'] for d in trips.values()}), len(trips))


def save_units_use(units_use, filename="free_units.csv"):
    units_list = []

    for unit, data in units_use.items():
        units_list.append({
            'unit_id': unit,
            'free_time': data['time'],
            'location': data['location'],
        })

    with open(filename, 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, units_list[0].keys())
        dict_writer.writeheader()
        dict_writer.writerows(units_list)


if __name__ == '__main__':
    trips, distances, kilometers = read_trips(return_trips=True)
    save_routes(trips, distances, kilometers)
