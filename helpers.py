import csv
import datetime
import psycopg2
import traceback
import re
from collections import defaultdict

DATE_RANGE_START = datetime.datetime(2021, 2, 7, 0)
DATE_RANGE_END = datetime.datetime(2021, 2, 9, 0)
HOUR_REGEX = r"\d{1,2}:\d{1,2}:\d{1,2}\s[ap].m."

def build_locations_set(trips):
    "Build set of locations that will be used in the optimization using the trips from the database."
    locations = set([trip['origen'] for trip in trips.values()]) | set([trip['destino'] for trip in trips.values()])
    ubicaciones = []
    try:
        conn = psycopg2.connect(database='_', user='_', password='_', host='_')
        cur = conn.cursor()

        cur.execute(
            """
            SELECT ubicacion_id
            FROM ubicaciones
            WHERE ubicacion_id = ANY(%s);
            """,
            (list(locations), )
        )
        ubicaciones_disponibles = cur.fetchall()
        ubicaciones = [a[0] for a in ubicaciones_disponibles]
    except Exception:
        traceback.print_exc()

    return ubicaciones

def filter_trips_range(trips):
    """"
    Filter the trips that occur on the date range and discard others. Also discard trips that don't
    have a clear way of calculating start and end date.
    """

    filtered_trips = {}
    unidades = []
    for trip_id, data in trips.items():
        try:
            hora_inicio_re = re.fullmatch(HOUR_REGEX, data['hora_inicio'])
            hora_fin_re = re.fullmatch(HOUR_REGEX, data['hora_fin'])

            if not (hora_inicio_re and hora_fin_re):
                continue

            hora_inicio = data['hora_inicio'].upper().replace(':', '-').replace('.', '')
            hora_fin = data['hora_fin'].upper().replace(':', '-').replace('.', '')
            hora_inicio = datetime.datetime.strptime(hora_inicio, "%I-%M-%S %p")
            hora_fin = datetime.datetime.strptime(hora_fin, "%I-%M-%S %p")

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
            }
            unidades.append(data['unidad'])
        except Exception:
            traceback.print_exc()
            continue
    print("Amount of trucks originally used:")
    print(len(set(unidades)))
    print("Amount of trips to optimize:")
    print(len(filtered_trips))

    locations = build_locations_set(filtered_trips)

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
        initial_minutes = initial_timedelta.days * 24 * 60 + initial_timedelta.seconds // 60

        end_timedelta = data['fin_datetime'] - DATE_RANGE_START
        end_minutes = end_timedelta.days * 24 * 60 + end_timedelta.seconds // 60

        trips_dict[trip_id] = {
                'origen': data['origen'],
                'destino': data['destino'],
                'inicio_datetime': data['inicio_datetime'],
                'fin_datetime': data['fin_datetime'],
                'inicio_minutes': initial_minutes,
                'fin_minutes': end_minutes,
            }

    return trips_dict

def build_locations(trips):
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
    locations = {}
    counter = 1
    pickups = []

    sorted_trips = sorted(trips.values(), key=lambda trip: trip['inicio_minutes'])

    for values in sorted_trips:
        locations[counter] = {
            'id': values['origen'],
            'minutes': values['inicio_minutes']
        }
        locations[counter + 1] = {
            'id': values['destino'],
            'minutes': values['fin_minutes']
        }
        pickups.append((counter, counter + 1))
        counter += 2

    return locations, pickups

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
        conn = psycopg2.connect(database='_', user='_', password='_', host='_')
        cur = conn.cursor()

        cur.execute(
            """SELECT ubicacion_id_origen, ubicacion_id_destino, tiempo
            FROM distancias
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
        for orig, dest, time in distancias:
            dist_dict[orig][dest] = int(time) if time != 0 else 99999
        return dict(dist_dict)
    else:
        return {}

def read_trips():
    with open("CSVArg.csv", "r", encoding = "ISO-8859-1") as f:
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
        }

    trips, locations_set = filter_trips_range(trips)

    trips = calculate_start_end_datetime(trips)

    distances = get_location_distances(locations_set)

    locations, pickups = build_locations(trips)

    return locations, distances, pickups


if __name__ == '__main__':
    read_trips()
