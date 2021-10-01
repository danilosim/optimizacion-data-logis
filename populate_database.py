import csv
import psycopg2
import requests
import sys
import traceback

API_KEY = 'AIzaSyDZe6aGOFQI_5yzd-hasPikD26jdEAPx9k'
GEOCODING_URL = 'https://maps.googleapis.com/maps/api/geocode/json?address={address}&region=ar&key={key}'
DISTANCE_URL = 'http://router.project-osrm.org/table/v1/driving/{locations}?annotations=distance,duration&sources=0'

print('Connecting to database...')

conn = psycopg2.connect(database='assistcargo', user='postgres', password='totiDRS0753', host='localhost')

cur = conn.cursor()

def query_geo(ubicacion):
    try:
        address = f"{ubicacion['domicilio']}, {ubicacion['localidad']}"
        result = requests.get(GEOCODING_URL.format(address=address, key=API_KEY))
        result_json = result.json()

        geolocation = result_json['results'][0]['geometry']['location']

        return geolocation
    except Exception:
        traceback.print_exc()
        return False

def insert_entry(ubicacion_id, domicilio, localidad, latitud, longitud):
    cur.execute(
        "INSERT INTO ubicaciones (ubicacion_id, direccion, localidad, latitud, longitud) VALUES (%s, %s, %s, %s, %s);",
        (ubicacion_id, domicilio, localidad, latitud, longitud),
    )
    conn.commit()

def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

def distancias_faltantes(ubicacion_id, list_ubicaciones):
    cur.execute("SELECT ubicacion_id_destino FROM distancias WHERE ubicacion_id_destino = ANY(%s) AND ubicacion_id_origen = %s;", (list_ubicaciones, ubicacion_id))
    ubicaciones_existentes = cur.fetchall()
    cur.execute("SELECT ubicacion_id_origen FROM distancias WHERE ubicacion_id_origen = ANY(%s) AND ubicacion_id_destino = %s;", (list_ubicaciones, ubicacion_id))
    ubicaciones_existentes_2 = cur.fetchall()
    ubicaciones_faltantes = set(list_ubicaciones) - set([a[0] for a in ubicaciones_existentes if ubicaciones_existentes]) - set([a[0] for a in ubicaciones_existentes_2 if ubicaciones_existentes_2]) - {ubicacion_id}
    return list(ubicaciones_faltantes)

def insert_time_distance(origin, destination, distance, time):
    try:
        cur.execute(
            "INSERT INTO distancias (ubicacion_id_origen, ubicacion_id_destino, distancia, tiempo) VALUES (%s, %s, %s, %s);",
            (min([origin, destination]), max([origin, destination]), distance, time),
        )
    except Exception:
        traceback.print_exc()
    finally:
        conn.commit()

def guardar_resultados_distancias(ids, distance_results, time_results):
    origin = ids[0]
    distance_results = [round(i / 1000, 2) if i else 0 for i in distance_results ]
    time_results = [int(i / 60) if i else 0 for i in time_results]

    for idx, id in enumerate(ids):
        if id == origin:
            continue
        insert_time_distance(origin, id, distance_results[idx], time_results[idx])

def calcular_distancias(origin, destinations, info_dict):
    origin_string = f"{info_dict[origin]['longitude']},{info_dict[origin]['latitude']}"
    destination_strings = ';'.join([f"{info_dict[i]['longitude']},{info_dict[i]['latitude']}" for i in destinations])
    full_string = f"{origin_string};{destination_strings}"
    try:
        formated_url = DISTANCE_URL.format(locations=full_string)
        result = requests.get(formated_url)
        result_json = result.json()
        ids = destinations
        ids.insert(0, origin)

        guardar_resultados_distancias(ids=ids, distance_results=result_json['distances'][0], time_results=result_json['durations'][0])
    except Exception:
        traceback.print_exc()
        sys.exit(1)

print('Reading csv...')

with open("./csvs/CSVArg.csv", "r") as f:
    reader = csv.DictReader(f)
    trip_list = list(reader)


ubicaciones = {
    trip['UBICACIONORIGENID']: {
        'domicilio': trip['UBICACION_ORIGEN'],
        'localidad': trip['LOCALIDAD_ORI']
    } for trip in trip_list
    if trip['UBICACION_ORIGEN'] and trip['LOCALIDAD_ORI']
}

ubicaciones.update({
    trip['UBICACIONDESTINOID']: {
        'domicilio': trip['UBICACION_DESTINO'],
        'localidad': trip['LOCALIDAD_DEST']
    } for trip in trip_list
    if trip['UBICACION_DESTINO'] and trip['LOCALIDAD_DEST']
})

# for ubicacion_id, value in ubicaciones.items():
#     cur.execute("SELECT * FROM ubicaciones WHERE ubicacion_id = %s;", (ubicacion_id, ))
#     object = cur.fetchone()
#     geo = None
#     if not object:
#         geo = query_geo(value)
#     if geo:
#         insert_entry(ubicacion_id, value['domicilio'], value['localidad'], geo['lat'], geo['lng'])

print('Getting locations...')

cur.execute("SELECT * FROM ubicaciones")
ubicaciones = cur.fetchall()

set_ubicaciones_id = list(set(item[0] for item in ubicaciones))

ubicaciones = {item[0]: {'latitude': str(round(item[3], 5)), 'longitude': str(round(item[4], 5))} for item in ubicaciones}

ubicaciones_remaining = len(ubicaciones)

for ubicacion_id, values in ubicaciones.items():
    print(f"\n{str(int((len(ubicaciones) - ubicaciones_remaining) * 100 / len(ubicaciones)))}% done. {str(ubicaciones_remaining)} locations remaining...")
    dist_not_calculated = distancias_faltantes(ubicacion_id, set_ubicaciones_id)
    total = len(dist_not_calculated)
    for dist_to_calculate in batch(dist_not_calculated, 100):
        print(f"    {str(total)} remaining calculations for {str(ubicacion_id)}...")
        calcular_distancias(ubicacion_id, dist_to_calculate, ubicaciones)
        total -= 100
    ubicaciones_remaining -= 1
print("\n\n100% DONE!!")
