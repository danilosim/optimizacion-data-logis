import csv
import psycopg2
import traceback

def get_location_distances():
    distancias = None
    try:
        conn = psycopg2.connect(database='_', user='_', password='_', host='_')
        cur = conn.cursor()

        cur.execute("SELECT ubicacion_id_origen, ubicacion_id_destino, tiempo FROM distancias;")
        distancias = cur.fetchall()
    except Exception:
        traceback.print_exc()

    if distancias:
        distancias_list = []
        for orig, dest, time in distancias:
            distancias_list.append({
                'ORIGEN': orig,
                'DESTINO': dest,
                'DISTANCIA': int(time) if time != 0 else -1
            })
            distancias_list.append({
                'ORIGEN': dest,
                'DESTINO': orig,
                'DISTANCIA': int(time) if time != 0 else -1
            })
        return distancias_list
    else:
        return []


if __name__ == '__main__':
    distances = get_location_distances()

    keys = distances[0].keys()
    with open('distances.csv', 'w', newline='')  as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(distances)
