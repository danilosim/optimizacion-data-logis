import csv
import requests

API_KEY = 'AIzaSyDZe6aGOFQI_5yzd-hasPikD26jdEAPx9k'
GEOCODING_URL = 'https://maps.googleapis.com/maps/api/geocode/json?address={address}&region=ar&key={key}'
DISTANCE_URL = 'http://router.project-osrm.org/table/v1/driving/{locations}?annotations=duration'

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


address_1 = f"{ubicaciones['31390']['domicilio']}, {ubicaciones['31390']['localidad']}"
address_2 = f"{ubicaciones['31325']['domicilio']}, {ubicaciones['31325']['localidad']}"

result_1 = requests.get(GEOCODING_URL.format(address=address_1, key=API_KEY))
result_1 = result_1.json()
result_2 = requests.get(GEOCODING_URL.format(address=address_2, key=API_KEY))
result_2 = result_2.json()

ubicaciones['31390']['geolocation'] = result_1['results'][0]['geometry']['location']
ubicaciones['31325']['geolocation'] = result_2['results'][0]['geometry']['location']

locations_list = ";".join([",".join([str(a['lat']), str(a['lng'])]) for a in [ubicaciones['31390']['geolocation'], ubicaciones['31325']['geolocation']]])

# locations_list = '-38.71276,-62.27596;-45.86653,-67.52117'
locations_list = "-58.86824,-34.42020;-58.82420,-34.67602"

print(locations_list)

result_3 = requests.get(DISTANCE_URL.format(locations=locations_list))
result_3 = result_3.json()

print(result_3)


