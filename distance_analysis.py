import csv
from statistics import mean

trip_list = 0

with open("./results/trips.csv", "r", encoding = "ISO-8859-1") as f:
    reader = csv.DictReader(f)
    trip_list = list(reader)

valid_trips = [a for a in trip_list if int(a['real_minutes']) > int(a['calculated_minutes'])]
lower_50 = [a for a in valid_trips if int(a['real_minutes']) <= 50]
lower_100 = [a for a in valid_trips if int(a['real_minutes']) <= 100 and int(a['real_minutes']) > 50]
lower_500 = [a for a in valid_trips if int(a['real_minutes']) <= 500 and int(a['real_minutes']) > 100]
lower_1000 = [a for a in valid_trips if int(a['real_minutes']) <= 1000 and int(a['real_minutes']) > 500]
upper = [a for a in valid_trips if int(a['real_minutes']) > 1000]

number_of_trips = len(trip_list)
number_of_valid_trips = len(valid_trips)
percentage_of_valid_trips = round(number_of_valid_trips * 100 / number_of_trips, 2)

absolute_deviation = [int(trip['real_minutes']) - int(trip['calculated_minutes']) for trip in trip_list]
max_positive_absolute_deviation = max(absolute_deviation)
max_negative_absolute_deviation = min(absolute_deviation)
avg_positive_absolute_deviation = round(mean([e for e in absolute_deviation if e >= 0]))
avg_negative_absolute_deviation = [e for e in absolute_deviation if e < 0]
avg_negative_absolute_deviation = round(mean(avg_negative_absolute_deviation)) if avg_negative_absolute_deviation else 0
avg_overall_absolute_deviation = round(mean([abs(e) for e in absolute_deviation]))

relative_deviation_zips = zip(absolute_deviation, [int(trip['real_minutes']) for trip in trip_list])
relative_deviation = [round(abs_dev * 100 / real_distance, 2) for abs_dev, real_distance in relative_deviation_zips if real_distance]
max_positive_relative_deviation = max(relative_deviation)
max_negative_relative_deviation = min(relative_deviation)
avg_positive_relative_deviation = round(mean([e for e in relative_deviation if e >= 0]))
avg_negative_relative_deviation = [e for e in relative_deviation if e < 0]
avg_negative_relative_deviation = round(mean(avg_negative_relative_deviation)) if avg_negative_relative_deviation else 0
avg_overall_relative_deviation = round(mean([abs(e) for e in relative_deviation]))

# <=50
absolute_deviation_lower_50 = [int(trip['real_minutes']) - int(trip['calculated_minutes']) for trip in lower_50]
avg_absolute_deviation_lower_50 = round(mean(absolute_deviation_lower_50))
relative_deviation_zips_lower_50 = zip(absolute_deviation_lower_50, [int(trip['real_minutes']) for trip in lower_50])
relative_deviation_lower_50 = [round(abs_dev * 100 / real_distance, 2) for abs_dev, real_distance in relative_deviation_zips_lower_50]
avg_relative_deviation_lower_50 = round(mean(relative_deviation_lower_50))
# >=100
absolute_deviation_lower_100 = [int(trip['real_minutes']) - int(trip['calculated_minutes']) for trip in lower_100]
avg_absolute_deviation_lower_100 = round(mean(absolute_deviation_lower_100))
relative_deviation_zips_lower_100 = zip(absolute_deviation_lower_100, [int(trip['real_minutes']) for trip in lower_100])
relative_deviation_lower_100 = [round(abs_dev * 100 / real_distance, 2) for abs_dev, real_distance in relative_deviation_zips_lower_100]
avg_relative_deviation_lower_100 = round(mean(relative_deviation_lower_100))
# >=500
absolute_deviation_lower_500 = [int(trip['real_minutes']) - int(trip['calculated_minutes']) for trip in lower_500]
avg_absolute_deviation_lower_500 = round(mean(absolute_deviation_lower_500))
relative_deviation_zips_lower_500 = zip(absolute_deviation_lower_500, [int(trip['real_minutes']) for trip in lower_500])
relative_deviation_lower_500 = [round(abs_dev * 100 / real_distance, 2) for abs_dev, real_distance in relative_deviation_zips_lower_500]
avg_relative_deviation_lower_500 = round(mean(relative_deviation_lower_500))
# >=1000
absolute_deviation_lower_1000 = [int(trip['real_minutes']) - int(trip['calculated_minutes']) for trip in lower_1000]
avg_absolute_deviation_lower_1000 = round(mean(absolute_deviation_lower_1000))
relative_deviation_zips_lower_1000 = zip(absolute_deviation_lower_1000, [int(trip['real_minutes']) for trip in lower_1000])
relative_deviation_lower_1000 = [round(abs_dev * 100 / real_distance, 2) for abs_dev, real_distance in relative_deviation_zips_lower_1000]
avg_relative_deviation_lower_1000 = round(mean(relative_deviation_lower_1000))
# >1000
absolute_deviation_upper = [int(trip['real_minutes']) - int(trip['calculated_minutes']) for trip in upper]
avg_absolute_deviation_upper = round(mean(absolute_deviation_upper))
relative_deviation_zips_upper = zip(absolute_deviation_upper, [int(trip['real_minutes']) for trip in upper])
relative_deviation_upper = [round(abs_dev * 100 / real_distance, 2) for abs_dev, real_distance in relative_deviation_zips_upper]
avg_relative_deviation_upper = round(mean(relative_deviation_upper))

print('\nTrips distance analysis\n\n')
print(f'- Number of trips: {number_of_trips}')
print(f'- Number of valid trips: {number_of_valid_trips}')
print(f'- Percentage of valid trips: {percentage_of_valid_trips}%\n\n')
print('Absolute deviation analysis:\n')
print(f'- Maximum positive absolute deviation: {max_positive_absolute_deviation} minutes')
print(f'- Maximum negative absolute deviation: {max_negative_absolute_deviation} minutes')
print(f'- Average positive absolute deviation: {avg_positive_absolute_deviation} minutes')
print(f'- Average negative absolute deviation: {avg_negative_absolute_deviation} minutes')
print(f'- Average overall absolute deviation: {max_positive_absolute_deviation} minutes\n\n')
print('Relative deviation analysis:\n')
print(f'- Maximum positive relative deviation: {max_positive_relative_deviation}%')
print(f'- Maximum negative relative deviation: {max_negative_relative_deviation}%')
print(f'- Average positive relative deviation: {avg_positive_relative_deviation}%')
print(f'- Average negative relative deviation: {avg_negative_relative_deviation}%')
print(f'- Average overall relative deviation: {max_positive_relative_deviation}%\n\n')
print('Analysis for valid trips under 50 minutes (real duration):\n')
print(f'- Average absolute deviation: {avg_absolute_deviation_lower_50} minutes')
print(f'- Average relative deviation: {avg_relative_deviation_lower_50}%\n')
print('Analysis for valid trips under 100 minutes (real duration):\n')
print(f'- Average absolute deviation: {avg_absolute_deviation_lower_100} minutes')
print(f'- Average relative deviation: {avg_relative_deviation_lower_100}%\n')
print('Analysis for valid trips under 500 minutes (real duration):\n')
print(f'- Average absolute deviation: {avg_absolute_deviation_lower_500} minutes')
print(f'- Average relative deviation: {avg_relative_deviation_lower_500}%\n')
print('Analysis for valid trips under 1000 minutes (real duration):\n')
print(f'- Average absolute deviation: {avg_absolute_deviation_lower_1000} minutes')
print(f'- Average relative deviation: {avg_relative_deviation_lower_1000}%\n')
print('Analysis for valid trips over 1000 minutes (real duration):\n')
print(f'- Average absolute deviation: {avg_absolute_deviation_upper} minutes')
print(f'- Average relative deviation: {avg_relative_deviation_upper}%\n')


