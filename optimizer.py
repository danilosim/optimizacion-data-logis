import datetime

from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp

from helpers import read_trips, save_routes, save_units_use


SOLVER_STATUS_MAP = {
    0: 'ROUTING_NOT_SOLVED: Problem not solved yet.\n\n',
    1: 'ROUTING_SUCCESS: Problem solved successfully.\n\n',
    2: 'ROUTING_FAIL: No solution found to the problem.\n\n',
    3: 'ROUTING_FAIL_TIMEOUT: Time limit reached before finding a solution.\n\n',
    4: 'ROUTING_INVALID: Model, model parameters, or flags are not valid.\n\n',
}

distance_matrix = None
kilometers_matrix = None
starts_definition = None

DATE_RANGE_START = datetime.datetime(2021, 2, 1, 0)
DATE_RANGE_END = datetime.datetime(2021, 2, 28, 0)


def create_data_model():
    """Stores the data for the problem."""
    global current_date
    for trips_data in read_trips(current_date, current_date + datetime.timedelta(days=1)):
        try:
            locations, pickups, starts, demands, distances, kilometers, starts_data, key, old_trip_info = trips_data
        except Exception:
            return None
        global distance_matrix
        distance_matrix = distances
        global kilometers_matrix
        kilometers_matrix = kilometers
        global starts_definition
        starts_definition = starts_data
        data = {}
        data['demands'] = demands
        data['locations'] = locations
        data['distances'] = distances
        data['starts'] = starts
        data['pickups_deliveries'] = pickups
        data['ends'] = [0 for _ in range(len(starts))]
        data['num_vehicles'] = len(starts)
        data['key'] = key
        data['old_trip_info'] = old_trip_info
        yield data


def print_solution(data, manager, routing, solution, file_counter):
    """Prints solution on console."""
    print(f'Objective: {solution.ObjectiveValue()}\n\n')
    time_dimension = routing.GetDimensionOrDie('Time')
    total_time = 0
    vehicles_used = 0
    total_trips_counter = 0
    trips = {}
    free_units = {}
    print_lines = []
    for vehicle_id in range(data['num_vehicles']):
        index = routing.Start(vehicle_id)
        plan_output = '\nRoute for vehicle {} - {}:\n'.format(
            vehicle_id + 1, starts_definition[vehicle_id])
        counter = 0
        vehicles_locations = []
        unused_vehicle_time = None
        while not routing.IsEnd(index):
            time_var = time_dimension.CumulVar(index)
            if counter == 1:
                vehicles_used += 1
                plan_output += '\nLocation {0} Time({1},{2})'.format(
                    data['locations'][manager.IndexToNode(
                        index)]['id'], solution.Min(time_var),
                    solution.Max(time_var))
            elif counter > 1:
                plan_output += ' -> Location {0} Time({1},{2})'.format(
                    data['locations'][manager.IndexToNode(
                        index)]['id'], solution.Min(time_var),
                    solution.Max(time_var))

            if counter != 0:
                vehicles_locations.append(
                    (
                        data['locations'][manager.IndexToNode(index)]['id'],
                        (solution.Max(time_var) + solution.Min(time_var)) // 2,
                        manager.IndexToNode(index),
                    )
                )
            else:
                unused_vehicle_time = (
                    data['locations'][manager.IndexToNode(index)]['id'],
                    int(solution.Min(time_var)),
                )
            index = solution.Value(routing.NextVar(index))
            counter += 1

        for i in range(len(vehicles_locations) - 1):
            trips[total_trips_counter] = {
                'origen': vehicles_locations[i][0],
                'destino': vehicles_locations[i+1][0],
                'unidad': starts_definition[vehicle_id],
                'inicio_datetime': current_date + datetime.timedelta(minutes=vehicles_locations[i][1]),
                'fin_datetime': current_date + datetime.timedelta(minutes=vehicles_locations[i+1][1]),
                'carga': True if vehicles_locations[i][2] + 1 == vehicles_locations[i+1][2] else False,
            }
            total_trips_counter += 1

        if vehicles_locations:
            free_units[starts_definition[vehicle_id]] = {
                'time': current_date + datetime.timedelta(minutes=vehicles_locations[-1][1]),
                'location': vehicles_locations[-1][0],
            }
        else:
            free_units[starts_definition[vehicle_id]] = {
                'time': current_date + datetime.timedelta(minutes=unused_vehicle_time[1]),
                'location': unused_vehicle_time[0],
            }

        time_var = time_dimension.CumulVar(index)
        plan_output += '\nTime of the route: {} minutes\n'.format(
            solution.Min(time_var))
        if counter > 1:
            print(plan_output)
            print_lines.append(plan_output)
        total_time += solution.Min(time_var)

    print('\nTotal time of all routes: {} minutes'.format(total_time))
    print(f'\nAmount of trucks used by optimizer: {vehicles_used}')
    print('\n\n')

    print_lines.append(
        '\nTotal time of all routes: {} minutes'.format(total_time))
    print_lines.append(
        f'\nAmount of trucks used by optimizer: {vehicles_used}\n\n')

    with open(f'optimizer_results/{str(file_counter)}.txt', 'a') as f:
        f.writelines(print_lines)

    save_routes(trips, distance_matrix, kilometers_matrix,
                f'optimizer_results/result.csv', data["key"])
    save_units_use(
        free_units, f'optimizer_results/free_units_{data["key"]}.csv')


def optimize(data, counter):

    # Create the routing index manager.
    manager = pywrapcp.RoutingIndexManager(len(data['locations']),
                                           data['num_vehicles'], data['starts'],
                                           data['ends'])

    # Create Routing Model.
    routing = pywrapcp.RoutingModel(manager)

    # Define time cost of each arc.
    def time_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)

        if from_node == 0 or to_node == 0 or from_node == to_node:
            return 0

        from_location = data['locations'][from_node]['id']
        to_location = data['locations'][to_node]['id']

        if from_location == to_location:
            return 0
        try:
            distance = distance_matrix[min(from_location, to_location)][max(
                from_location, to_location)]
        except Exception:
            distance = 99999999999999

        return distance

    transit_callback_index = routing.RegisterTransitCallback(time_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

    # Add Distance constraint.
    dimension_name = 'Time'
    routing.AddDimension(
        transit_callback_index,
        60*10000000,  # slack
        60*24*15000,  # vehicle maximum travel time
        False,  # start cumul to zero
        dimension_name)
    time_dimension = routing.GetDimensionOrDie(dimension_name)
    # time_dimension.SetGlobalSpanCostCoefficient(100)

    # Define capacity constraints
    def demand_callback(from_index):
        """Returns the demand of the node."""
        # Convert from routing variable Index to demands NodeIndex.
        from_node = manager.IndexToNode(from_index)
        return data['demands'][from_node]

    demand_callback_index = routing.RegisterUnaryTransitCallback(
        demand_callback)
    routing.AddDimensionWithVehicleCapacity(
        demand_callback_index,
        0,  # null capacity slack
        [1 for _ in range(data['num_vehicles'])],  # vehicle maximum capacities
        True,  # start cumul to zero
        'Capacity'
    )

    # Add time window constraints for each location except start_nodes.
    for location_idx, location_data in data['locations'].items():
        if location_idx in data['starts'] or location_idx in data['ends'] or location_idx == 0:
            continue
        index = manager.NodeToIndex(location_idx)
        # time_dimension.CumulVar(index).SetRange(
        #     location_data['start_range'],
        #     location_data['end_range']
        # )
        # if location_data.get('remove_ranges'):
        #     for lapse in location_data['remove_ranges']:
        #         time_dimension.CumulVar(index).RemoveInterval(lapse[0], lapse[1])
        if location_data['type'] == 'PICKUP':
            time_dimension.CumulVar(index).SetRange(
                location_data['minutes'],
                1440 + (location_data['day_delta'] * 1440)
            )
        elif location_data['type'] == 'DELIVERY':
            time_dimension.CumulVar(index).SetRange(
                location_data['minutes_start'],
                9999999
            )

    # Add time window constraints for each vehicle start node.
    for vehicle_id in range(data['num_vehicles']):
        index = routing.Start(vehicle_id)
        location = data['starts'][vehicle_id]
        # time_dimension.CumulVar(index).SetRange(
        #     data['locations'][location]['start_range'],
        #     data['locations'][location]['end_range'],
        # )
        time_dimension.CumulVar(index).SetRange(
            data['locations'][location]['minutes'],
            data['locations'][location]['end_range'],
        )

    # Instantiate route start and end times to produce feasible times.
    for i in range(data['num_vehicles']):
        routing.AddVariableMinimizedByFinalizer(
            time_dimension.CumulVar(routing.Start(i)))
        routing.AddVariableMinimizedByFinalizer(
            time_dimension.CumulVar(routing.End(i)))

    for request in data['pickups_deliveries']:
        pickup_index = manager.NodeToIndex(request[0])
        delivery_index = manager.NodeToIndex(request[1])
        routing.AddPickupAndDelivery(pickup_index, delivery_index)
        routing.solver().Add(
            routing.VehicleVar(
                pickup_index) == routing.VehicleVar(delivery_index)
        )
        routing.solver().Add(
            time_dimension.CumulVar(
                pickup_index) <= time_dimension.CumulVar(delivery_index)
        )

    # Setting first solution heuristic.
    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    # search_parameters.first_solution_strategy = (routing_enums_pb2.FirstSolutionStrategy.AUTOMATIC)
    # search_parameters.first_solution_strategy = (routing_enums_pb2.FirstSolutionStrategy.PARALLEL_CHEAPEST_INSERTION)
    # search_parameters.first_solution_strategy = (routing_enums_pb2.FirstSolutionStrategy.ALL_UNPERFORMED)
    search_parameters.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC)
    # search_parameters.local_search_metaheuristic = (routing_enums_pb2.LocalSearchMetaheuristic.AUTOMATIC)
    search_parameters.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH)
    search_parameters.time_limit.seconds = 60 * 2

    print("Initializing solving process...")

    # Solve the problem.
    solution = routing.SolveWithParameters(search_parameters)

    with open(f'optimizer_results/{str(counter)}.txt', 'a') as f:
        f.write(
            f"Amount of {data['key']} trucks originally used: {data['old_trip_info'][0]}\n\n")
        f.write(f"Amount of trips to optimize: {data['old_trip_info'][1]}\n\n")
        f.write(SOLVER_STATUS_MAP[routing.status()])

    print('\n')
    print(SOLVER_STATUS_MAP[routing.status()])
    print('\n')
    # Print solution on console.
    if solution:
        print_solution(data, manager, routing, solution, counter)


def date_generator():
    global current_date
    current_date = DATE_RANGE_START
    while current_date < DATE_RANGE_END:
        yield current_date, current_date + datetime.timedelta(days=1)
        current_date = current_date + datetime.timedelta(days=1)


if __name__ == "__main__":
    counter = 0
    for _ in date_generator():
        for data in create_data_model():
            # continue
            if data and data['key'] == 'Thorton':
                optimize(data, counter)
        counter += 1
        print("Done")
