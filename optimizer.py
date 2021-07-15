from collections import defaultdict
import traceback

from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp

from helpers import read_trips


SOLVER_STATUS_MAP = {
    0: 'ROUTING_NOT_SOLVED: Problem not solved yet.',
    1: 'ROUTING_SUCCESS: Problem solved successfully.',
    2: 'ROUTING_FAIL: No solution found to the problem.',
    3: 'ROUTING_FAIL_TIMEOUT: Time limit reached before finding a solution.',
    4: 'ROUTING_INVALID: Model, model parameters, or flags are not valid.',
}
VEHICLES = 200
MINUTE_RANGE = 4 * 60

def create_starting_locations(locations):
    counter = max([a for a in locations.keys()]) + 1
    starting_points = range(1, VEHICLES*2, 2)
    starting_indexes = []
    for i in starting_points:
        locations[counter] = {
            'id': locations[i]['id'],
            'minutes': locations[i]['minutes']
        }
        starting_indexes.append(counter)
        counter += 1
    locations[0] = defaultdict(int)
    return locations, starting_indexes

def create_demands(locations_length):
    demands = [0] # For the finishing location
    next = 1
    for _ in range(locations_length - VEHICLES):
        demands.append(next)
        next = -next
    demands.extend([0 for _ in range(VEHICLES)])
    return demands

def create_data_model():
    """Stores the data for the problem."""
    locations, distances, pickups = read_trips()
    data = {}
    locations, starts = create_starting_locations(locations)
    data['demands'] = create_demands(len(locations) - 1)
    data['locations'] = locations
    data['distances'] = distances
    data['starts'] = starts
    data['pickups_deliveries'] = pickups
    data['ends'] = [0 for _ in range(VEHICLES)]
    data['num_vehicles'] = VEHICLES
    return data

def print_solution(data, manager, routing, solution):
    """Prints solution on console."""
    print(f'Objective: {solution.ObjectiveValue()}\n\n')
    time_dimension = routing.GetDimensionOrDie('Time')
    total_time = 0
    vehicles_used = 0
    for vehicle_id in range(data['num_vehicles']):
        index = routing.Start(vehicle_id)
        plan_output = 'Route for vehicle {}:\n'.format(vehicle_id + 1)
        counter = 0
        while not routing.IsEnd(index):
            time_var = time_dimension.CumulVar(index)
            if counter == 1:
                vehicles_used += 1
                plan_output += 'Location {0} Time({1},{2})'.format(
                    data['locations'][manager.IndexToNode(index)]['id'], solution.Min(time_var),
                    solution.Max(time_var))
            elif counter > 1:
                plan_output += ' -> Location {0} Time({1},{2})'.format(
                   data['locations'][manager.IndexToNode(index)]['id'], solution.Min(time_var),
                    solution.Max(time_var))
            index = solution.Value(routing.NextVar(index))
            counter += 1
        time_var = time_dimension.CumulVar(index)
        plan_output += '\nTime of the route: {} minutes\n'.format(solution.Min(time_var))
        print(plan_output)
        total_time += solution.Min(time_var)
    print('\nTotal time of all routes: {} minutes'.format(total_time))
    print(f'\nAmount of trucks used by optimizer: {vehicles_used}')

def main():
    """Entry point of the program."""
    # Instantiate the data problem.
    print("Initializing process...")
    data = create_data_model()

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

        return data['distances'][min(from_location, to_location)][max(from_location, to_location)]

    transit_callback_index = routing.RegisterTransitCallback(time_callback)
    # routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

    # Add Distance constraint.
    dimension_name = 'Time'
    routing.AddDimension(
        transit_callback_index,
        10000000000000000,  # slack
        10000000000000000,  # vehicle maximum travel time
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

    demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
    routing.AddDimensionWithVehicleCapacity(
        demand_callback_index,
        0,  # null capacity slack
        [1 for _ in range(VEHICLES)],  # vehicle maximum capacities
        True,  # start cumul to zero
        'Capacity'
    )

    # Add time window constraints for each location except start_nodes.
    for location_idx, location_data in data['locations'].items():
        if location_idx in data['starts'] or location_idx == 0:
            continue
        index = manager.NodeToIndex(location_idx)
        time_dimension.CumulVar(index).SetRange(max(0, location_data['minutes'] - MINUTE_RANGE), location_data['minutes'] + MINUTE_RANGE)

    # Add time window constraints for each vehicle start node.
    for vehicle_id in range(data['num_vehicles']):
        index = routing.Start(vehicle_id)
        location = data['starts'][vehicle_id]
        time_dimension.CumulVar(index).SetRange(
            max(0, data['locations'][location]['minutes'] - MINUTE_RANGE),
            data['locations'][location]['minutes'] + MINUTE_RANGE,
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
            routing.VehicleVar(pickup_index) == routing.VehicleVar(delivery_index)
        )
        routing.solver().Add(
            time_dimension.CumulVar(pickup_index) <=time_dimension.CumulVar(delivery_index)
        )

    # Setting first solution heuristic.
    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    # search_parameters.first_solution_strategy = (routing_enums_pb2.FirstSolutionStrategy.PARALLEL_CHEAPEST_INSERTION)
    search_parameters.first_solution_strategy = (routing_enums_pb2.FirstSolutionStrategy.AUTOMATIC)
    # search_parameters.first_solution_strategy = (routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC)
    # search_parameters.local_search_metaheuristic = (routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH)
    search_parameters.local_search_metaheuristic = (routing_enums_pb2.LocalSearchMetaheuristic.AUTOMATIC)
    search_parameters.time_limit.seconds = 20 * 60

    print("Initializing solving process...")

    # Solve the problem.
    solution = routing.SolveWithParameters(search_parameters)

    print('\n')
    print(SOLVER_STATUS_MAP[routing.status()])
    print('\n')
    # Print solution on console.
    if solution:
      print_solution(data, manager, routing, solution)


if __name__ == "__main__":
    main()
