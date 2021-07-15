from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
import ipdb

SOLVER_STATUS_MAP = {
    0: 'ROUTING_NOT_SOLVED: Problem not solved yet.',
    1: 'ROUTING_SUCCESS: Problem solved successfully.',
    2: 'ROUTING_FAIL: No solution found to the problem.',
    3: 'ROUTING_FAIL_TIMEOUT: Time limit reached before finding a solution.',
    4: 'ROUTING_INVALID: Model, model parameters, or flags are not valid.',
}

def create_data_model():
    """Stores the data for the problem."""
    data = {}
    data['time_matrix'] = [
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        [0, 0, 8, 3, 2, 6, 8, 4, 8, 8, 13, 7, 5, 8, 12, 10],
        [0, 8, 0, 11, 10, 6, 3, 9, 5, 8, 4, 15, 14, 13, 9, 18],
        [0, 3, 11, 0, 1, 7, 10, 6, 10, 10, 14, 6, 7, 9, 14, 6],
        [0, 2, 10, 1, 0, 6, 9, 4, 8, 9, 13, 4, 6, 8, 12, 8],
        [0, 6, 6, 7, 6, 0, 2, 3, 2, 2, 7, 9, 7, 7, 6, 12],
        [0, 8, 3, 10, 9, 2, 0, 6, 2, 5, 4, 12, 10, 10, 6, 15],
        [0, 4, 9, 6, 4, 3, 6, 0, 4, 4, 8, 5, 4, 3, 7, 8],
        [0, 8, 5, 10, 8, 2, 2, 4, 0, 3, 4, 9, 8, 7, 3, 13],
        [0, 8, 8, 10, 9, 2, 5, 4, 3, 0, 4, 6, 5, 4, 3, 9],
        [0, 13, 4, 14, 13, 7, 4, 8, 4, 4, 0, 10, 9, 8, 4, 13],
        [0, 7, 15, 6, 4, 9, 12, 5, 9, 6, 10, 0, 1, 3, 7, 3],
        [0, 5, 14, 7, 6, 7, 10, 4, 8, 5, 9, 1, 0, 2, 6, 4],
        [0, 8, 13, 9, 8, 7, 10, 3, 7, 4, 8, 3, 2, 0, 4, 5],
        [0, 12, 9, 14, 12, 6, 6, 7, 3, 3, 4, 7, 6, 4, 0, 9],
        [0, 10, 18, 6, 8, 12, 15, 8, 13, 9, 13, 3, 4, 5, 9, 0],
    ]
    data['time_windows'] = [
        (0, 300),  # depot
        (0, 12),  # 1
        (0, 15),  # 2
        (0, 18),  # 3
        (0, 13),  # 4
        (0, 5),  # 5
        (5, 10),  # 6
        (0, 12),  # 7
        (5, 8),  # 8
        (10, 14),  # 9
        (10, 16),  # 10
        (20, 25),  # 11
        (0, 5),  # 12
        (5, 10),  # 13
        (7, 8),  # 14
        (13, 19),  # 15
    ]
    data['pickups_deliveries'] = [
        (6, 7),
        (8, 9),
        (10, 11),
        (12, 13),
        (14, 15),
    ]
    data['starts'] = [1, 2, 3, 4, 5]
    data['ends'] = [0, 0, 0, 0, 0]
    data['num_vehicles'] = 5
    return data

def print_solution(data, manager, routing, solution):
    """Prints solution on console."""
    print(f'Objective: {solution.ObjectiveValue()}\n\n')
    time_dimension = routing.GetDimensionOrDie('Time')
    total_time = 0
    for vehicle_id in range(data['num_vehicles']):
        index = routing.Start(vehicle_id)
        plan_output = 'Route for vehicle {}:\n'.format(vehicle_id)
        counter = 0
        while not routing.IsEnd(index):
            time_var = time_dimension.CumulVar(index)
            if counter == 0:
                plan_output += 'Location {0} Time({1},{2})'.format(
                    manager.IndexToNode(index), solution.Min(time_var),
                    solution.Max(time_var))
            else:
                plan_output += ' -> Location {0} Time({1},{2})'.format(
                    manager.IndexToNode(index), solution.Min(time_var),
                    solution.Max(time_var))
            index = solution.Value(routing.NextVar(index))
            counter += 1
        time_var = time_dimension.CumulVar(index)
        plan_output += '\nTime of the route: {} hours\n'.format(
            solution.Min(time_var))
        print(plan_output)
        total_time += solution.Min(time_var)
    print('\nTotal time of all routes: {} hours'.format(total_time))

def main():
    """Entry point of the program."""
    # Instantiate the data problem.
    data = create_data_model()

    # Create the routing index manager.
    manager = pywrapcp.RoutingIndexManager(len(data['time_matrix']),
                                           data['num_vehicles'], data['starts'],
                                           data['ends'])

    # Create Routing Model.
    routing = pywrapcp.RoutingModel(manager)


    # Define cost of each arc.
    def time_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return data['time_matrix'][from_node][to_node]

    transit_callback_index = routing.RegisterTransitCallback(time_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

    # Add Distance constraint.
    dimension_name = 'Time'
    routing.AddDimension(
        transit_callback_index,
        300,  # slack
        3000,  # vehicle maximum travel distance
        False,  # start cumul to zero
        dimension_name)
    time_dimension = routing.GetDimensionOrDie(dimension_name)
    # time_dimension.SetGlobalSpanCostCoefficient(100)

    # Add time window constraints for each location except depot.
    for location_idx, time_window in enumerate(data['time_windows']):
        # ipdb.set_trace()
        if location_idx in data['starts'] or location_idx == 0:
            continue
        index = manager.NodeToIndex(location_idx)
        time_dimension.CumulVar(index).SetRange(time_window[0], time_window[1])
    # Add time window constraints for each vehicle start node.
    for vehicle_id in range(data['num_vehicles']):
        index = routing.Start(vehicle_id)
        time_dimension.CumulVar(index).SetRange(
            data['time_windows'][data['starts'][vehicle_id]][0],
            data['time_windows'][data['starts'][vehicle_id]][1],
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
            routing.VehicleVar(pickup_index) == routing.VehicleVar(
                delivery_index))
        routing.solver().Add(
            time_dimension.CumulVar(pickup_index) <=
            time_dimension.CumulVar(delivery_index))

    # Setting first solution heuristic.
    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC)
    search_parameters.time_limit.seconds = 120

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
