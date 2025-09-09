from typing import Any, Dict, List, Tuple
from datetime import datetime
from ortools.constraint_solver import pywrapcp, routing_enums_pb2


class ORToolsVRPSolver:
    """
    Vehicle Routing with Time Windows and Capacity using OR-Tools.
    Single-vehicle by default, but can be configured for multiple.
    """

    def __init__(self, vehicle_count: int = 1, capacity: float = 10.0) -> None:
        self.vehicle_count = vehicle_count
        self.capacity = capacity

    def _build_time_matrix(self, orders: List[Dict[str, Any]], time_predict_fn) -> List[List[int]]:
        # Nodes: depot (0) + pickups/drops sequentially
        coords: List[Tuple[float, float]] = []
        # Depot: use first pickup as start
        if not orders:
            return [[0]]
        depot = tuple(orders[0]["pickup"])  # (lat, lon)
        coords.append(depot)
        for o in orders:
            coords.append(tuple(o["pickup"]))
            coords.append(tuple(o["drop"]))

        n = len(coords)
        matrix = [[0] * n for _ in range(n)]
        now = datetime.utcnow()
        for i in range(n):
            for j in range(n):
                if i == j:
                    matrix[i][j] = 0
                else:
                    sec = time_predict_fn(coords[i], coords[j], now)
                    matrix[i][j] = int(max(1, round(sec)))
        return matrix

    def _build_demand(self, orders: List[Dict[str, Any]]) -> List[int]:
        # depot demand 0, pickups positive volume, drops negative volume
        demand: List[int] = [0]
        for o in orders:
            vol = int(round(float(o.get("volume", 1.0))))
            demand.append(vol)
            demand.append(-vol)
        return demand

    def _build_time_windows(self, orders: List[Dict[str, Any]]) -> List[Tuple[int, int]]:
        # Depot with wide window; pickups/drops with provided earliest/latest
        windows: List[Tuple[int, int]] = [(0, 24 * 3600)]
        for o in orders:
            e = int(datetime.fromisoformat(o["earliest"].replace("Z", "+00:00")).timestamp() % (24 * 3600))
            l = int(datetime.fromisoformat(o["latest"].replace("Z", "+00:00")).timestamp() % (24 * 3600))
            if l < e:
                l = e + 2 * 3600  # ensure feasible window
            windows.append((e, l))  # pickup
            windows.append((e, l))  # drop (simplified: same window)
        return windows

    def solve(self, orders: List[Dict[str, Any]], time_predict_fn) -> Dict[str, Any]:
        if not orders:
            return {"status": "empty", "routes": []}

        time_matrix = self._build_time_matrix(orders, time_predict_fn)
        demands = self._build_demand(orders)
        time_windows = self._build_time_windows(orders)

        manager = pywrapcp.RoutingIndexManager(len(time_matrix), self.vehicle_count, 0)
        routing = pywrapcp.RoutingModel(manager)

        def transit_callback(from_index, to_index):
            i = manager.IndexToNode(from_index)
            j = manager.IndexToNode(to_index)
            return time_matrix[i][j]

        transit_callback_index = routing.RegisterTransitCallback(transit_callback)
        routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

        # Capacity dimension
        def demand_callback(from_index):
            i = manager.IndexToNode(from_index)
            return demands[i]

        demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
        routing.AddDimensionWithVehicleCapacity(
            demand_callback_index,
            0,
            [int(self.capacity)] * self.vehicle_count,
            True,
            "Capacity",
        )

        # Time dimension
        routing.AddDimension(
            transit_callback_index,
            60 * 60,
            24 * 3600,
            False,
            "Time",
        )
        time_dimension = routing.GetDimensionOrDie("Time")

        # Apply time windows
        for node in range(len(time_windows)):
            index = manager.NodeToIndex(node)
            window = time_windows[node]
            time_dimension.CumulVar(index).SetRange(window[0], window[1])

        # Search parameters
        search_parameters = pywrapcp.DefaultRoutingSearchParameters()
        search_parameters.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
        search_parameters.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
        search_parameters.time_limit.FromSeconds(5)

        assignment = routing.SolveWithParameters(search_parameters)
        if assignment is None:
            return {"status": "infeasible", "routes": []}

        # Extract routes (single vehicle)
        index = routing.Start(0)
        route_nodes: List[int] = []
        while not routing.IsEnd(index):
            node = manager.IndexToNode(index)
            route_nodes.append(node)
            index = assignment.Value(routing.NextVar(index))
        route_nodes.append(manager.IndexToNode(index))

        return {
            "status": "ok",
            "nodes": route_nodes,  # indices into [depot, p1, d1, p2, d2, ...]
        }

