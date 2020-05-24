import heapq
import datetime
from collections import namedtuple
import pickle
import pandas as pd
from geopy.distance import distance as geo_distance

NodeTuple = namedtuple('NodeTuple', ['plan_duration', 'node'])


def load_obj(name):
    with open('data/' + name + '.pkl', 'rb') as f:
        obj = pickle.load(f)
    return obj


class Node:
    """A node class for A* Pathfinding representing the state"""

    def __init__(self, plan_duration, latest_arrival_time, current_station,
                 previous_station, is_walk, trip_id, visited_stations,
                 parent_real_arrival_time, certainty=None, parent_node=None):
        self.plan_duration = plan_duration
        self.latest_arrival_time = latest_arrival_time
        self.parent_real_arrival_time = parent_real_arrival_time
        self.current_station = current_station
        self.previous_station = previous_station
        self.is_walk = is_walk
        self.trip_id = trip_id
        self.visited_stations = visited_stations
        self.certainty = certainty
        self.parent_node = parent_node

    def __eq__(self, other):
        return isinstance(other, self.__class__) \
            and self.plan_duration == other.plan_duration \
            and self.latest_arrival_time == other.latest_arrival_time \
            and self.current_station == other.current_station \
            and self.previous_station == other.previous_station \
            and self.is_walk == other.is_walk \
            and self.trip_id == other.trip_id

    def __lt__(self, other):
        return self.latest_arrival_time >= other.latest_arrival_time

    def __hash__(self):
        return hash((self.current_station, self.previous_station,
                     self.latest_arrival_time, self.plan_duration,
                     self.is_walk, self.trip_id))

    # def set_actual_arrival(self, parent_real_arrival_time):
    #     self.parent_real_arrival_time = parent_real_arrival_time


class Planner:
    # TODO: Decide if create the dict here, filter df, possibly create dict gen etator

    def __init__(self, schedules, walking_times, stops_info, stops_info_names, trip_route_translation, stats, desired_certainty=1):
        self.schedules = schedules
        self.walking_times = walking_times
        self.dict = None
        self.stops_info = stops_info
        self.stops_info_names = stops_info_names
        self.trip_route_translation = trip_route_translation
        self.stats = None
        self.desired_certainty = desired_certainty

    def test_goal(self, current_node, destination_station):
        return current_node.current_station == destination_station

    def get_route_id(self, trip_id):
        if trip_id is None:
            return None
        return self.trip_route_translation[trip_id]

    def get_station_id(self, name):
        station_id = self.stops_info_names[name]['stop_id']
        return station_id

    def get_station_name(self, station_id):
        try:
            station_name = self.stops_info[station_id]['stop_name']
        except KeyError:
            station_name = None
        return station_name

    # TODO change function to support also hour, current_station, next_station,transport_type
    def compute_certainty(self, current_station, x):
        # return self.stats['arr_function'][current_station](x)
        return 1

        # TODO : adapt time related operations with datetime
        # TODO : add uncertainty

    # TODO : adapt time related operations with datetime
    # TODO :correct bugs related to df, choose parameter to add children

    def generate_children(self, state: Node):
        children = []

        # is_destination = (state.previous_station == None) & (state.is_walk == True) & (state.trip_id == None)
        if (state.current_station in self.schedules.keys()):
            for (station, schedule) in self.schedules[state.current_station].items():

                if station in state.visited_stations:
                    continue

                possible_schedule = schedule[
                    schedule["arr_time"] <= state.latest_arrival_time]

                if possible_schedule.shape[0] == 0:
                    continue

                # To me the 2 parameter here is too arbitrary, consider changing for
                # a bigger value to ensure better uncertainty levels
                for index, row in possible_schedule.nlargest(2, 'dep_time').iterrows():

                    # Difference in trip_id signifies a connection, 2 mins of
                    # connection time means you should arrive at earlier station at
                    # least 2 mins earlier than the schedule
                    if (state.trip_id != row.trip_id) and (state.trip_id is not None):
                        child_latest_arrival_time = row.dep_time - \
                            datetime.timedelta(minutes=2)
                        # stage_duration = state.latest_arrival_time - \
                        #     row.dep_time + datetime.timedelta(minutes=2)
                        stage_duration = state.latest_arrival_time - \
                            child_latest_arrival_time

                        delay_margin = state.latest_arrival_time - row.arr_time

                        node_certainty = self.compute_certainty(
                            state.current_station, delay_margin) * state.certainty

                        if node_certainty < self.desired_certainty:
                            continue
                    else:
                        child_latest_arrival_time = row.dep_time
                        stage_duration = state.latest_arrival_time - row.dep_time
                        node_certainty = state.certainty * 1

                    # TODO Add uncertainty here

                    children_visited_stations = set()
                    children_visited_stations.update(state.visited_stations)
                    children_visited_stations.add(station)

                    children.append(Node(plan_duration=state.plan_duration + stage_duration,
                                         latest_arrival_time=child_latest_arrival_time,
                                         current_station=station,
                                         previous_station=state.current_station,
                                         is_walk=False, trip_id=row.trip_id,
                                         parent_real_arrival_time=row.arr_time,
                                         visited_stations=children_visited_stations,
                                         certainty=node_certainty, parent_node=state))

            if not state.is_walk:

                if state.current_station in walking_times_df.index.get_level_values(0).unique():

                    for index, row in self.walking_times.loc[state.current_station].iterrows():

                        if row.to in state.visited_stations:
                            continue

                        children_visited_stations = set()
                        children_visited_stations.update(
                            state.visited_stations)
                        children_visited_stations.add(row.to)

                        children.append(Node(plan_duration=state.plan_duration + row.walking_time,
                                             latest_arrival_time=state.latest_arrival_time - row.walking_time,
                                             current_station=row.to,
                                             previous_station=state.current_station,
                                             is_walk=True, trip_id=None,
                                             parent_real_arrival_time=state.latest_arrival_time - row.walking_time,
                                             visited_stations=children_visited_stations,
                                             certainty=state.certainty,
                                             parent_node=state))

        return children

    # TODO
    def compute_heuristic(self, current_station, departure_station, speed=29.9):
        # estimate the time it takes to go from current_station to destination station in seconds
        distance = self.compute_distance(current_station, departure_station)
        return round(distance / speed)

    def make_plan(self, node: Node):
        # trace back the nodes and construct printable plan
        temp_node = node
        plan_str = []
        while temp_node is not None:
            current_station = self.get_station_name(temp_node.current_station)
            latest_arrival_time = temp_node.latest_arrival_time
            trip_id = temp_node.trip_id
            walk = temp_node.is_walk
            certainty = temp_node.certainty
            # route_id = self.get_route_id(trip_id)

            trip_str = "{}, act_arr: {}, lat_arr: {}, walk: {}, trip_id: {}".format(
                current_station, temp_node.parent_real_arrival_time, latest_arrival_time, walk, trip_id)

            plan_str.append(trip_str)

            temp_node = temp_node.parent_node

        return plan_str

    # TODO add uncertainty support
    # TODO add support for multi solution output
    # TODO add verbose output
    def a_star(self, departure_station, destination_station, arrival_time,
               uncertainty_level=None, verbose=True):

        # Create start node
        plan_duration = datetime.timedelta()
        latest_arrival_time = arrival_time
        current_station = destination_station
        previous_station = None

        is_walk = None
        trip_id = None
        visited_stations_set = set()
        visited_stations_set.add(current_station)

        start_node = Node(plan_duration=plan_duration,
                          latest_arrival_time=latest_arrival_time,
                          current_station=current_station,
                          previous_station=previous_station, is_walk=is_walk,
                          parent_real_arrival_time=None,
                          trip_id=trip_id, certainty=1,
                          visited_stations=visited_stations_set)

        # Initialize both open and closed list
        unvisited_nodes = []
        visited_nodes = set()

        # add heuristic
        f = start_node.plan_duration.total_seconds() + self.compute_heuristic(
            current_station=start_node.current_station,
            departure_station=departure_station)
        # f = start_node.plan_duration.total_seconds()

        # next state with the closest departure time
        node_tuple = NodeTuple(f, start_node)

        unvisited_nodes.append(node_tuple)

        # Reorder priority queue
        heapq.heapify(unvisited_nodes)

        iteration = 0

        # Loop until you find the end
        while len(unvisited_nodes) > 0:

            # Get the current node
            current_node = heapq.heappop(unvisited_nodes).node

            # current_node.parent_node.set_actual_arrival()

            if current_node not in visited_nodes:

                if verbose:
                    print('iteration :{:d}'.format(iteration))
                    print('total plan duration {}'.format(
                        current_node.plan_duration))
                    # print('current station {}, current trip {}'.format(
                    #    self.get_station_name(current_node.current_station),
                    #    self.get_route_id(current_node.trip_id)))

                # Found the goal
                if self.test_goal(current_node, departure_station):
                    print('found goal')
                    return self.make_plan(current_node)
                else:
                    visited_nodes.add(current_node)
                    # Generate children
                    children = self.generate_children(current_node)

                    # Loop through children
                    for child in children:
                        f = child.plan_duration.total_seconds() + self.compute_heuristic(
                            current_station=child.current_station,
                            departure_station=departure_station)
                        # f = child.plan_duration.total_seconds()

                        child_node_tuple = NodeTuple(f, child)

                        # Add the child the visited list
                        heapq.heappush(unvisited_nodes, child_node_tuple)

            iteration += 1

        return None

    def compute_distance(self, station_id_1, station_id_2):
        lat_1 = self.stops_info[station_id_1]['stop_lat']
        lon_1 = self.stops_info[station_id_1]['stop_lon']
        lat_2 = self.stops_info[station_id_2]['stop_lat']
        lon_2 = self.stops_info[station_id_2]['stop_lon']

        distance = geo_distance((lat_1, lon_1), (lat_2, lon_2)).m
        return distance


walking_times_df = load_obj('walking_times')
timetable_dict = load_obj('timetable1')
# trip_route_translation_dict = load_obj('trip_route_translation')
trip_route_translation_dict = None
stats = None
stops_info_dict = load_obj('stops_info_with_id')
stops_info_names_dict = load_obj('stops_info_with_names')

planner = Planner(schedules=timetable_dict, walking_times=walking_times_df, stops_info=stops_info_dict,
                  stops_info_names=stops_info_names_dict, trip_route_translation=trip_route_translation_dict, stats=stats)


max_arrival_time = datetime.timedelta(days=0, hours=12, minutes=30)

# arrival_station_id = 8587348  # zurich bahnofplatz
# departure_station_id = 8503000  # Zürich, Lochergut	# arrival_station_id = 8591315  # zurich rehalp
# departure_station_id = 8591259  # Zürich, Lochergut


# print(planner.get_station_name("Zürich, Bezirksgebäude"))	# arrival_station_id = planner.get_station_id("Zürich, Berninaplatz")
# departure_station_id = planner.get_station_id("Zürich, Beckenhof")


# departure_station_id = 8503011  # zurich wiedikon
departure_station_id = planner.get_station_id("Zürich HB")
# departure_station_id = 8530813  # zurich kreuzplatz
arrival_station_id = planner.get_station_id("Zürich, Auzelg")
# departure_station_id = 8591427  # zurich Werd
plan = planner.a_star(departure_station_id,
                      arrival_station_id, max_arrival_time)
if plan:
    for t in plan:
        print(t)
