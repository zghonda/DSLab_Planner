import heapq
import datetime
from collections import namedtuple
import pickle
import pandas as pd
from geopy.distance import distance as geo_distance
from scipy.stats import expon

NodeTuple = namedtuple('NodeTuple', ['plan_duration', 'arrival_time', 'node'])
StepTuple = namedtuple('StepTuple', ['station_name',
                                     'station_id', 'departure_time', 'arrival_time', 'walk', 'trip_id'])


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

    def __init__(self, schedules, walking_times, stops_info, stops_info_names, stats_with_pairs,
                 stats_with_pairs_jo, desired_certainty=1.0):
        self.schedules = schedules
        self.walking_times = walking_times
        self.stops_info = stops_info
        self.stops_info_names = stops_info_names
        self.stats_with_pairs = stats_with_pairs
        self.stats_with_pairs_jo = stats_with_pairs_jo
        self.desired_certainty = desired_certainty
        self.count = 0

    def test_goal(self, current_node, destination_station):
        return current_node.current_station == destination_station

    def get_station_id(self, name):
        station_id = self.stops_info_names[name]['stop_id']
        return station_id

    def get_station_name(self, station_id):
        try:
            station_name = self.stops_info[station_id]['stop_name']
        except KeyError:
            station_name = None
        return station_name

    def compute_certainty(self, x, current_station, next_station, time):
        hour = time.total_seconds() // 3600
        temp_df = None

        if (current_station in self.stats_with_pairs.keys()) and (
                next_station in self.stats_with_pairs[current_station].keys()):
            temp_df = self.stats_with_pairs[current_station][next_station]

        else:
            if (current_station in self.stats_with_pairs_jo.keys()) and (
                    next_station in self.stats_with_pairs_jo[current_station].keys()):
                temp_df = self.stats_with_pairs_jo[current_station][next_station]

        if temp_df is not None:
            temp_df['dist_to_hour'] = abs(temp_df['arrival'] - hour)
            delay = temp_df.loc[temp_df.idxmin().dist_to_hour].avg_delay
            return expon(scale=delay).cdf(x.total_seconds() // 60) if delay > 0 else 1.0

        return 1

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
                        node_certainty = self.compute_certainty(x=delay_margin,
                                                                current_station=state.current_station,
                                                                next_station=station,
                                                                time=child_latest_arrival_time) * state.certainty

                        if node_certainty < self.desired_certainty:
                            continue
                    else:
                        child_latest_arrival_time = row.dep_time
                        stage_duration = state.latest_arrival_time - row.dep_time
                        node_certainty = state.certainty * 1.0

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

                if state.current_station in self.walking_times.index.get_level_values(0).unique():

                    for index, row in self.walking_times.loc[state.current_station].iterrows():

                        if row.to in state.visited_stations:
                            continue

                        children_visited_stations = set()
                        children_visited_stations.update(
                            state.visited_stations)
                        children_visited_stations.add(row.to)

                        stage_duration = row.walking_time + datetime.timedelta(minutes=2)
                        child_latest_arrival_time = state.latest_arrival_time - stage_duration

                        children.append(Node(plan_duration=state.plan_duration + stage_duration,
                                             latest_arrival_time=child_latest_arrival_time,
                                             current_station=row.to,
                                             previous_station=state.current_station,
                                             is_walk=True, trip_id=None,
                                             parent_real_arrival_time=state.latest_arrival_time,
                                             visited_stations=children_visited_stations,
                                             certainty=state.certainty,
                                             parent_node=state))

        return children

    def compute_heuristic(self, current_station, departure_station, speed=29.9):
        # estimate the time it takes to go from current_station to destination station in seconds
        distance = self.compute_distance(current_station, departure_station)
        return round(distance / speed)

    def make_plan(self, node: Node):
        # trace back the nodes and construct printable plan
        temp_node = node
        step_tuples = []
        certainty = 1
        while temp_node is not None:
            current_station = self.get_station_name(temp_node.current_station)
            latest_arrival_time = temp_node.latest_arrival_time

            # trip_str = "{}, act_arr: {}, lat_arr: {}, walk: {}, trip_id: {}".format(
            #    current_station, temp_node.parent_real_arrival_time, latest_arrival_time, walk, trip_id)

            step_tuple = StepTuple(current_station, self.get_station_id(current_station), latest_arrival_time,
                                   temp_node.parent_real_arrival_time,
                                   temp_node.is_walk, temp_node.trip_id)

            certainty = temp_node.certainty
            step_tuples.append(step_tuple)

            temp_node = temp_node.parent_node

        return certainty, step_tuples

    def a_star(self, departure_station, destination_station, arrival_time, verbose=False):

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
                          trip_id=trip_id, certainty=1.0,
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
        node_tuple = NodeTuple(f, -start_node.latest_arrival_time, start_node)

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
                    print(self.count)

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

                        child_node_tuple = NodeTuple(f, -child.latest_arrival_time, child)

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


def init_planner(desired_certainty):
    walking_times_df = load_obj('walking_times')
    timetable_dict = load_obj('timetable')
    stats_with_pairs = load_obj('stats_with_pairs')
    stats_with_pairs_jo = load_obj('stats_with_pairs_jo')
    stops_info_dict = load_obj('stops_info_with_id')
    stops_info_names_dict = load_obj('stops_info_with_names')

    planner = Planner(schedules=timetable_dict, walking_times=walking_times_df, stops_info=stops_info_dict,
                      stops_info_names=stops_info_names_dict, stats_with_pairs_jo=stats_with_pairs_jo,
                      stats_with_pairs=stats_with_pairs, desired_certainty=desired_certainty)

    return planner


planner = init_planner(0.5)
plan = planner.a_star(8503000, 8591049, datetime.timedelta(hours=12, minutes=30))


def stringify_plan(plan):
    result_str = ''
    for i in range(len(plan) - 1):
        result_str += "{} ({}) at {} -> {} ({}) at {} :  {} ".format(plan[i].station_name, plan[i].station_id,
                                                                     to_time(plan[i].departure_time),
                                                                     plan[i + 1].station_name,
                                                                     plan[i + 1].station_id,
                                                                     to_time(plan[i].arrival_time), plan[i].trip_id)
        if (plan[i].walk):
            result_str += " [Walk] \n"
        else:
            result_str += '\n'
    return result_str


def to_time(time_delta):
    if time_delta is None:
        return None

    h = time_delta.total_seconds() // 3600
    m = (time_delta.total_seconds() % 3600) // 60
    s = (time_delta.total_seconds() - h * 3600 - m * 60)
    return "{:d}:{:d}:{:d}".format(int(h), int(m), int(s))


print(stringify_plan(plan[1]))
