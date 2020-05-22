import datetime

import pandas as pd
import datetime as dt
import pickle
import time
import heapq
from geopy.distance import distance as geo_dist
import scipy.stats


def save_obj(obj, name):
    with open(name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load_obj(name):
    with open(name + '.pkl', 'rb') as f:
        return pickle.load(f)


class Node:

    def __init__(self, current_station, previous_station, current_time, elapsed_time, transport_type,
                 trip_id, set_visited_stations, certainty, father_node):

        self.current_station = current_station
        self.previous_station = previous_station
        self.current_time = current_time
        self.elapsed_time = elapsed_time
        self.transport_type = transport_type
        self.trip_id = trip_id
        self.set_visited_stations = set_visited_stations
        self.certainty = certainty
        self.father_node = father_node

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.current_station == other.current_station \
                   and self.previous_station == other.previous_station \
                   and self.current_time == other.current_time \
                   and self.elapsed_time == other.elapsed_time \
                   and self.transport_type == other.transport_type \
                   and self.trip_id == other.trip_id
        return False

    def __hash__(self):
        return hash((self.current_station,
                     self.previous_station,
                     self.current_time,
                     self.elapsed_time,
                     self.transport_type,
                     self.trip_id
                     ))

    def __lt__(self, other):
        return self.current_time <= other.current_time


class Planner:

    def __init__(self, time_tables_dict, walking_times_df, coordinates_df, delays_distributions_dict, max_walking_time=5.0):

        self.time_tables = time_tables_dict
        self.walking_times_df = walking_times_df
        self.coordinates = coordinates_df
        self.delays_distributions_dict = delays_distributions_dict
        self.trip_ids = set(delays_distributions_dict.keys())
        self.walking_times = self.create_walking_time_dict(max_walking_time)

    def create_walking_time_dict(self, max_walking_time):
        walking_times_filtered = self.walking_times_df.loc[self.walking_times_df.walk_minutes <= max_walking_time].copy()
        walking_times_filtered.walk_minutes = walking_times_filtered.walk_minutes.apply(
            lambda x: x if pd.isnull(x) else dt.timedelta(minutes=x)
        )

        station_walking_time_dict = {}
        for name, group in walking_times_filtered.groupby('station_name'):
            station_walking_time_dict[str(name)] = group[['id', 'station_name', 'id2', 'station_name2', 'walk_minutes']]

        return station_walking_time_dict

    def heuristic_from_destination(self, destination, speed=30.079527):
        stations_list = list(self.time_tables.keys())

        heuristic_dict = {}

        for station_elem in stations_list:
            distance = self.compute_distance(destination, station_elem)
            heuristic_dict[station_elem] = round((distance / speed))

        return heuristic_dict

    def shortest_path(self, departure_station, destination_station, current_time, use_heuristic=False,
                      num_of_solutions=1, desired_certainty=0.50, set_walking_time=None):

        # let the user choose the maximum walking time per step
        if set_walking_time is not None:
            self.walking_times = self.create_walking_time_dict(max_walking_time=set_walking_time)

        self.query_time = current_time
        self.destination = destination_station
        self.desired_certainty = desired_certainty
        if use_heuristic:
            print('start computing heuristics')
            self.destination_heuristics = self.heuristic_from_destination(destination_station)
            print('heuristics_computed')

        elapsed_time = datetime.timedelta()
        current_station = departure_station
        previous_station = None
        transport_type = 'walk'
        trip_id = None
        set_visited_stations = set()
        set_visited_stations.add(current_station)
        father_node = None

        root_node = Node(current_station=current_station,
                         previous_station=previous_station,
                         current_time=current_time,
                         elapsed_time=elapsed_time,
                         transport_type=transport_type,
                         trip_id=trip_id,
                         set_visited_stations=set_visited_stations,
                         certainty=1.0,
                         father_node=father_node)

        iter = 0

        # Put tuple pair into the priority queue
        unvisited_queue = []

        temp_elapsed_time = root_node.elapsed_time.total_seconds()
        if use_heuristic:
            temp_elapsed_time += self.destination_heuristics[root_node.current_station]

        unvisited_queue.append((temp_elapsed_time,
                                root_node.current_time,
                                iter,
                                root_node))

        heapq.heapify(unvisited_queue)

        # creating the visited node set
        visited_node_set = set()

        goal_nodes_list = []

        while len(unvisited_queue) != 0:

            unvisited_elem = heapq.heappop(unvisited_queue)
            elapsed_time_unv = unvisited_elem[0]
            current_node = unvisited_elem[-1]

            if iter % 200 == 0:
                print(round(elapsed_time_unv / 60))

            if current_node not in visited_node_set:

                # TODO: remove, for debug purpose
                if current_node.elapsed_time.total_seconds() > 3000:
                    return None

                if current_node.current_station == destination_station and current_node.certainty >= self.desired_certainty:
                    print('iterations: ' + str(iter))
                    goal_nodes_list.append(current_node)
                    if len(goal_nodes_list) == num_of_solutions:
                        plans_found = []
                        for goal_node in goal_nodes_list:
                            plans_found.append(self.create_plan(goal_node))
                        return plans_found
                else:
                    visited_node_set.add(current_node)
                    children_nodes = self.create_node_children(current_node)

                    # adding new nodes to the unvisited queue
                    for child_node in children_nodes:

                        temp_elapsed_time = child_node.elapsed_time.total_seconds()
                        if use_heuristic:
                            temp_elapsed_time += self.destination_heuristics[child_node.current_station]

                        item = (temp_elapsed_time,
                                child_node.current_time,
                                iter,
                                child_node)

                        heapq.heappush(unvisited_queue, item)

            iter += 1

        return None

    def compute_certainty(self, current_node, next_trip_id, next_certainty, row):

        # updating the certainty:
        if next_trip_id != current_node.trip_id and next_trip_id in self.trip_ids:

            # in both cases we need to do this part:
            # probabilty about the delay of the departure of the next transport
            # from the current station
            departure_delay_dict = self.delays_distributions_dict[next_trip_id]
            available_stations = set(departure_delay_dict.keys())

            if current_node.current_station in available_stations:
                departure_delay_distr_params = self.delays_distributions_dict[next_trip_id][current_node.current_station]
                shape_dep, loc_dep, scale_dep = departure_delay_distr_params[:3]
                temp_certainty = 1 - scipy.stats.lognorm.cdf(0, s=shape_dep, loc=loc_dep, scale=scale_dep)
                next_certainty = next_certainty * temp_certainty

            # now we check if the current transport is diffent from 'walk'
            # in the positive case we compute the probability regarding
            # the arrival delay to the current station
            if current_node.transport_type != 'walk':
                delays_params_dict = self.delays_distributions_dict[current_node.trip_id]
                available_stations = set(delays_params_dict.keys())
                if current_node.current_station in available_stations:
                    delays_params_station = delays_params_dict[current_node.current_station]
                    shape_arrival, loc_arrival, scale_arrival = delays_params_station[-3:]
                    cumulative_constant = current_node.current_time - row.departure_time
                    cumulative_constant = cumulative_constant.total_seconds() / 60
                    prob_certainty = scipy.stats.lognorm.cdf(cumulative_constant,
                                                             s=shape_arrival,
                                                             loc=loc_arrival,
                                                             scale=scale_arrival)
                    next_certainty = next_certainty * prob_certainty

        return next_certainty

    def create_node_children(self, current_node):

        node_list = []
        current_station = current_node.current_station

        # TODO: check that these times are compliant to datetime timedelta
        current_time = current_node.current_time
        current_visited_stations = current_node.set_visited_stations
        for next_station, time_table in self.time_tables[current_station].items():

            # check in order to avoid going back
            if next_station == current_node.previous_station or next_station in current_visited_stations:
                continue

            # filtering table based on the current time of the actual node
            filtered_table = time_table.loc[time_table.departure_time >= current_time]

            # for each next station, we only take the first two entries after the filtering
            max_index = min(len(filtered_table), 2)

            if max_index < 1:
                continue

            filtered_table = filtered_table[:max_index]
            filtered_table.reset_index(drop=True, inplace=True)

            # for each row of the filtered table we create a child node
            for index, row in filtered_table.iterrows():

                arr_nx = row.arr_nx.to_pytimedelta()
                elapsed_time = arr_nx - self.query_time
                next_current_time = arr_nx
                transport_type = row.transport_type
                trip_id = row.trip_id
                next_visited_stations_set = set()
                next_visited_stations_set.update(current_visited_stations)
                next_visited_stations_set.add(next_station)

                next_certainty = current_node.certainty

                next_certainty = self.compute_certainty(current_node=current_node,
                                                        next_trip_id=trip_id,
                                                        next_certainty=next_certainty,
                                                        row=row)

                '''
                if current_node.transport_type == 'walk':
                    departure_delay_dict = self.delays_distributions_dict[trip_id]
                    available_stations = set(departure_delay_dict.keys())

                    if current_station in available_stations:
                        departure_delay_distr_params = self.delays_distributions_dict[trip_id][current_node.current_station]
                        shape_dep, loc_dep, scale_dep = departure_delay_distr_params[:3]
                        temp_certainty = 1 - scipy.stats.lognorm.cdf(0, s=shape_dep, loc=loc_dep, scale=scale_dep)
                        next_certainty = next_certainty * temp_certainty
                    else:
                        next_certainty = next_certainty
                else:
                '''
                # discard candidate nodes that have certainty lower than the certainty of the query
                if next_certainty < self.desired_certainty:
                    continue

                child_node = Node(current_station=next_station,
                                  previous_station=current_station,
                                  current_time=next_current_time,
                                  elapsed_time=elapsed_time,
                                  transport_type=transport_type,
                                  trip_id=trip_id,
                                  set_visited_stations=next_visited_stations_set,
                                  certainty=next_certainty,
                                  father_node=current_node)

                node_list.append(child_node)

        # now we deal with walking times, if the current node has been
        # reached by "walk", we do not add the walk edges
        if current_node.transport_type != "walk" \
                and current_station in self.walking_times.keys():

            walking_times_from_current = self.walking_times[current_station]

            for index, row in walking_times_from_current.iterrows():
                next_station = row.station_name2

                if next_station == current_node.previous_station or next_station in current_visited_stations:
                    continue

                walk_minutes = row.walk_minutes.to_pytimedelta()
                elapsed_time = current_node.elapsed_time + walk_minutes
                next_current_time = current_time + walk_minutes
                transport_type = 'walk'

                next_visited_stations_set = set()
                next_visited_stations_set.update(current_visited_stations)
                next_visited_stations_set.add(next_station)

                # when walking to next station, the certainty does not change
                next_certainty = current_node.certainty

                child_node = Node(current_station=next_station,
                                  previous_station=current_station,
                                  current_time=next_current_time,
                                  elapsed_time=elapsed_time,
                                  transport_type=transport_type,
                                  trip_id=None,
                                  set_visited_stations=next_visited_stations_set,
                                  certainty=next_certainty,
                                  father_node=current_node)

                node_list.append(child_node)

        return node_list

    def create_plan(self, goal_node):

        plan = []
        temp_node = goal_node

        while temp_node is not None:
            current_station = temp_node.current_station
            current_time = temp_node.current_time
            trip_id = temp_node.trip_id
            transport_type = temp_node.transport_type
            certainty = temp_node.certainty

            temp_dict = {}
            temp_dict['station'] = current_station
            temp_dict['time'] = current_time
            temp_dict['trip_id'] = trip_id
            temp_dict['transport'] = transport_type
            temp_dict['certainty'] = certainty

            plan.append(temp_dict)
            temp_node = temp_node.father_node

        plan = plan[::-1]

        return plan

    def compute_distance(self, name1, name2):
        row_1 = self.coordinates.loc[self.coordinates.station_name == name1]
        row_2 = self.coordinates.loc[self.coordinates.station_name == name2]
        distance = geo_dist((row_1.latitude.values[0], row_1.longitude.values[0]),
                            (row_2.latitude.values[0], row_2.longitude.values[0])).m
        return distance


print('loading data...')
station_tables = load_obj('station_tables_dictionary')
# walking_times = load_obj('station_walking_times_dictionary')
walking_time_df = pd.DataFrame.from_csv('walking_time_table.csv', encoding="ISO-8859-1")
coordinates_df = pd.DataFrame.from_csv('zurich_hb_stops.csv')
stats_params = load_obj('stats_param_dictionary_filtered')
# distances = load_obj('all_distances')
print('data loaded successfully!')

planner = Planner(time_tables_dict=station_tables,
                  walking_times_df=walking_time_df,
                  coordinates_df=coordinates_df,
                  delays_distributions_dict=stats_params)

planner.query_time = datetime.timedelta(days=0, hours=14, minutes=30)

current_time = datetime.timedelta(days=0,hours=14, minutes=30)
elapsed_time = datetime.timedelta()
'''
current_station = 'Zürich, Waidspital'
previous_station = None
transport_type = 'walk'
father_node = None

root_node = Node(current_station=current_station,
                 previous_station=previous_station,
                 current_time=current_time,
                 elapsed_time=elapsed_time,
                 transport_type=transport_type,
                 trip_id=None,
                 father_node=father_node)

second_node = Node(current_station=current_station,
                 previous_station=previous_station,
                 current_time=current_time,
                 elapsed_time=elapsed_time,
                 transport_type=transport_type,
                 trip_id=None,
                 father_node=root_node)
'''

stations = list(station_tables.keys())
len_stations = len(stations) - 1

plans_list = []

for index, station in enumerate(stations):
    if station == 'Zürich HB':
        continue

    print("trip test number: %d / %d" % (index + 1, len_stations))
    print('destination: ' + station)
    time_start = time.clock()

    plans = planner.shortest_path(departure_station='Zürich HB',
                                  destination_station=station,
                                  current_time=datetime.timedelta(days=0, hours=14, minutes=30),
                                  use_heuristic=True,
                                  num_of_solutions=2
                                  )
    time_elapsed = (time.clock() - time_start)

    print(time_elapsed)
    if plans is not None:
        for index, plan in enumerate(plans):
            if plan is not None:
                plan.append({'computed_time': time_elapsed})
            print('plan ' + str(index + 1) + ': ' + str(plan))
    else:
        plans = 'failed, destination: ' + station
        print(plans)

    plans_list.append(plans)
    save_obj(plans_list, "test_results_on_all path_heuristic")

'''
time_start = time.clock()
plans = planner.shortest_path(departure_station='Zürich HB',
                                 destination_station='Adliswil, Ahornweg',
                                 current_time=datetime.timedelta(days=0, hours=14, minutes=30),
                                 use_heuristic=True,
                                 num_of_solutions=1
                                 )
time_elapsed = (time.clock() - time_start)

print(time_elapsed)
print(plans)
'''


