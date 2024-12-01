import numpy as np
import pandas as pd
import networkx as nx
from matplotlib import pyplot as plt
from collections import defaultdict
from typing import Tuple


class ZTMParser:
    def __init__(self, data_dir_path: str = 'gtfs'):
        df_calendar = pd.read_table(f'{data_dir_path}/calendar.txt', sep=',')
        df_trips = pd.read_table(f'{data_dir_path}/trips.txt', sep=',')
        df_routes = pd.read_table(f'{data_dir_path}/routes.txt', sep=',')
        df_stops = pd.read_table(f'{data_dir_path}/stops.txt', sep=',')
        df_stop_times = pd.read_table(f'{data_dir_path}/stop_times.txt', sep=',')
        self.df = self.get_merged_df(df_calendar, df_trips, df_routes, df_stops, df_stop_times)
        self.df_trams = self.df[self.df["route_type"] == 0]
        self.df_trains = self.df[self.df["route_type"] == 2]
        self.df_buses = self.df[self.df["route_type"] == 3]
        self.G_trams = None
        self.G_buses = None
        self.G_trains = None

    @staticmethod
    def get_merged_df(df_calendar: pd.DataFrame, df_trips: pd.DataFrame, df_routes: pd.DataFrame,
                      df_stops: pd.DataFrame, df_stop_times: pd.DataFrame) -> pd.DataFrame:
        df_calendar = df_calendar[['service_id', 'start_date', 'end_date']]
        df_calendar['start_date'] = pd.to_datetime(df_calendar['start_date'], format='%Y%m%d')
        df_calendar['end_date'] = pd.to_datetime(df_calendar['end_date'], format='%Y%m%d')

        df_trips = df_trips[['route_id', 'service_id', 'trip_id']]
        df_routes = df_routes[['route_id', 'agency_id', 'route_type']]

        df_stop_times = df_stop_times[
            (df_stop_times['arrival_time'] <= '23:59:59') & (df_stop_times['departure_time'] <= '23:59:59')]
        df_stop_times['arrival_time'] = pd.to_datetime(df_stop_times['arrival_time'], format='%H:%M:%S')
        df_stop_times['departure_time'] = pd.to_datetime(df_stop_times['departure_time'], format='%H:%M:%S')

        df_trips_daily = df_calendar.merge(df_trips, on='service_id')

        df_trips_routes_daily = df_trips_daily.merge(df_routes, on='route_id')

        df_with_stops = df_trips_routes_daily.merge(
            df_stop_times.drop(columns=['stop_headsign', 'pickup_type', 'drop_off_type', 'timepoint']), on='trip_id')

        df = df_with_stops.merge(df_stops.drop(columns=['stop_code']), on='stop_id')

        df['arrival_time'] = pd.to_datetime(
            df['start_date'].dt.date.astype(str) + ' ' + df['arrival_time'].dt.time.astype(str))
        df['departure_time'] = pd.to_datetime(
            df['start_date'].dt.date.astype(str) + ' ' + df['departure_time'].dt.time.astype(str))
        df = df.drop(columns=['start_date', 'end_date'])
        df["next_stop"] = df.groupby("trip_id")["stop_id"].shift(-1).astype('Int64')
        return df

    @staticmethod
    def generate_graph(df: pd.DataFrame) -> nx.Graph:
        G = nx.DiGraph()
        df_stops = df.drop_duplicates('stop_id')
        G.add_nodes_from(df_stops.stop_id)
        nx.set_node_attributes(G, df_stops.set_index('stop_id')[['stop_lat', 'stop_lon']].to_dict('index'))
        df_routes = df_stops.drop_duplicates(['route_id', 'stop_id'])
        df_routes = df_routes[~df_routes['next_stop'].isna()]
        edge_tuples = list(zip(df_routes['stop_id'], df_routes['next_stop']))
        G.add_edges_from(edge_tuples)
        return G

    def create_graphs(self) -> None:
        self.G_trams = self.generate_graph(self.df_trams)
        self.G_buses = self.generate_graph(self.df_buses)
        self.G_trains = self.generate_graph(self.df_trains)
        return None

    def draw_graph(self, data_type: str = 'trams') -> None:
        plt.figure(figsize=(12, 12))
        if data_type == 'trams':
            G = self.G_trams
        elif data_type == 'buses':
            G = self.G_buses
        else:
            G = self.G_trains
        pos = {node: (data['stop_lon'], data['stop_lat']) for node, data in G.nodes(data=True)}
        nx.draw(G, pos, node_size=20)
        plt.show()
        return
