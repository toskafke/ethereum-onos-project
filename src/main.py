from datetime import timedelta

import networkx as nx
import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

CREDENTIALS_PATH = r'../references/Scam Transaction Detection-031b9e755035.json'
PROJECT_ID = 'scam-transaction-detection'
GRAPH_FILE_NAME = "../visualizations/graph.png"


def get_big_query_client():
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_PATH
    )
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    return client


def query_data(client, query):
    query_job = client.query(query)
    iterator = None
    for i in range(3):
        while True:
            try:
                iterator = query_job.result(timeout=30)
            except ConnectionError:
                print(f"Error while pulling the data, try {i+1} out of 3")
                continue
            break
    rows = list(iterator)
    data = None
    if rows:
        data = pd.DataFrame(data=[list(x.values()) for x in rows], columns=list(rows[0].keys()))
    return data


def address_transactions(client, address):
    query = F"""
    SELECT
      *
    FROM
      `bigquery-public-data.crypto_ethereum.transactions` AS txns
    WHERE TRUE
      AND txns.value > 0
      AND (txns.from_address = "{address}" OR txns.to_address = "{address}")
    """
    return query_data(client, query)


def temporal_network(client, timestamp):
    date = pd.to_datetime(timestamp)
    date_min = date - timedelta(minutes=5)
    date_max = date + timedelta(minutes=5)
    query = F"""
    SELECT
      *
    FROM
      `bigquery-public-data.crypto_ethereum.transactions` AS txns
    WHERE TRUE
      AND txns.value > 0
      AND txns.block_timestamp >= "{date_min}"
      AND txns.block_timestamp <= "{date_max}"
      ORDER BY txns.block_timestamp
    """
    return query_data(client, query)


def create_graph(data):
    return nx.from_pandas_edgelist(data, source='from_address', target='to_address',
                                   edge_attr=['value', 'nonce', 'block_timestamp'], create_using=nx.MultiDiGraph)


def plot_graph(G, scam_address, layout="spring"):
    import matplotlib.pyplot as plt
    plt.figure(figsize=(16, 12))
    d = dict(G.degree)
    color_map = []
    edge_color_map = []
    edge_size = []
    for u, v in G.edges():
        if u == scam_address or v == scam_address:
            edge_size.append(10)
            edge_color_map.append("red")
        else:
            edge_size.append(0.9)
            edge_color_map.append("black")
    for node in G:
        if node == scam_address:
            color_map.append('red')
        else:
            color_map.append('green')
    if layout == "spring":
        pos = nx.spring_layout(G, k=0.3, iterations=20)
    elif layout == "circular":
        pos = nx.circular_layout(G, scale=2)
        pos[scam_address] = np.array([0, 0])
    else:
        pos = nx.random_layout(G)
    nx.draw_networkx_nodes(G, pos=pos, node_color=color_map, node_size=[v * 10 for v in d.values()], alpha=0.8)
    nx.draw_networkx_edges(G, pos, width=edge_size, edge_color=edge_color_map)
    plt.savefig(GRAPH_FILE_NAME)
    # this is constant so we don't even need to return anything
    return GRAPH_FILE_NAME


def network_statistics():
    """
    Calculates and returns network statistics.

    """
    # TODO: Implement this :)

    raise NotImplementedError


client = get_big_query_client()

timestamp = "2019-11-29 16:25:57"
temporal_data = temporal_network(client, timestamp)
print(temporal_data.head())

scam_address = '0x7c9001c50ea57c1b2ec1e3e63cf04c297534bfc1'
address_data = address_transactions(client, scam_address)
print(address_data.head())

G = create_graph(temporal_data)
file_name = plot_graph(G, scam_address, layout="spring")
print(file_name)






