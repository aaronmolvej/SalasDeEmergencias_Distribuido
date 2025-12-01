import json
import os

CONFIG_PATH = "config/cluster_config.json"

def load_cluster_config():
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(f"No se encontró {CONFIG_PATH}")
    with open(CONFIG_PATH, 'r') as f:
        return json.load(f)