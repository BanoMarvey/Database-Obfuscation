import sqlite3
import pandas as pd
import pm4py
from pm4py.objects.log.util import dataframe_utils
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
from pm4py.visualization.dfg import visualizer as dfg_visualization
from pm4py.algo.discovery.alpha import algorithm as alpha_miner
from pm4py.algo.discovery.heuristics import algorithm as heuristics_miner
from pm4py.statistics.attributes.log import get as attributes_get
from pm4py.visualization.petri_net import visualizer as pn_visualizer


def load_database(db_name, table_name='event_log'):
    """Load event log from a database and convert it to PM4Py format."""
    try:
        conn = sqlite3.connect(db_name)
        data = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()

        data['timestamp'] = pd.to_datetime(data['timestamp'])
        data = dataframe_utils.convert_timestamp_columns_in_df(data)

        data = data.rename(columns={
            'case_id': 'case:concept:name',
            'activity_name': 'concept:name',
            'timestamp': 'time:timestamp'
        })

        return log_converter.apply(data), data  # Return both PM4Py event log and DataFrame
    except Exception as e:
        print(f"Error loading {db_name}: {e}")
        return None, None


def analyze_case_volume(df_orig, df_adj):
    """Compare the number of cases started per weekday."""
    def get_case_starts(df):
        df_sorted = df.sort_values(by=['time:timestamp']).drop_duplicates(subset=['case:concept:name'])
        return df_sorted['time:timestamp'].dt.day_name().value_counts().sort_index()

    orig_volume = get_case_starts(df_orig)
    adj_volume = get_case_starts(df_adj)

    comparison = pd.DataFrame({'Original': orig_volume, 'Adjusted': adj_volume}).fillna(0)
    comparison['Difference'] = comparison['Adjusted'] - comparison['Original']

    print("\nCase Volume per Weekday:\n", comparison)


def discover_and_compare_process_models(event_log_orig, event_log_adj):
    """Compare process models using PM4Py."""
    
    def discover_and_save(log, variant, filename):
        net, im, fm = variant.apply(log)
        gviz = pn_visualizer.apply(net, im, fm, variant=pn_visualizer.Variants.WO_DECORATION)
        pn_visualizer.save(gviz, filename)
        return net, im, fm

    # Alpha Miner Comparison
    net_orig, im_orig, fm_orig = discover_and_save(event_log_orig, alpha_miner, "alpha_miner_original.png")
    net_adj, im_adj, fm_adj = discover_and_save(event_log_adj, alpha_miner, "alpha_miner_adjusted.png")

    print("Alpha Miner models saved: 'alpha_miner_original.png' & 'alpha_miner_adjusted.png'.")

    # Heuristics Miner Comparison
    net_orig, im_orig, fm_orig = discover_and_save(event_log_orig, heuristics_miner, "heuristics_miner_original.png")
    net_adj, im_adj, fm_adj = discover_and_save(event_log_adj, heuristics_miner, "heuristics_miner_adjusted.png")

    print("Heuristics Miner models saved: 'heuristics_miner_original.png' & 'heuristics_miner_adjusted.png'.")

    # Directly-Follows Graph (DFG) Comparison
    for log, filename in [(event_log_orig, "dfg_original.png"), (event_log_adj, "dfg_adjusted.png")]:
        dfg = dfg_discovery.apply(log)
        gviz = dfg_visualization.apply(dfg, variant=dfg_visualization.Variants.FREQUENCY)
        dfg_visualization.save(gviz, filename)

    print("DFG models saved: 'dfg_original.png' & 'dfg_adjusted.png'.")


def compare_case_durations(event_log_orig, event_log_adj):
    """Compare average case duration between two logs."""
    
    orig_durations = pm4py.statistics.traces.generic.log.get_trace_length(event_log_orig)
    adj_durations = pm4py.statistics.traces.generic.log.get_trace_length(event_log_adj)

    avg_orig = sum(orig_durations) / len(orig_durations) if orig_durations else 0
    avg_adj = sum(adj_durations) / len(adj_durations) if adj_durations else 0

    print(f"\n--- Case Duration Comparison ---")
    print(f"Original Log: Average duration = {avg_orig:.2f} events")
    print(f"Adjusted Log: Average duration = {avg_adj:.2f} events")
    print(f"Difference: {avg_adj - avg_orig:.2f} events")


def compare_databases(original_db, adjusted_db):
    """Compare two event logs using process mining techniques."""
    print(f"\nComparing {original_db} with {adjusted_db}")

    event_log_orig, df_orig = load_database(original_db)
    event_log_adj, df_adj = load_database(adjusted_db)

    if not event_log_orig or not event_log_adj:
        print("Error: Failed to load one or both event logs.")
        return

    print("\n1. Case Volume Analysis:")
    analyze_case_volume(df_orig, df_adj)

    print("\n2. Process Model Comparison:")
    discover_and_compare_process_models(event_log_orig, event_log_adj)

def main():
    original_db = input("Enter original database: ").strip()
    adjusted_db = input("Enter adjusted database: ").strip()
    compare_databases(original_db, adjusted_db)


if __name__ == "__main__":
    main()