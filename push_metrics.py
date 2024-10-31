import csv
from typing import List, Tuple, Dict

import time
import os

from prometheus_client.core import GaugeMetricFamily, REGISTRY
from prometheus_client import start_http_server


def read_file_to_dict(file_path: str, include_index=False, start_index=1) -> Tuple[Dict[str, Dict[str, float]], int]:
    """
    Reads data from a single file and stores it in a dictionary.

    :param file_path: Path to the CSV file.
    :param include_index: Whether to include an incrementing index (e.g., A1, A2, ...) in the dictionary.
    :param start_index: The starting index for the 'A' labels.
    :return: A tuple of (dictionary with data, next available index).
    """
    data = {}
    current_index = start_index

    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            if len(row) >= 3:  # Ensure there are enough columns
                name_id = row[0].split('=')[1].strip()
                gpu_hours = float(row[1].split('=')[1].strip())
                gpu_tres_hours = float(row[2].split('=')[1].strip())
                
                if include_index:
                    # Create an index label like A1, A2, ...
                    index_label = f"A{current_index}"
                    data[name_id] = {'index': index_label, 'gpu_hours': gpu_hours, 'gpu_tres_hours': gpu_tres_hours}
                    current_index += 1
                else:
                    # Store without index
                    data[name_id] = {'gpu_hours': gpu_hours, 'gpu_tres_hours': gpu_tres_hours}
                    
    return data, current_index

def read_file_pairs(file_pairs: List[Tuple[str, str]]):
    """
    Reads file pairs one at a time and stores data in six dictionaries.

    :param file_pairs: List of tuples containing file paths for each category (partition, group, user).
    :return: Six dictionaries for each data type and sum data.
    """
    # Initialize separate dictionaries for each type and sum type
    partition_dict = {}
    partition_dict_sum = {}
    group_dict = {}
    group_dict_sum = {}
    user_dict = {}
    user_dict_sum = {}

    # Initialize starting index for 'A' labels for each dictionary
    partition_index = 1
    partition_sum_index = 1
    group_index = 1
    group_sum_index = 1
    user_index = 1
    user_sum_index = 1

    for file_sum, file_regular in file_pairs:
        # Determine which dictionary to update based on the file path
        if 'partition' in file_sum:
            # Update partition sum and regular dictionaries with unique indices
            partition_dict_sum_data, partition_sum_index = read_file_to_dict(file_sum, include_index=True, start_index=partition_sum_index)
            partition_dict_data, partition_index = read_file_to_dict(file_regular, include_index=True, start_index=partition_index)
            partition_dict_sum.update(partition_dict_sum_data)
            partition_dict.update(partition_dict_data)
        elif 'group' in file_sum:
            # Update group sum and regular dictionaries with unique indices
            group_dict_sum_data, group_sum_index = read_file_to_dict(file_sum, include_index=True, start_index=group_sum_index)
            group_dict_data, group_index = read_file_to_dict(file_regular, include_index=True, start_index=group_index)
            group_dict_sum.update(group_dict_sum_data)
            group_dict.update(group_dict_data)
        elif 'user' in file_sum:
            # Update user sum and regular dictionaries with unique indices
            user_dict_sum_data, user_sum_index = read_file_to_dict(file_sum, include_index=True, start_index=user_sum_index)
            user_dict_data, user_index = read_file_to_dict(file_regular, include_index=True, start_index=user_index)
            user_dict_sum.update(user_dict_sum_data)
            user_dict.update(user_dict_data)

    return partition_dict, partition_dict_sum, group_dict, group_dict_sum, user_dict, user_dict_sum

# Example usage
file_pairs = [
    ('/tmp/sacct_tmp_files/partition_dictionary_sum.csv', '/tmp/sacct_tmp_files/partition_dictionary.csv'),
    ('/tmp/sacct_tmp_files/group_dictionary_sum.csv', '/tmp/sacct_tmp_files/group_dictionary.csv'),
    ('/tmp/sacct_tmp_files/user_dictionary_sum.csv', '/tmp/sacct_tmp_files/user_dictionary.csv')
]

# Call the function and store the results in separate dictionaries
partition_dict, partition_dict_sum, group_dict, group_dict_sum, user_dict, user_dict_sum = read_file_pairs(file_pairs)

class SlurmKempnerSacctsCollector:

    def collect(self):
        # Create GaugeMetricFamily for gpu_hours and gpu_tres_hours with name_id and index labels
        day_gpu_hours_part_metric = GaugeMetricFamily(
            'day_gpu_part_hours',
            'Total GPU hours for partition',
            labels=['name_id', 'index']
        )
        day_gpu_tres_hours_part_metric = GaugeMetricFamily(
            'day_gpu_tres_part_hours',
            'Total GPU hours for partition',
            labels=['name_id', 'index']
        )

        day_gpu_hours_group_metric = GaugeMetricFamily(
            'day_gpu_group_hours',
            'Total GPU hours for group',
            labels=['name_id', 'index']
        )
        day_gpu_tres_hours_group_metric = GaugeMetricFamily(
            'day_gpu_tres_group_hours',
            'Total GPU hours for group',
            labels=['name_id', 'index']
        )

        day_gpu_hours_user_metric = GaugeMetricFamily(
            'day_gpu_user_hours',
            'Total GPU hours for user',
            labels=['name_id', 'index']
        )
        day_gpu_tres_hours_user_metric = GaugeMetricFamily(
            'day_gpu_tres_user_hours',
            'Total GPU hours for user',
            labels=['name_id', 'index']
        )

        tot_gpu_hours_part_metric = GaugeMetricFamily(
            'tot_gpu_part_hours',
            'Total GPU hours for partition',
            labels=['name_id', 'index']
        )
        tot_gpu_tres_hours_part_metric = GaugeMetricFamily(
            'tot_gpu_tres_part_hours',
            'Cumulative Total GPU hours for partition',
            labels=['name_id', 'index']
        )

        tot_gpu_hours_group_metric = GaugeMetricFamily(
            'tot_gpu_group_hours',
            'Cumulative Total GPU hours for group',
            labels=['name_id', 'index']
        )
        tot_gpu_tres_hours_group_metric = GaugeMetricFamily(
            'tot_gpu_tres_group_hours',
            'Cumulative Total GPU hours for group',
            labels=['name_id', 'index']
        )

        tot_gpu_hours_user_metric = GaugeMetricFamily(
            'tot_gpu_user_hours',
            'Cumulative Total GPU hours for user',
            labels=['name_id', 'index']
        )
        tot_gpu_tres_hours_user_metric = GaugeMetricFamily(
            'tot_gpu_tres_user_hours',
            'Cumulative Total GPU hours for user',
            labels=['name_id', 'index']
        )

        # Add metrics from partition_dict, group_dict, user_dict
        for name_id, metrics in partition_dict.items():
            index = metrics['index']
            day_gpu_hours_part_metric.add_metric([name_id, index], metrics['gpu_hours'])
            day_gpu_tres_hours_part_metric.add_metric([name_id, index], metrics['gpu_tres_hours'])
        for name_id, metrics in group_dict.items():
            index = metrics['index']
            day_gpu_hours_group_metric.add_metric([name_id, index], metrics['gpu_hours'])
            day_gpu_tres_hours_group_metric.add_metric([name_id, index], metrics['gpu_tres_hours'])
        for name_id, metrics in user_dict.items():
            index = metrics['index']
            day_gpu_hours_user_metric.add_metric([name_id, index], metrics['gpu_hours'])
            day_gpu_tres_hours_user_metric.add_metric([name_id, index], metrics['gpu_tres_hours'])


        # Add metrics from partition_dict_sum, group_dict_sum, user_dict_sum
        for name_id, metrics in partition_dict_sum.items():
            index = metrics['index']
            tot_gpu_hours_part_metric.add_metric([name_id, index], metrics['gpu_hours'])
            tot_gpu_tres_hours_part_metric.add_metric([name_id, index], metrics['gpu_tres_hours'])
        for name_id, metrics in group_dict_sum.items():
            index = metrics['index']
            tot_gpu_hours_group_metric.add_metric([name_id, index], metrics['gpu_hours'])
            tot_gpu_tres_hours_group_metric.add_metric([name_id, index], metrics['gpu_tres_hours'])
        for name_id, metrics in user_dict_sum.items():
            index = metrics['index']
            tot_gpu_hours_user_metric.add_metric([name_id, index], metrics['gpu_hours'])
            tot_gpu_tres_hours_user_metric.add_metric([name_id, index], metrics['gpu_tres_hours'])



        # Yield metrics to Prometheus
        yield day_gpu_hours_part_metric
        yield day_gpu_tres_hours_part_metric
        yield day_gpu_hours_group_metric
        yield day_gpu_tres_hours_group_metric
        yield day_gpu_hours_user_metric
        yield day_gpu_tres_hours_user_metric
        yield tot_gpu_hours_part_metric
        yield tot_gpu_tres_hours_part_metric
        yield tot_gpu_hours_group_metric
        yield tot_gpu_tres_hours_group_metric
        yield tot_gpu_hours_user_metric
        yield tot_gpu_tres_hours_user_metric

if __name__ == "__main__":
    start_http_server(10003)
    REGISTRY.register(SlurmKempnerSacctsCollector())
    while True:
        time.sleep(160)
