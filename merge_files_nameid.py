import csv
import re

def parse_line(line: str) -> dict:
    """
    Parses a line of the CSV file with format 'name= id1, gpu_hours= 1.5, gpu_tres_hours= 2.0'.

    :param line: A line from the file.
    :return: Dictionary with 'name_id', 'gpu_hours', and 'gpu_tres_hours'.
    """
    pattern = r"name=\s*(\S+)\s*,\s*gpu_hours=\s*([\d.]+)\s*,\s*gpu_tres_hours=\s*([\d.]+)"
    match = re.match(pattern, line)
    if match:
        name_id = match.group(1)
        gpu_hours = float(match.group(2))
        gpu_tres_hours = float(match.group(3))
        return {
            'name_id': name_id,
            'gpu_hours': gpu_hours,
            'gpu_tres_hours': gpu_tres_hours
        }
    else:
        raise ValueError(f"Line format is incorrect: {line}")


def read_custom_csv(file_name: str) -> dict:
    """
    Reads a custom-formatted CSV file into a dictionary using 'name_id' as the key.
    
    :param file_name: Path to the custom-formatted CSV file.
    :return: Dictionary where key is 'name_id' and value is a dictionary with 'gpu_hours' and 'gpu_tres_hours'.
    """
    data = {}
    with open(file_name, mode='r') as file:
        for line in file:
            entry = parse_line(line.strip())
            name_id = entry['name_id']
            data[name_id] = {
                'gpu_hours': entry['gpu_hours'],
                'gpu_tres_hours': entry['gpu_tres_hours']
            }
    return data

def merge_dictionaries(dict1: dict, dict2: dict) -> dict:
    """
    Merges two dictionaries by summing 'gpu_hours' and 'gpu_tres_hours' for matching 'name_id' keys.
    
    :param dict1: First dictionary.
    :param dict2: Second dictionary.
    :return: Merged dictionary.
    """
    merged_data = dict1.copy()  # Start with all entries from dict1
    
    for name_id, values in dict2.items():
        if name_id in merged_data:
            # Sum the values for matching 'name_id'
            merged_data[name_id]['gpu_hours'] += values['gpu_hours']
            merged_data[name_id]['gpu_tres_hours'] += values['gpu_tres_hours']
        else:
            # Add new entry for 'name_id' not in dict1
            merged_data[name_id] = values
    
    return merged_data

# Process each pair of files 
def process_each_pair(file1, file2):
    """
    Processes each pair of files and saves their merged data into separate output files.
    
    :param file_pairs: List of tuples, each containing paths to two custom-formatted CSV files to merge.
    """
    # Read each file into a dictionary
    data1 = read_custom_csv(file1)
    data2 = read_custom_csv(file2)
        
    # Merge the dictionaries for the current pair
    merged_data = merge_dictionaries(data1, data2)

    return merged_data        
        


def write_dict_to_file(data_dict, file_name):
    """
    Write the dictionary to the specified file.
    """
    with open(file_name, 'w') as file:
        sorted_data_dict = dict(sorted(data_dict.items(), key=lambda x: x[1]['gpu_tres_hours'], reverse=True))
        for k, v in sorted_data_dict.items():
           file.write("name= {} , gpu_hours= {:0.1f}, gpu_tres_hours= {:0.1f} \n".format(k, v['gpu_hours'], v['gpu_tres_hours']))


# Example usage
file_pairs = [
    ('partition_dictionary_sum.csv', 'partition_dictionary.csv'),
    ('group_dictionary_sum.csv', 'group_dictionary.csv'),
    ('user_dictionary_sum.csv', 'user_dictionary.csv')
]

result_partition_dict = process_each_pair(file_pairs[0][0], file_pairs[0][1])
result_group_dict = process_each_pair(file_pairs[1][0], file_pairs[1][1])
result_user_dict = process_each_pair(file_pairs[2][0], file_pairs[2][1])

write_dict_to_file(result_partition_dict, "partition_dictionary_sum.csv")
write_dict_to_file(result_group_dict, "partition_group_sum.csv")
write_dict_to_file(result_user_dict, "partition_user_sum.csv")


