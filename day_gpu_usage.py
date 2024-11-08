import re
import subprocess
import sys


def extract_gres_gpu(string):
    # Use regular expression to find the number after "gres/gpu="
    match = re.search(r'gres/gpu=(\d+)', string)

    if match:
        return int(match.group(1))  # Return the number as an integer
    else:
        return int(0) # Return None if not found



def extract_gpu_factor(input_string):
    # Split the string by commas to separate the different attributes and extract A100 or H100
    attributes = input_string.split(',')

    # Initialize the factor to one
    factor = 0.0

    # Iterate over each attribute to find the 'gres/gpu' part
    for attribute in attributes:
        if attribute.startswith('gres/gpu:'):
            # Extract the GPU type (if present)
            gpu_info = attribute.split('=')[0].replace('gres/gpu:', '')

            # Assign factor based on GPU type
            if 'h100' in gpu_info.lower():
                factor = wgpu['h100'] 
            elif 'a100' in gpu_info.lower():
                factor = wgpu['a100'] 

    return factor



def convert_to_hours(time_str):
    # Split the time string into components based on the presence of '-'
    if '-' in time_str:
        days_part, time_part = time_str.split('-')
        days = int(days_part)  # Convert days to integer
    else:
        time_part = time_str
        days = 0  # No days present in the format

    # Split the time part (hours:minutes:seconds)
    hours, minutes, seconds = map(int, time_part.split(':'))

    # Calculate total hours
    total_hours = days * 24 + hours + minutes / 60 + seconds / 3600

    return float(total_hours)


def update_dictionary(data_dict, name, t_time, g_time, g_tr_time):
    if name in data_dict:
        # If name exists, update the amount and quantity by adding
        data_dict[name]['total_hours'] += t_time 
        data_dict[name]['gpu_hours'] += g_time 
        data_dict[name]['gpu_tres_hours'] += g_tr_time 
    else:
        # If name doesn't exist, add a new entry
        data_dict[name] = {'total_hours': t_time, 'gpu_hours': g_time, 'gpu_tres_hours': g_tr_time}
        #print(data_dict)
    #return data_dict


def get_node_names():
    """
    Runs the shell command to get the list of node names and returns them as a list.
    """
    try:
        # Run the shell command and capture the output
        command = "sinfo -p kempner_requeue -N 1 | grep kempner | awk '{print $1}'"
        result = subprocess.check_output(command, shell=True, text=True)
        
        # Split the result into a list of node names (each line represents a node)
        node_names = result.strip().split('\n')
        return node_names
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while running the command: {e}")
        return []


def check_kempner_node(n_name, n_list, p_key):
    """
    Check if any node name matches a line and if the line does not contain 'kempner'.
    """
    for n in n_list: 
        if n_name in n_list and "kempner" not in p_key: 
            return "non-kempner"  
        else: 
            return p_key

def print_sorted_dictionary(data_dict): 
    sorted_data_dict = dict(sorted(data_dict.items(), key=lambda x: x[1]['gpu_tres_hours'], reverse=True))

    print("\nDictionary (sorted by gpu_rest_hourst):")
    for k, v in sorted_data_dict.items():
       print("name= {} , gpu_hours= {:0.1f}, gpu_tres_hours= {:0.1f}".format(k, v['gpu_hours'], v['gpu_tres_hours']))


def write_dict_to_file(data_dict, file_name):
    """
    Write the dictionary to the specified file.
    """
    with open(file_name, 'w') as file:
        sorted_data_dict = dict(sorted(data_dict.items(), key=lambda x: x[1]['gpu_tres_hours'], reverse=True))
        for k, v in sorted_data_dict.items():
           file.write("name= {} , gpu_hours= {:0.1f}, gpu_tres_hours= {:0.1f} \n".format(k, v['gpu_hours'], v['gpu_tres_hours']))

# Initialize an empty data dictionary
partition_dict = {}
user_dict = {}
group_dict = {}

#Get the list of kempner nodes
node_list = get_node_names()
wgpu = {'a100': 209.1, 'h100': 546.9}


# Get the input file name from the argument
input_file_name = sys.argv[1]


# Open the file and process it line by line
with open(input_file_name, 'r') as file:
    for line in file:
        # Filter lines containing the finished jobs
        if "gpu" in line and "RUNNING" not in line and "PENDING" not in line:
            # Split the line using the '|' separator
            fields = line.strip().split('|')
            # Ensure there are enough fields to avoid index errors
            if len(fields) >= 8:
                user_key = fields[2]
                group_key = fields[3].split(',')[0]
                partition_key = fields[4].split(',')[0]
                gpu_tfield = fields[5]
                gpu_count = extract_gres_gpu(fields[6])
                node_name = fields[7]
                cpu_count = fields[11]
                if gpu_count > 0:
                    gpu_thours = convert_to_hours(gpu_tfield)           
                    tres_factor = extract_gpu_factor(fields[6]) 
                    if tres_factor > 0:
                        gpu_hours = gpu_thours*gpu_count
                        gpu_tres_hours = gpu_hours*tres_factor
                        update_dictionary(user_dict, user_key, gpu_thours, gpu_hours, gpu_tres_hours)
                        if "kempner" in group_key:
                            update_dictionary(group_dict, group_key, gpu_thours, gpu_hours, gpu_tres_hours)
                        partition_name = check_kempner_node(node_name, node_list, partition_key)
                        #print(node_name, partition_name)
                        if "kempner" in partition_name:
                            update_dictionary(partition_dict, partition_name, gpu_thours, gpu_hours, gpu_tres_hours)


#print_sorted_dictionary(user_dict)
write_dict_to_file(user_dict, "user_dictionary.csv")
write_dict_to_file(group_dict, "group_dictionary.csv")
write_dict_to_file(partition_dict, "partition_dictionary.csv")


