# prometheus-slurm-exporter
[Prometheus](https://prometheus.io/) Exporter for Slurm, utilizing the [Prometheus Python client library](https://github.com/prometheus/client_python). Here is the sample snapshot of the dashboard. 

<p align="center">
  <img src="https://raw.githubusercontent.com/KempnerInstitute/kempner-prometheus-slurm-exporter/add-description/figures/png/dash_board_snapshot.png" width="80%" />
</p>

## Description

This repository contains collectors intended to be used with Prometheus to gather and export statistics from Slurm. Each collector focuses on a specific aspect of Slurm. These collectors were written based on the existing FASRC collectors https://github.com/fasrc/prometheus-slurm-exporter

### Kempner Node Status

This collector retrieves the current [state](http://slurm.schedmd.com/scontrol.html "scontrol") of all Kempner nodes in the cluster and calculates overall cluster statistics, such as the number of nodes down, nodes in use, and more. Metrics are defined and processed in the Python script `slurm_kempner_node_status_collector.py`.

### Kempner Partition Status

This collector gathers the current [showq](http://slurm.schedmd.com/showq.html "showq") information for all Kempner partitions. Refer to the Python script `slurm_kempner_partitionstats_collector.py` for metric details.

### Kempner Historical Usage

This collector pulls historical usage data using [sacct](http://slurm.schedmd.com/sacct.html "sacct") for all Kempner partitions. Metrics are defined in the Python script `slurm_kempner_partitionstats_collector.py`.

## Dashboards

Sample dashboards for the various collectors can be found in the dash board server. 

