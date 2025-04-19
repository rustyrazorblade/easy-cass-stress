# cassandra-stress: A workload centric stress tool and framework designed for ease of use.

This project is a work in progress.

cassandra-stress is a configuration-based tool for doing benchmarks and testing simple data models for Apache Cassandra. 
Unfortunately, it can be challenging to configure a workload. There are fairly common data models and workloads seen on Apache Cassandra.  
This tool aims to provide a means of executing configurable, pre-defined profiles.

Full docs are here: https://rustyrazorblade.github.io/cassandra-stress/

# Installation

The easiest way to get started on Linux is to use system packages.
Instructions for installation can be found here: https://rustyrazorblade.github.io/cassandra-stress/#_installation


# Building

Clone this repo, then build with gradle:

    git clone https://github.com/rustyrazorblade/cassandra-stress.git
    cd cassandra-stress
    ./gradlew shadowJar

Use the shell script wrapper to start and get help:

    bin/cassandra-stress -h

# Examples

Time series workload with a billion operations:

    bin/cassandra-stress run BasicTimeSeries -i 1B

Key value workload with a million operations across 5k partitions, 50:50 read:write ratio:

    bin/cassandra-stress run KeyValue -i 1M -p 5k -r .5


Time series workload, using TWCS:

    bin/cassandra-stress run BasicTimeSeries -i 10M --compaction "{'class':'TimeWindowCompactionStrategy', 'compaction_window_size': 1, 'compaction_window_unit': 'DAYS'}"

Time series workload with a run lasting 1h and 30mins:

    bin/cassandra-stress run BasicTimeSeries -d "1h30m"

Time series workload with Cassandra Authentication enabled:

    bin/cassandra-stress run BasicTimeSeries -d '30m' -U '<username>' -P '<password>'
    **Note**: The quotes are mandatory around the username/password
    if they contain special chararacters, which is pretty common for password

# Generating docs

Docs are served out of /docs and can be rebuild using `./gradlew docs`.