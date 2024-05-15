# easy-cass-stress: A workload centric stress tool and framework designed for ease of use.

This project is a work in progress.

cassandra-stress is a configuration-based tool for doing benchmarks and testing simple data models for Apache Cassandra. 
Unfortunately, it can be challenging to configure a workload. There are fairly common data models and workloads seen on Apache Cassandra.  
This tool aims to provide a means of executing configurable, pre-defined profiles.

Full docs are here: https://rustyrazorblade.github.io/easy-cass-stress/

# Installation

The easiest way to get started on Linux is to use system packages.
Instructions for installation can be found here: https://rustyrazorblade.github.io/easy-cass-stress/#_installation


# Building

Clone this repo, then build with gradle:

    git clone https://github.com/rustyrazorblade/easy-cass-stress.git
    cd easy-cass-stress
    ./gradlew shadowJar

Use the shell script wrapper to start and get help:

    bin/easy-cass-stress -h

# Compatibility Matrix

easy-cass-stress can be built in one version of Java/OpenJDK and can be run on some other version of Java/OpenJDK.

| easy-cass-stress | Java 1.8.0 | Java 11.0.x | Java 17.0.x | Java 21.0.x |
|------------------|------------|-------------|-------------|-------------|
| build            | [x]        | [x]         | [x]         | [ ]         |
| run              | [x]        | [x]         | [x]         | [ ]         |

easy-cass-stress can connect to many versions of Apache Cassandra clusters.

| easy-cass-stress | Cassandra 3.11.x  | Cassandra 4.0.x | Cassandra 4.1.x | Cassandra 5.x | DSE              |
|------------------|-------------------|-----------------|-----------------|---------------|------------------|
| tested on        | 3.11.0 to 3.11.17 | 4.0.0 to 4.0.12 | 4.1.0 to 4.1.4  | 5.0-beta1     | 6.8.37 to 6.8.47 |

# Examples

Time series workload with a billion operations:

    bin/easy-cass-stress run BasicTimeSeries -i 1B

Key value workload with a million operations across 5k partitions, 50:50 read:write ratio:

    bin/easy-cass-stress run KeyValue -i 1M -p 5k -r .5


Time series workload, using TWCS:

    bin/easy-cass-stress run BasicTimeSeries -i 10M --compaction "{'class':'TimeWindowCompactionStrategy', 'compaction_window_size': 1, 'compaction_window_unit': 'DAYS'}"

Time series workload with a run lasting 1h and 30mins:

    bin/easy-cass-stress run BasicTimeSeries -d "1h30m"

Time series workload with Cassandra Authentication enabled:

    bin/easy-cass-stress run BasicTimeSeries -d '30m' -U '<username>' -P '<password>'
    **Note**: The quotes are mandatory around the username/password
    if they contain special chararacters, which is pretty common for password

# Generating docs

Docs are served out of /docs and can be rebuild using `./gradlew docs`.
