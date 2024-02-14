#!/usr/bin/env bash

# requires a running ccm cluster (otherwise certain tests will fail)

set -x

print_shell() {
    # params
    # $1 = name
    # #2 = command
    echo "running $1"

    printf "$ %s\n" "$2" > manual/examples/"${1}.txt"
    eval $2 >> manual/examples/"${1}.txt"
    echo "Sleeping"
    sleep 5
}

# help
print_shell "easy-cass-stress-help" "bin/easy-cass-stress"

# key value
print_shell "easy-cass-stress-keyvalue" "bin/easy-cass-stress run KeyValue -n 10000"

# info
print_shell "info-key-value" "bin/easy-cass-stress info KeyValue"


# list all workloads
print_shell "list-all" "bin/easy-cass-stress list"

print_shell "field-example-book" 'bin/easy-cass-stress run KeyValue --field.keyvalue.value="book(20,40)"'



