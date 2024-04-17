#!/usr/bin/env python3

import os
import sys


def make_clean():
    os.system("rm monitors/configs/TEST_*")
    os.system("rm -r monitors/TEST_*")
    os.system("rm -r monitors/groups/*")

def create_monitors(n_monitors: int):
    for i_monitor in range(n_monitors):
        create_monitor(f"TEST_{i_monitor:010d}", "XPTO")

    os.makedirs(f"monitors/groups", exist_ok=True)
    with open(f"monitors/groups/groups.jq", mode='w') as cyaml:
        cyaml.write(group_str())

def create_monitor(name: str, group: str):
    os.makedirs(f"monitors/configs", exist_ok=True)
    os.makedirs(f"monitors/{name}", exist_ok=True)
    with open(f"monitors/configs/{name}.yaml", mode='w') as cyaml:
        cyaml.write(config(name, group))
    with open(f"monitors/{name}/filter.jq", mode='w') as fjq:
        fjq.write(filter_str(name))
    with open(f"monitors/{name}/lambda.jq", mode='w') as ljq:
        ljq.write(lambda_str())
    with open(f"monitors/{name}/monitor.jq", mode='w') as ajq:
        ajq.write(monitor_str(name))

def config(name: str, group: str) -> str:
    return f'''
group: {group}
namespace: {name}
granularity: 60
cardinality: 15
snapshot: 1
current: true
store_type: cached_pebble_store
'''

def group_str() -> str:
    return f'''
if .domain == "XPTO" then
    "XPTO"
else
    filter_error("ola")
end
'''

# store_type: cached_pebble_store

def filter_str(name: str) -> str:
    return f'''
"{name}" as $namespace |
.start_time as $time |
1 as $m |

select(.domain == "XPTO" and ( .code | ctest("ERR") )) // filter_error($namespace) |
log($namespace; .code; .start_time; {{"count": 1, "dummy": "12345678901234567890123456789012345678901234567890"}} | map_values(.) )
'''


def lambda_str() -> str:
    return '''
if ($state == null) then
    $metric
else
    $state | .count += $metric.count
end
'''

def monitor_str(name: str) -> str:
    return f'''
def filter_error($namespace): error($namespace);
def validate_namespace($namespace): select(.namespace == $namespace) // filter_error($namespace);

def machine_monitor_time: now | strftime("%Y-%m-%dT%H:%M:%S");

def monitor($namespace; $monitor_time): {{"namespace": $namespace, "time": machine_monitor_time, "store_time": $monitor_time, "monitor": .}};

def window_math: reduce .[] as $state (0; if ($state == null) then . else . + $state.count end);

"{name}" as $namespace |

.time as $monitor_time |

validate_namespace($namespace) | .windows | map_values(window_math) | monitor($namespace; $monitor_time)
'''

if __name__=="__main__":
    #print(datetime.datetime.now(datetime.timezone.utc).isoformat())

    n_monitors = 100

    if len(sys.argv) == 1:
        print(f"Using default number of monitors: {n_monitors}")
    else:
        n_monitors = int(sys.argv[1])

    make_clean()

    create_monitors(n_monitors)

    #make_clean()
