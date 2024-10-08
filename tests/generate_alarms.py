#!/usr/bin/env python3

import os
import sys


def make_clean():
    os.system("rm metrics/configs/TEST_*")
    os.system("rm -r metrics/TEST_*")
    os.system("rm -r metrics/groups/*")


def create_namespaces(n_namespaces: int):
    for i in range(n_namespaces):
        create_namespace(f"TEST_{i:010d}", "XPTO")

    os.makedirs(f"metrics/groups", exist_ok=True)
    with open(f"metrics/groups/groups.jq", mode='w') as cyaml:
        cyaml.write(group_str())


def create_namespace(name: str, group: str):
    os.makedirs(f"metrics/configs", exist_ok=True)
    os.makedirs(f"metrics/{name}", exist_ok=True)
    with open(f"metrics/configs/{name}.yaml", mode='w') as cyaml:
        cyaml.write(config(name, group))
    with open(f"metrics/{name}/filter.jq", mode='w') as fjq:
        fjq.write(filter_str(name))


def config(name: str, group: str) -> str:
    return f'''
group: {group}
namespace: {name}
'''


def group_str() -> str:
    return f'''
if .domain == "XPTO" then
    "XPTO"
else
    filter_error("ola")
end
'''


def filter_str(name: str) -> str:
    return f'''
"{name}" as $namespace |
.start_time as $time |
1 as $m |

select(.domain == "XPTO" and ( .code | ctest("ERR") )) // filter_error($namespace) |
log($namespace; .start_time; {{"count": 1, "dummy": "12345678901234567890123456789012345678901234567890"}} | map_values(.) )
'''


if __name__ == "__main__":
    n_namespaces = 1

    if len(sys.argv) == 1:
        print(f"Using default number of metrics: {n_namespaces}")
    else:
        n_namespaces = int(sys.argv[1])

    make_clean()

    create_namespaces(n_namespaces)

    #make_clean()
