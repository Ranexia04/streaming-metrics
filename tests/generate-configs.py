#!/usr/bin/env python3

import os
import sys
import random

from jinja2 import Template

def clean():
    os.system("rm -r ./namespaces")
    os.system("rm -r ./groups")
    os.system("rm -r ./filters")


def create_configs():
    create_groups()

    for namespace in namespaces:
        group=random.choice(groups)
        create_namespace(namespace, group)
        create_filter(namespace, group)

def create_groups():
    jinja_group_str = Template(group_str)
    redered_jinja_group_str = jinja_group_str.render(groups=groups)

    os.makedirs("./groups", exist_ok=True)
    with open("./groups/groups.jq", mode='w') as cyaml:
        cyaml.write(redered_jinja_group_str)

def create_namespace(namespace: str, group: str):
    os.makedirs(f"./namespaces", exist_ok=True)

    with open(f"./namespaces/{namespace}.yaml", mode='w') as cyaml:
        cyaml.write(namespace_str(namespace, group))

def create_filter(namespace: str, group: str):
    os.makedirs(f"./filters", exist_ok=True)

    with open(f"./filters/{namespace}.jq", mode='w') as fjq:
        fjq.write(filter_str(namespace, group))

group_str = '''
. as $message |
[] as $groups |
$groups |
{% for group in groups %}
if $message.domain == "{{ group }}" then
    . + ["{{ group }}"]
end |
{% endfor %}
.

'''

def namespace_str(name: str, group: str) -> str:
    return f'''
group: {group}
namespace: {name}
metrics:
    request_total_count:
        type: counter
        help: counter for request_total_count
    request_success_count:
        type: counter
        help: counter for request_success_count
    request_tech_error_count:
        type: counter
        help: counter for request_tech_error_count
    request_func_error_count:
        type: counter
        help: counter for request_func_error_count
    request_duration:
        type: histogram
        help: histogram for request_duration
'''

def filter_str(namespace: str, group: str) -> str:
    return f'''
select(.domain == "{group}" and ( .code | ctest("STATUS") )) // filter_error("{namespace}") |
log("{namespace}"; .start_time; {metric_str()} | map_values(.) )
'''

def metric_str():
    return '{"request_total_count": (1), "request_success_count": (if .code != "STATUS1" then 1 else 0 end), "request_tech_error_count": (if .code == "STATUS1" then 1 else 0 end), "request_func_error_count": (if .code == "STATUS2" then 1 else 0 end), "request_duration": (12.5 / 1000)}'

if __name__ == "__main__":
    try:
        n_namespaces = int(sys.argv[1])
    except IndexError:
        n_namespaces = 100

    try:
        n_groups = int(sys.argv[2])
    except IndexError:
        n_groups = 10

    namespaces = [f"NAMESPACE{i}" for i in range(n_namespaces)]
    groups = [f"GROUP{i}" for i in range(n_groups)]


    clean()

    create_configs()
