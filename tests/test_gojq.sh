echo \
'
[3, 4]
' \
| podman run -i --rm docker.io/itchyny/gojq \
'
now | strftime("%Y-%m-%dT%H:%M:%S")
'

