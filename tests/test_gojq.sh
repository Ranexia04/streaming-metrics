echo \
'
[3, 4]
' \
| docker run -i --rm itchyny/gojq \
'
now | strftime("%Y-%m-%dT%H:%M:%S")
'

