from fastavro import writer, reader, parse_schema

schema = {
    'doc': 'A weather reading.',
    'name': 'Weather',
    'namespace': 'test',
    'type': 'record',
    'fields': [
        {'name': 'station', 'type': 'string'},
        {'name': 'time', 'type': 'long'},
        {'name': 'temp', 'type': 'int'},
    ],
}
parsed_schema = parse_schema(schema)

# 'records' can be an iterable (including generator)
records = [
    {u'station': u'011990-99999', u'temp': 0, u'time': 1433269388},
    {u'station': u'011990-99999', u'temp': 22, u'time': 1433270389},
    {u'station': u'011990-99999', u'temp': -11, u'time': 1433273379},
    {u'station': u'012650-99999', u'temp': 111, u'time': 1433275478},
]

# Writing to a file
with open('weather.avro', 'wb') as out:
    writer(out, parsed_schema, records)

# Reading from a file
with open('weather.avro', 'rb') as fo:
    for record in reader(fo):
        print(record, type(record))

# trying without a file
from io import BytesIO

fo = BytesIO()
writer(fo, parsed_schema, records)
fo.seek(0)
for record in reader(fo):
    print(record, type(record))

# trying a complex type...
from json import loads
child_schema = loads(open('example.avro.Favorites.avsc', 'rb').read())
parent_schema = loads(open('example.avro.ComplexUser.avsc', 'rb').read())
named_schemas = {}
parsed_child = parse_schema(child_schema, named_schemas)
parsed_parent = parse_schema(parent_schema, named_schemas)
print(named_schemas)
