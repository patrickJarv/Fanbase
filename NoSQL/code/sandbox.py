import json

with open('../metro_data/metros.json', 'r') as file:
    data = json.load(file)

new_data = []
for val in data:
    val.pop('seasonArray')
    val['metro'] = val['metro'].replace('$', '').replace('#', '').replace('[', '').replace(']', '').replace('.', '').replace('/', '')
    buf = val['metro'].split(', ')
    metro = buf[0]
    region = ''
    if len(buf) > 1:
        region = buf[1]
    new_data.append({
        'metro': metro,
        'region': region,
        'population': val['population'],
        'teams': [team['team'] for team in val['teamArray']]
    })

with open('../metro_data/metros_edited.json', 'w') as file:
    json.dump(new_data, file, indent=4)