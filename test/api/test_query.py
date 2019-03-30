import json
from tldb.core.main import query_parser


with open('in/query1.json') as f:
    query = json.load(f)
    query_parser(query)

