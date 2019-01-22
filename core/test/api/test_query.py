import json
from pprint import pprint
from core.main.api.Query import query_parser


with open('in/query1.json') as f:
    query = json.load(f)
    query_parser(query)

