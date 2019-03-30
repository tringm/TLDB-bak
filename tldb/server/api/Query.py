import json

# {
#     "from": From,
#     "where": null | Proposition,
#     "get": null | Get,
#     "orderBy": null | OrderBy,
#     "select": null | Selection,
#     "offset": null | long,
#     "limit": null | long
# }


def query_parser(query):
    """

    :param jsonString:
    :return:
    """
    if "from" not in query:
        return False
    from_proposition = From(query["from"])


def query_validator(jsonObj):
    """
    Check if query contains all
    :param jsonObj:
    :return:
    """


class From:
    def __init__(self, something):
        print("init from", something)
