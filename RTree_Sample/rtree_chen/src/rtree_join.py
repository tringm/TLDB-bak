import time
from rtree import index
import os

def tutorial():
    idx = index.Index()
    left, bottom, right, top = (1, 1, 1, 1)
    idx.insert(0, (left, bottom, right, top), "asdf")
    left, bottom, right, top = (4, 4, 4, 4)
    idx.insert(0, (left, bottom, right, top), "fdsa")
    print([(x.id, x.object) for x in list(idx.intersection((0, 0, 4, 4), objects=True))])
    print(idx.bounds)
    return


def read_stream(xml_filename, stream):
    lines = []
    tmp = []
    ids = [line.rstrip('\n') for line in open(xml_filename+stream+"_id.dat")]
    values = [line.rstrip('\n') for line in open(xml_filename+stream+"_v.dat")]
    for i in range(len(ids)):
        tmp.append(float(values[i]))
        for i in ids[i].split("."):
            tmp.append(int(i))
        lines.append(tmp)
        tmp = []
    return lines

def read_stream_index(xml_filename, stream):
    lines = []
    tmp = []
    ids = [line.rstrip('\n') for line in open(xml_filename+stream+"_id.dat")]
    values = [line.rstrip('\n') for line in open(xml_filename+stream+"_v.dat")]
    indexs = [line.rstrip('\n') for line in open(xml_filename+stream+"_v.dat")]
    for i in range(len(ids)):
        tmp.append(float(values[i]))
        for j in ids[i].split("."):
            tmp.append(int(j))
        lines.append(tmp)
        tmp.append(float(indexs[i]))
        tmp = []
    return lines

def build_tree(stream, data, parent_data, dimension ):
    lines = []

    parent_id_len = len(parent_data[0])-2
    i = 1
    for line in data:
        # print(line[1:parent_id_len+1])
        # parent_id = [x[parent_id_len+1] for x in parent_data if x[1:parent_id_len+1]== line[1:parent_id_len+1] ]
        # print(index)
        lines.append((line,i))
        i = i +1
    # print(lines)

    #preparing the tree
    p = index.Property()
    p.dimension = dimension
    p.dat_extension = 'data'
    p.idx_extension = 'index'
    idxkd = index.Index("tree/" + stream, properties=p)

    for line in lines:
        tmp = []
        #only 2 dimension
        tmp.append(line[0][0])
        tmp.append(line[1])
        tmp.append(line[0][0])
        tmp.append(line[1])
        # print("build tree")
        # print(tmp)
        idxkd.insert(1, (tmp), line[0])  # line[0] is the value line[1:] is the id
    # print("build tree")
    # print(idxkd.bounds)
    return idxkd.bounds

def build_trees(dimension,xml_filename, tree_streams, search_streams):
    # flush tree
    # for stream in tree_streams:
    #     os.remove("tree/" + stream + ".data")
    #     os.remove("tree/" + stream + ".index")

    for stream in tree_streams:
        print("build_trees")
        print(build_tree(stream, read_stream(xml_filename, stream), read_stream_index(xml_filename, search_streams), dimension))


def read_table(table_filename):
    lines = []
    for line in [line.rstrip('\n') for line in open(table_filename)]:
        lines.append(line.split(" "))
    return lines[0],lines[1:]

def get_search_bound(dimension, xml_filename, table_filename, search_streams, tree_streams):

    table_property, table_value = read_table(table_filename)
    # print("here")
    # print(table_property)
    # print(table_value)
    # get the min and max bound for trees parent
    parent_min = 0
    parent_max = 0
    for i in range(len(table_property)):
        if table_property[i]==search_streams:
            parent_min, parent_max = min([float(x[i]) for x in table_value]), max([float(x[i]) for x in table_value])
            # print(parent_min,parent_max)
        #first dimension
    search_bound = []
    for i in range(len(table_property)):
        tmp = []
        if table_property[i] in tree_streams:
            min_table, max_table = min([float(x[i]) for x in table_value]), max([float(x[i]) for x in table_value])
            tmp.append(min_table)
            tmp.append(parent_min)
            tmp.append(max_table)
            tmp.append(parent_max)
            # first dimension
            search_bound.append([table_property[i], tmp])
    return search_bound

def join(dimension, xml_filename, table_filename, search_streams, tree_streams):
    search_bound = get_search_bound(dimension, xml_filename, table_filename, search_streams, tree_streams)

    # print (search_bound)

    start_time = time.time()

    for i in range(len(search_bound)):
        p = index.Property()
        p.dimension = dimension
        p.dat_extension = 'data'
        p.idx_extension = 'index'
        idxkd = index.Index("tree/" + search_bound[i][0], properties=p)

        # print(idxkd.bounds)
        # result = [(x.bounds,x.object) for x in idxkd.intersection(search_bound[i][1], objects=True)]
        # print(search_bound[i][1])
        result = [(x.bounds,x.object) for x in idxkd.intersection(search_bound[i][1], objects=True)]
        print("join results")
        print(search_bound[i][0])
        print(len(result))
    print("elapsed_time")
    elapsed_time = time.time() - start_time
    print(elapsed_time)

def test_1():
    dimension = 2
    xml_filename = "../data/"
    table_filename = "../data/tableABC.dat"
    search_streams = "A"
    tree_streams = ["B","C"]
    build_trees(dimension, xml_filename, tree_streams, search_streams)
    # join(dimension, xml_filename, table_filename, search_streams, tree_streams)


def test_2():
    dimension = 2
    xml_filename = "../data/"
    table_filename = "../data/order_asin_price.dat"
    search_streams = "orderLine"
    tree_streams = ["asin", "price"]
    # build_trees(dimension, xml_filename, tree_streams,search_streams)
    join(dimension, xml_filename, table_filename, search_streams, tree_streams)

if __name__ == "__main__":
    #tutorial()
    test_2()
    print("done")