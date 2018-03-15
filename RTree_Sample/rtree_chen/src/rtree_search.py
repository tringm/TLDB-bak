import time
from rtree import index
import os

# xjoin dirctly apply Rtree
# rtree dimensions : tagvalue.tagID1.tagID2.tagID3
# search dimensions : tablevalue.parentID1.parentID2.tagID3

def tutorial():
    idx = index.Index()
    left, bottom, right, top = (1, 1, 1, 1)
    idx.insert(0, (left, bottom, right, top), "asdf")
    left, bottom, right, top = (4, 4, 4, 4)
    idx.insert(0, (left, bottom, right, top), "fdsa")
    print([(x.id, x.object) for x in list(idx.intersection((0, 0, 4, 4), objects=True))])
    print(idx.bounds)
    return

def test_kd():
    #flush tree
    os.remove("3d_index.data")
    os.remove("3d_index.index")

    p = index.Property()
    p.dimension = 3
    p.dat_extension = 'data'
    p.idx_extension = 'index'
    idx3d = index.Index('3d_index', properties=p)
    idx3d.insert(1, (1,1,1, 1,1,1),"asdf")
    idx3d.insert(1, (2, 2, 2, 2, 2, 2), "das")
    print(idx3d.bounds)
    print([(x.id, x.object) for x in  list(idx3d.intersection((-1, -1, -1, 62, 22, 43), objects=True))])

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

def read_table(table_filename):
    lines = []
    for line in [line.rstrip('\n') for line in open(table_filename)]:
        lines.append(line.split(" "))
    return lines[0],lines[1:]

def build_tree(stream, data, dimension):
    lines = data

    #preparing the tree
    p = index.Property()
    p.dimension = dimension
    p.dat_extension = 'data'
    p.idx_extension = 'index'
    idxkd = index.Index(stream + '_index', properties=p)

    for line in lines:
        tmp = []
        for i in range(dimension):
            tmp.append(line[i])
        for i in range(dimension):
            tmp.append(line[i])
        idxkd.insert(1, (tmp), line[0])  # line[0] is the value line[1:] is the id
    return idxkd.bounds

def build_trees(dimension,xml_filename, tree_streams):
    # flush tree
    for stream in tree_streams:
        os.remove(stream + "_index.data")
        os.remove(stream + "_index.index")

    # +1 for value dimension
    dimension = dimension + 1
    for stream in tree_streams:
        print(build_tree(stream, read_stream(xml_filename, stream), dimension))

def get_search_bound(dimension, xml_filename, table_filename, search_streams, tree_streams):


    search_lines = read_stream(xml_filename, search_streams)

    search_stream_bounds = []
    for i in range(len(search_lines[0])): #depth of search stream
        search_stream_bounds.append(min([x[i] for x in search_lines]))
    for i in range(len(search_lines[0])):
        search_stream_bounds.append(max([x[i] for x in search_lines]))
    #print(search_stream_bounds)
    table_property, table_value = read_table(table_filename)


    # get the min and max bound for trees
    bound = []

    for stream in tree_streams:
        p = index.Property()
        p.dimension = dimension + 1
        p.dat_extension = 'data'
        p.idx_extension = 'index'
        idxkd = index.Index("test/" + stream + '_index', properties=p)
        bound.append([stream, idxkd.bounds])


    search_bound = []
    for i in range(len(bound)):
        stream_bound = [x[1] for x in bound if x[0]==table_property[i]][0]
        # print(stream_bound)
        min_table, max_table = min([float(x[i]) for x in table_value]),max([float(x[i]) for x in table_value])
        #print(min_table, max_table)
        #print(stream_bound)

        tmp = []
        #append min
        for j in range(int(len(stream_bound)/2)):
            #append min value
            if j == 0:
                tmp.append(min_table)
            #append min parent
            elif len(search_stream_bounds)/2 > j:
                tmp.append(search_stream_bounds[j])
            #append bound from stream
            else:
                tmp.append(stream_bound[j])

        # append max
        for j in range(int(len(stream_bound)/2)):
            #append max value
            if j == 0:
                tmp.append(max_table)
            #append max parent
            elif len(search_stream_bounds)/2 > j:
                tmp.append(search_stream_bounds[int(len(search_stream_bounds)/2) + j])
            #append bound from stream
            else:
                tmp.append(stream_bound[int(len(stream_bound)/2) + j])

        search_bound.append([table_property[i], tmp])

    return search_bound


#
def search(dimension, xml_filename, table_filename, search_streams, tree_streams):
    search_bound = get_search_bound(dimension, xml_filename, table_filename, search_streams, tree_streams)

    print (search_bound)

    start_time = time.time()

    for i in range(len(search_bound)):
        p = index.Property()
        p.dimension = dimension + 1
        p.dat_extension = 'data'
        p.idx_extension = 'index'
        idxkd = index.Index("test/" + search_bound[i][0] + '_index', properties=p)
        print(idxkd.bounds)
        # result = [(x.bounds,x.object) for x in idxkd.intersection(search_bound[i][1], objects=True)]
        result = [(x.bounds,x.object) for x in idxkd.intersection(search_bound[i][1], objects=True)]
        print(search_bound[i][0])
        print(len(result))

    elapsed_time = time.time() - start_time
    print(elapsed_time)


# test case
# X(A/B[/C]) R(B,C)
def test_1():
    dimension = 3
    xml_filename = "../data/"
    table_filename = "../data/tableBC.dat"
    search_streams = "A"
    tree_streams = ["B","C"]
    build_trees(dimension, xml_filename, tree_streams)
    search(dimension, xml_filename, table_filename, search_streams, tree_streams)

# order_id/price[/asin]
def test_2():
    dimension = 3
    xml_filename = "../data/"
    table_filename = "../data/asin_price.dat"
    search_streams = "orderLine"
    tree_streams = ["asin", "price"]
    # build_trees(dimension, xml_filename, tree_streams)


    search(dimension, xml_filename, table_filename, search_streams, tree_streams)


if __name__ == "__main__":
    #tutorial()
    #test_kd()
    test_2()
    print("done")
