import pandas as pd
import numpy as np
import time

def load_data(files):
    start_time = time.time()
    tables = []
    for file in files:
        with open(file) as f:
            lines = f.read().splitlines()
            tables.append([x.split(" ") for x in lines])

    print("--- load data %s seconds ---" % (time.time() - start_time))
    return tables

def join_first_element(tables, attribute):
    for t in tables:
        if attribute in t[0]:
            print ([x[0] for x in t])
            print ([x[1:] for x in t])
    return

def optimal_join(tables, joining_order):

    result = []

    for i in range(len(joining_order)):
        joining_attribute = joining_order[:i+1]
        # print (joining_attribute)

        lastAttribute = joining_attribute[-1]
        # print(lastAttribute)
        if result == []:
            result = [x for x in tables if lastAttribute==x[0][0]]
            join_first_element(result, lastAttribute)
        result = []
    return

# filtering by nodes
def filter_nodes(tables, attributes):
    for attribute in attributes:
        dfs = []
        for t in tables:
            i = 0
            if attribute in t[0]:
                column = pd.DataFrame(t[1:], columns=t[0])[attribute].tolist()
                # print(df)
                # df.set_index(attribute)
                # if i == 0:
                #     dfs=column
                # else:
                #     dfs.join(df, on=attribute, how='inner')
                dfs.append(column)
                i = i + 1
        # print(pd.concat(dfs, axis=1, join='inner'))
        # print (dfs)
        if len(dfs) > 1:
            s_base = set(dfs[0]).intersection(*dfs)
            # for i in range(len(dfs)):
            #     if i ==0:
            #         s_base = set(dfs[i])
            #     else:
            #         s_base = s_base.intersection(dfs[i])
            # print (s_base)
            new_tables = []
            for t in tables:
                if attribute in t[0]:
                    k=0
                    for i in range(len(t[0])):
                        if t[0][i]==attribute:
                            k=i
                    tmp = [x for x in t[1:] if x[k] in s_base]
                    tmp.insert(0, t[0])
                    new_tables.append(tmp)
                else:
                    new_tables.append(t)
            tables = new_tables
        # else:
        #     print(dfs)
    return tables

# join table by table
# join by multiple indexes
# input tables -> table -> header(attribute name) rows(data)
# output join result

def join_by_result(tables,joining_orders):

    dfs = []
    # tmpPD = pd.DataFrame([],columns=joining_order)
    i=0
    # print(tables)
    indexes = []
    for t in tables:

        commom_indexes = []
        for index in t[0]:
            if index not in indexes:
                indexes.append(index)
            elif index != 'v':
                commom_indexes.append(index)

        df = pd.DataFrame(t[1:], columns=t[0])
        # if commom_indexes == []:
        #     df = pd.DataFrame(t[1:],columns=t[0])
        # else:
        #     df = pd.DataFrame(t[1:],columns=t[0]).set_index(commom_indexes)

        if not df.empty:
            # print('xxx')
            # print(df)
            df = df.drop_duplicates(subset=df.columns.values.tolist(), keep='first')
            # print(df)

        if commom_indexes != []:
            df = df.set_index(commom_indexes)





        # df['v'] = pd.Series(np.random.randint(1, size=len(t[1:])), index=df.index)
        # dfs.append(df)
        if i==0:
            tmpPD = pd.concat([df],  axis=1, join='inner')
            tmpPD = tmpPD.drop_duplicates(keep='first')
            print ('count:' + str(tmpPD.count()))
            # tmpPD = tmpPD.drop_duplicates(keep=False)
            # print(tmpPD.columns.values.tolist())
            # tmpPD.reset_index()
        else:
            # if not df.empty:
            #     df.drop_duplicates()
            # print ('tmpPD size:' + str(tmpPD.count()))
            # print ('df size:' + str(df.count()))
            print('xxx')
            print(tmpPD.columns.values.tolist())
            print(df.columns.values.tolist())
            print(commom_indexes)
            print('kkk')
            tmpPD = tmpPD.join(df, on=commom_indexes,how='inner') #tmpPD= pd.concat([tmpPD,df], axis=1, join='inner').T.drop_duplicates().T
            # tmpPD.drop_duplicates(keep=False)
            print ('result size:' + str(tmpPD.count()))
            tmpPD = tmpPD.drop_duplicates(keep='first')
            print ('countddddd:' + str(tmpPD.count()))
            # tmpPD.reset_index()
        # print(tmpPD.columns.values.tolist())
        # tmpPD.set_index(indexes)
        i=i+1
        print ('countccccc:' + str(tmpPD.count()))
    # print('qqqqqq')
    # print(tmpPD[joining_orders])
    # print('qqqqqq')

    return tmpPD.drop_duplicates(keep='first')

def join_by_result_no_cut(tables):
    dfs = []
    # tmpPD = pd.DataFrame([],columns=joining_order)
    i=0
    # print(tables)
    indexes = []
    for t in tables:
        commom_indexes = []
        for index in t[0]:
            if index not in indexes:
                indexes.append(index)
            elif index != 'v':
                commom_indexes.append(index)

        # if commom_indexes == []:
        #     df = pd.DataFrame(t[1:], columns=t[0])
        #     # df = df.drop_duplicates()
        # else:
        #     # df = pd.DataFrame(t[1:], columns=t[0]).set_index(commom_indexes)
        #     df = pd.DataFrame(t[1:],columns=t[0])
        #     # df = df.drop_duplicates(keep='first')
        #
        # # df['v'] = pd.Series(np.random.randint(1, size=len(t[1:])), index=df.index)
        # if not df.empty:
        #     df = df.drop_duplicates(keep='first')
        df = pd.DataFrame(t[1:], columns=t[0])
        # if commom_indexes == []:
        #     df = pd.DataFrame(t[1:],columns=t[0])
        # else:
        #     df = pd.DataFrame(t[1:],columns=t[0]).set_index(commom_indexes)

        if not df.empty:
            # print('xxx')
            # print(df)
            print(df.columns.values.tolist())
            df = df.drop_duplicates(subset=df.columns.values.tolist(), keep='first')
            # print(df)

        if commom_indexes != []:
            df = df.set_index(commom_indexes)


        # print(dfs)
        if i==0:
            tmpPD = pd.concat([df],  axis=1, join='inner')
            # print(tmpPD.columns.values.tolist())
        else:
            if len(commom_indexes) !=0 :
                # tmpPD.set_index(commom_indexes)
                tmpPD = tmpPD.join(df, on=commom_indexes,how='inner') #tmpPD= pd.concat([tmpPD,df], axis=1, join='inner').T.drop_duplicates().T
                # tmpPD.drop_duplicates(keep=False)
                print ('count:' + str(tmpPD.count()))
                tmpPD = tmpPD.drop_duplicates(keep='first')
            else:
                tmpPD['tmp'] = 1
                df['tmp'] = 1
                tmpPD = pd.merge(left=tmpPD, right=df, on='tmp',how='inner')
                tmpPD = tmpPD.drop_duplicates(keep='first')
                print ('count:' + str(tmpPD.count()))

        # print(tmpPD.columns.values.tolist())
        # tmpPD.set_index(indexes)
        i=i+1
        print ('counttable:' + str(tmpPD.count()))
    # print(tmpPD)
    # print("abc")
    return tmpPD.drop_duplicates(keep='first')


#return parent ID
def deweyID_parent(deweyID):
    return deweyID.rsplit('.', 1)[0]

#return ancestor ID
def deweyID_ancestor(deweyID):
    length = len(deweyID.split('.'))
    descendants =[]
    for i in range(length-1):
        descendants.append(deweyID.rsplit('.', i+1)[0])
    return descendants

def twig_node_pair(id_value_pairs):
    node_pairs = []
    for pair in id_value_pairs:
        id =[]
        with open(pair[0]) as f:
            id = f.read().splitlines()
        value=[]
        with open(pair[1]) as f:
            value = f.read().splitlines()
        node_pair = list(zip(id, value))
        header = [node_pair[0][1],node_pair[0][0],node_pair[0][1]+'_parent',node_pair[0][1]+'_descendant']
        # print(header)
        node_pair = [[x[1], x[0] ,deweyID_parent(x[0]),deweyID_ancestor(x[0])] for x in node_pair[1:]]
        node_pair.insert(0, header)
        node_pairs.append(node_pair)
    # print (node_pairs)
    return node_pairs

#twig node to rdb table by P-C relation
def twig_tables(node_pairs, attribute_pairs,A_D_pairs):
    new_node_pairs = []
    attribute_set = [] #see if it exits
    for attribute_pair in attribute_pairs:
        # print(attribute_pair)

        for i in range(len(attribute_pair)):
            if attribute_pair[i] not in attribute_set:
                attribute_set.append(attribute_pair[i])
                if i != 0:
                    node_pair = [x for x in node_pairs if x[0][0] == attribute_pair[i]][0]
                    new_node_pair = node_pair[1:]
                    #with descendant
                    # new_node_pair.insert(0, [node_pair[0][0],node_pair[0][1],attribute_pair[i-1]+'_id' ,node_pair[0][3]])
                    #without descendant
                    new_node_pair.insert(0, [node_pair[0][0],node_pair[0][1],attribute_pair[i-1]+'_id'])
                    #with descendant
                    # new_node_pairs.append(new_node_pair)
                    #without descendant
                    new_node_pairs.append([[x[0],x[1],x[2]] for x in new_node_pair])
                else:
                    # with descendant
                    # new_node_pairs.append([x for x in node_pairs if x[0][0] == attribute_pair[i]][0])
                    # without descendant
                    new_node_pairs.append([[y[0],y[1]] for y in [x for x in node_pairs if x[0][0] == attribute_pair[i]][0]])

    for a_d_pair in A_D_pairs:
        for i in range(len(a_d_pair[1])):
            if a_d_pair[1][i] not in attribute_set:
                attribute_set.append(a_d_pair[1][i])
                if i != 0:

                    node_pair = [x for x in node_pairs if x[0][0] == a_d_pair[1][i]][0]
                    #make child to greatchild
                    new_node_pair = []
                    #number of ad nodes to expend
                    num_ad_nodes = a_d_pair[0][0]
                    for k in range(num_ad_nodes):
                        # new_node_pair = new_node_pair + [[x[0],x[1],x[3][k],x[3]] for x in node_pair[1:] if len(x[3])>=num_ad_nodes]
                        #no descendant
                        new_node_pair = new_node_pair + [[x[0],x[1],x[3][k]] for x in node_pair[1:] if len(x[3])>=num_ad_nodes]

                        # new_node_pair = new_node_pair + [[x[0],x[1],x[2].rsplit('.', k)[0],x[3]] for x in node_pair[1:]]

                    #no descendant
                    # new_node_pair.insert(0, [node_pair[0][0],node_pair[0][1],a_d_pair[1][i-1]+'_id' ,node_pair[0][3]])
                    new_node_pair.insert(0, [node_pair[0][0],node_pair[0][1],a_d_pair[1][i-1]+'_id'])
                    new_node_pairs.append(new_node_pair)
                else:
                    # new_node_pairs.append([x for x in node_pairs if x[0][0] == a_d_pair[1][i]][0])
                    new_node_pairs.append([[y[0],y[1]] for y in[x for x in node_pairs if x[0][0] == a_d_pair[1][i]][0]])

                    # new_node_pairs.append([[y[0],y[1]] for y in [x for x in node_pairs if x[0][0] == attribute_pair[i]][0]])


    return new_node_pairs

#join twig first
def naive_join_twig_rdb_filter(twig_tables, rdb_tables,joining_orders, fraction, filename):
    start_time = time.time()
    # print (twig_tables)


    # rdb_tables = filter_nodes(new_rdbs, joining_orders)
    # twig_tables = filter_nodes(new_twigs, joining_orders)
    twig_tables = join_by_result_no_cut(twig_tables)
    print("--- %s filering time seconds ---" % (time.time() - start_time))

    commom_indexes = []

    df1 = join_twig(twig_tables)
    print('!!!!!!!!!!!!!')
    print(df1.columns.values.tolist())
    print(df1)
    print('!!!!!!!!!!!!!')


    # print(df1)
    df2 = join_by_result_no_cut(rdb_tables)

    print('~~~~~~~~~~~~~~~~')
    print(df2.columns.values.tolist())
    print(df2)
    print('~~~~~~~~~~~~~~~~')


    for index in df1.columns.values.tolist():
        if index in df2.columns.values.tolist():
            commom_indexes.append(index)
    print('#')
    print(commom_indexes)
    print('#')
    # df1.set_index(commom_indexes)
    # df2.set_index(commom_indexes)
    # print(df2)
    #df3 = df1.join(df2, how='inner')  # tmpPD= pd.concat([tmpPD,df], axis=1, join='inner').T.drop_duplicates().T
    # print(df3)
    # df3 = df1.join(df2, on=commom_indexes, how='inner')  # tmpPD= pd.concat([tmpPD,df], axis=1, join='inner').T.drop_duplicates().T
    #
    df3 = pd.merge(df1, df2, on=commom_indexes)
    print("--- %s joing time seconds ---" % (time.time() - start_time))
    return df3

#join all together
def naive_join_twig_rdb(twig_tables, rdb_tables, joining_orders, fraction, filename):


    new_twigs= []
    for t in twig_tables:
        cut = int(len(t)*fraction)
        new_twigs.append(t[:cut])
    twig_tables = new_twigs
    new_rdbs= []
    for t in rdb_tables:
        cut = int(len(t)*fraction)
        new_rdbs.append(t[:cut])
    rdb_tables = new_rdbs

    start_time = time.time()
    # rdb_tables = filter_nodes(rdb_tables, joining_orders)
    # twig_tables = filter_nodes(twig_tables, joining_orders)
    # print("--- %s filering time seconds ---" % (time.time() - start_time))

    commom_indexes = []

    print('```````````````````join twig``````````````````````')
    df1 = join_twig(twig_tables)
    print('`````````````````````join rdb``````````````````````')
    df2 = join_by_result(rdb_tables,joining_orders)
    print('```````````````````join twig+rdb```````````````')

    for index in df1.columns.values.tolist():
        if index in df2.columns.values.tolist():
            commom_indexes.append(index)
    # df1.set_index(commom_indexes)
    # df2.set_index(commom_indexes)
    # print(df2)
    # df3 = df1.join(df2, how='inner')  # tmpPD= pd.concat([tmpPD,df], axis=1, join='inner').T.drop_duplicates().T
    # print(df3)
    # df3 = df1.join(df2, on=commom_indexes, how='inner')  # tmpPD= pd.concat([tmpPD,df], axis=1, join='inner').T.drop_duplicates().T
    #
    df3 = pd.merge(df1, df2, on=commom_indexes)
    print("end of joining")
    real_join_time = time.time() - start_time
    print("--- %s real joining time seconds ---" % real_join_time)
    with open(filename, "a") as f:
        f.write(str(fraction) + ' ' + str(real_join_time) + "\n")  # quantity incategory
    print(df3.columns.values.tolist())
    # print(df3.head(10).values.tolist())
    return df3

def naive_join_rdb(rdb_tables, joining_orders, fraction, filename):


    new_rdbs= []
    for t in rdb_tables:
        cut = int(len(t)*fraction)
        new_rdbs.append(t[:cut])
    rdb_tables = new_rdbs

    start_time = time.time()
    # rdb_tables = filter_nodes(rdb_tables, joining_orders)
    # twig_tables = filter_nodes(twig_tables, joining_orders)
    # print("--- %s filering time seconds ---" % (time.time() - start_time))

    commom_indexes = []


    print('`````````````````````join rdb``````````````````````')
    df2 = join_by_result(rdb_tables,joining_orders)
    print("end of joining")
    real_join_time = time.time() - start_time
    print("--- %s real joining time seconds ---" % real_join_time)
    with open(filename, "a") as f:
        f.write(str(fraction) + ' ' + str(real_join_time) + "\n")  # quantity incategory
    print(df2.columns.values.tolist())
    # print(df3.head(10).values.tolist())
    return df2

#joining twig tables and rdb tables
def join_twig_rdb(twig_tables, rdb_tables,joining_orders, fraction, filename):
    print('xxx')
    if len(twig_tables)!=0:
        print('kkk')
        cc = 0
        for t in twig_tables:
            rdb_tables.insert(cc,t)
            # rdb_tables.append(t)
            cc =cc+1

    new_tables= []
    for t in rdb_tables:
        cut = int(len(t)*fraction)
        new_tables.append(t[:cut])
    # rdb_tables = filter_nodes(rdb_tables, joining_orders)
    # print("--- %s filter time seconds ---" % (time.time() - start_time))
    start_time = time.time()
    result = join_by_result(new_tables,joining_orders)
    real_join_time = time.time() - start_time
    print("--- %s joing time seconds ---" % real_join_time)
    with open(filename, "a") as f:
        f.write(str(fraction) + ' ' + str(real_join_time) + "\n")  # quantity incategory
    return result

#joining twig tables and rdb tables
def join_twig_rdb_filtered(twig_tables, rdb_tables,joining_orders):
    start_time = time.time()
    for t in twig_tables:
        rdb_tables.append(t)
    rdb_tables = filter_nodes(rdb_tables, joining_orders)
    print("--- %s filter time seconds ---" % (time.time() - start_time))
    result = join_by_result(rdb_tables,joining_orders)
    print("--- %s joing time seconds ---" % (time.time() - start_time))
    return result

def join_twig(twig_tables):
    start_time = time.time()
    # print (twig_tables)
    result = join_by_result_no_cut(twig_tables)
    # print(result)
    print("--- %s joing time seconds ---" % (time.time() - start_time))
    return result

def test():
    files = ['data/test_case1/rdb/table_invoice_asin.dat','data/test_case1/rdb/rdb_table6_mediate.dat', 'data/test_case1/rdb/rdb_table7_mediate.dat']
    joining_order = ['asin', 'price', 'OrderId']
    print (join_by_result(load_data(files), joining_order))
    return

def test_big():
    start_time = time.time()
    files = ['data/test_case1/rdb/table_asin_price_OrderId.dat','data/test_case1/rdb/table_invoice_asin.dat', 'data/test_case1/rdb/table_orderline_asin.dat', 'data/test_case1/rdb/table_invoice_orderline.dat']
    joining_order = ['asin', 'price', 'OrderId', 'invoice', 'orderline']
    # print ()
    print (join_by_result(load_data(files)))

    # print (join_by_result(filter_nodes(load_data(files), joining_order)))
    print("--- %s seconds ---" % (time.time() - start_time))
    return

def test_xml_twig1():
    start_time = time.time()
    # id_value_pairs = [['data/test_case1/xml/asin_id_small.dat','data/test_case1/xml/asin_v_index_small.dat'],['data/test_case1/xml/price_id_small.dat','data/test_case1/xml/price_v_small.dat'],['data/test_case1/xml/Orderline_id_small.dat','data/test_case1/xml/Orderline_v_small.dat']]#,['data/test_case1/xml/asdf_id_small.dat','data/test_case1/xml/asdf_v_index_small.dat']]
    id_value_pairs = [['data/test_case1/xml/asin_id.dat','data/test_case1/xml/asin_v_index.dat'],['data/test_case1/xml/price_id.dat','data/test_case1/xml/price_v.dat'],['data/test_case1/xml/orderline_id.dat','data/test_case1/xml/orderline_v.dat']]#,['data/test_case1/xml/asdf_id_small.dat','data/test_case1/xml/asdf_v_index_small.dat']]
    attribute_pairs = [['orderline', 'asin'],['orderline', 'price']]
    # print(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs))


    # rdb_files = ['data/test_case1/rdb/table_invoice_asin.dat','data/test_case1/rdb/rdb_table6_mediate.dat', 'data/test_case1/rdb/rdb_table7_mediate.dat']
    rdb_files = ['data/test_case1/rdb/table_asin_price_OrderId.dat','data/test_case1/rdb/table_invoice_asin.dat', 'data/test_case1/rdb/table_orderline_asin.dat', 'data/test_case1/rdb/table_invoice_orderline.dat']
    # for row in (load_data(rdb_files)):
    #     print (row)
    joining_order = ['asin', 'price', 'orderline', 'invoice', 'OrderId']
    commom_indexes = ['asin', 'price', 'orderline']
    # print(join_twig(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs))) #only twig
    print(join_by_result_no_cut(load_data(rdb_files)))  # only rdb
    # print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order,commom_indexes))
    # print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))

    print("--- %s total time  seconds ---" % (time.time() - start_time))

def test_xml_twig2():
    start_time = time.time()
    # id_value_pairs = [['data/test_case2/xml/item_id_small.dat','data/test_case2/xml/item_v_small.dat'],['data/test_case2/xml/incategory_id_small.dat','data/test_case2/xml/incategory_v_small.dat'],['data/test_case2/xml/quantity_id_small.dat','data/test_case2/xml/quantity_v_small.dat']]
    id_value_pairs = [['data/test_case2/xml/item_id.dat','data/test_case2/xml/item_v.dat'],['data/test_case2/xml/incategory_id.dat','data/test_case2/xml/incategory_v.dat'],['data/test_case2/xml/quantity_id.dat','data/test_case2/xml/quantity_v.dat']]
    attribute_pairs = [['item', 'incategory'],['item', 'quantity']]
    # print(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs))


    # rdb_files = ['data/test_case2/rdb/table_asin_price_OrderId.dat','data/test_case2/rdb/table_invoice_asin.dat', 'data/test_case2/rdb/table_orderline_asin.dat']
    # rdb_files = ['data/test_case2/rdb/rdb_table5_mediate_small.dat','data/test_case2/rdb/rdb_table6_mediate_small.dat', 'data/test_case2/rdb/rdb_table7_mediate_small.dat']
    rdb_files = ['data/test_case2/rdb/rdb_table5_mediate.dat','data/test_case2/rdb/rdb_table6_mediate.dat', 'data/test_case2/rdb/rdb_table7_mediate.dat']
    # for row in (load_data(rdb_files)):
    #     print (row)
    joining_order = ['item', 'incategory', 'quantity']
    commom_indexes = ['item', 'incategory', 'quantity']
    # print(join_twig(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs)))
    # print(join_by_result_no_cut(load_data(rdb_files)))  # only rdb
    # print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order,commom_indexes))
    print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))

    print("--- %s total time  seconds ---" % (time.time() - start_time))



def test_xml_twig1_1():
    start_time = time.time()
    # id_value_pairs = [['data/test_case1/xml/asin_id_small.dat','data/test_case1/xml/asin_v_index_small.dat'],['data/test_case1/xml/price_id_small.dat','data/test_case1/xml/price_v_small.dat'],['data/test_case1/xml/Orderline_id_small.dat','data/test_case1/xml/Orderline_v_small.dat']]#,['data/test_case1/xml/asdf_id_small.dat','data/test_case1/xml/asdf_v_index_small.dat']]
    id_value_pairs = [['data/test_case1/xml/asin_id.dat','data/test_case1/xml/asin_v_index.dat'],['data/test_case1/xml/price_id.dat','data/test_case1/xml/price_v.dat'],['data/test_case1/xml/orderline_id.dat','data/test_case1/xml/orderline_v.dat'],['data/test_case1/xml/invoice_id.dat','data/test_case1/xml/invoice_v.dat'],['data/test_case1/xml/OrderId_id.dat','data/test_case1/xml/OrderId_v.dat']]#,['data/test_case1/xml/asdf_id_small.dat','data/test_case1/xml/asdf_v_index_small.dat']]
    attribute_pairs = [['invoice','OrderId'],['invoice','orderline', 'asin'],['invoice','orderline', 'price']]
    # print(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs))


    # rdb_files = ['data/test_case1/rdb/table_invoice_asin.dat','data/test_case1/rdb/rdb_table6_mediate.dat', 'data/test_case1/rdb/rdb_table7_mediate.dat']
    rdb_files = ['data/test_case1/rdb/table_asin_price_OrderId.dat','data/test_case1/rdb/table_invoice_asin.dat', 'data/test_case1/rdb/table_orderline_asin.dat', 'data/test_case1/rdb/table_invoice_orderline.dat']
    # for row in (load_data(rdb_files)):
    #     print (row)
    joining_order = ['asin', 'price', 'orderline', 'invoice', 'OrderId']
    commom_indexes = ['asin', 'price', 'orderline','invoice', 'OrderId']
    print(join_twig(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs))) #only twig
    # print(join_by_result_no_cut(load_data(rdb_files)))  # only rdb
    # print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order,commom_indexes))
    # print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))

    print("--- %s total time  seconds ---" % (time.time() - start_time))

def test_case3_small():
    start_time = time.time()
    id_value_pairs = [['data/test_case3/small/asin_id.dat','data/test_case3/small/asin_v.dat'],['data/test_case3/small/price_id.dat','data/test_case3/small/price_v.dat'],['data/test_case3/small/orderline_id.dat','data/test_case3/small/orderline_v.dat']]#,['data/test_case1/xml/asdf_id_small.dat','data/test_case1/xml/asdf_v_index_small.dat']]
    attribute_pairs = [['orderline', 'asin'],['orderline', 'price']]
    # print(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs))


    # rdb_files = ['data/test_case1/rdb/table_invoice_asin.dat','data/test_case1/rdb/rdb_table6_mediate.dat', 'data/test_case1/rdb/rdb_table7_mediate.dat']
    rdb_files = ['data/test_case3/small/price_asin_table.dat','data/test_case3/small/orderline_asin_table.dat']
    # for row in (load_data(rdb_files)):
    #     print (row)
    joining_order = ['asin', 'price', 'orderline']
    commom_indexes = ['asin', 'price', 'orderline','invoice', 'OrderId']
    #filtered
    rdb_tables = filter_nodes(rdb_tables, joining_orders)
    # print(join_twig(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs))) #only twig
    # print(join_by_result_no_cut(load_data(rdb_files)))  # only rdb
    # print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order,commom_indexes))
    print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))

    print("--- %s total time  seconds ---" % (time.time() - start_time))

def test_case3_big():
    start_time = time.time()
    id_value_pairs = [['data/test_case3/big5k/asin_id.dat','data/test_case3/big5k/asin_v.dat'],['data/test_case3/big5k/price_id.dat','data/test_case3/big5k/price_v.dat'],['data/test_case3/big5k/orderline_id.dat','data/test_case3/big5k/orderline_v.dat']]#,['data/test_case1/xml/asdf_id_small.dat','data/test_case1/xml/asdf_v_index_small.dat']]
    attribute_pairs = [['orderline', 'asin'],['orderline', 'price']]
    # print(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs))


    # rdb_files = ['data/test_case1/rdb/table_invoice_asin.dat','data/test_case1/rdb/rdb_table6_mediate.dat', 'data/test_case1/rdb/rdb_table7_mediate.dat']
    rdb_files = ['data/test_case3/big5k/price_asin_table.dat','data/test_case3/big5k/orderline_asin_table.dat']
    # for row in (load_data(rdb_files)):
    #     print (row)
    joining_order = ['asin', 'price', 'orderline']
    commom_indexes = ['asin', 'price', 'orderline','invoice', 'OrderId']
    # print(join_twig(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs))) #only twig
    # print(join_by_result_no_cut(load_data(rdb_files)))  # only rdb
    # print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order,commom_indexes))
    print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))

    print("--- %s total time  seconds ---" % (time.time() - start_time))

def evaluation_small_naive_q1():
    start_time = time.time()
    id_value_pairs = [['data/test_case3/small/asin_id.dat', 'data/test_case3/small/asin_v.dat'],
                      ['data/test_case3/small/price_id.dat', 'data/test_case3/small/price_v.dat'],
                      ['data/test_case3/small/orderline_id.dat',
                       'data/test_case3/small/orderline_v.dat']]
    attribute_pairs = [['orderline', 'asin'], ['orderline', 'price']]
    rdb_files = ['data/test_case3/small/table_asin_price_OrderId.dat', 'data/test_case3/small/table_invoice_asin.dat',
                 'data/test_case3/small/table_invoice_orderline.dat',  'data/test_case3/small/table_orderline_asin.dat']
    joining_order = ['asin', 'price', 'orderline']
    commom_indexes = ['asin', 'price', 'orderline']


    # print(join_twig(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs)))
    # print(join_by_result_no_cut(load_data(rdb_files)))  # only rdb
    # filter
    # print (naive_join_twig_rdb_filter(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order,commom_indexes))
    # unfilter
    # print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order,commom_indexes))

    # filtered
    print (join_twig_rdb_filtered(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))
    # unfiltered
    # print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))
    print("--- %s total time  seconds ---" % (time.time() - start_time))
    return

def evaluation_mediate_naive_q1():
    start_time = time.time()
    id_value_pairs = [['data/test_case3/mediate/asin_id.dat', 'data/test_case3/mediate/asin_v.dat'],
                      ['data/test_case3/mediate/price_id.dat', 'data/test_case3/mediate/price_v.dat'],
                      ['data/test_case3/mediate/orderline_id.dat',
                       'data/test_case3/mediate/orderline_v.dat']]
    attribute_pairs = [['orderline', 'asin'], ['orderline', 'price']]
    rdb_files = ['data/test_case3/mediate/table_asin_price_OrderId.dat', 'data/test_case3/mediate/table_invoice_asin.dat',
                 'data/test_case3/mediate/table_invoice_orderline.dat',  'data/test_case3/mediate/table_orderline_asin.dat']
    joining_order = ['asin', 'price', 'orderline']
    commom_indexes = ['asin', 'price', 'orderline']


    # print(join_twig(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs)))
    # print(join_by_result_no_cut(load_data(rdb_files)))  # only rdb
    # filter
    # print (naive_join_twig_rdb_filter(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order,commom_indexes))
    # unfilter
    # print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order,commom_indexes))

    # filtered
    # print (join_twig_rdb_filtered(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))
    # unfiltered
    print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))
    print("--- %s total time  seconds ---" % (time.time() - start_time))
    return

def evaluation_big_naive_q1():
    start_time = time.time()
    id_value_pairs = [['data/test_case3/big5k/asin_id.dat', 'data/test_case3/big5k/asin_v.dat'],
                      ['data/test_case3/big5k/price_id.dat', 'data/test_case3/big5k/price_v.dat'],
                      ['data/test_case3/big5k/orderline_id.dat',
                       'data/test_case3/big5k/orderline_v.dat']]
    attribute_pairs = [['orderline', 'asin'], ['orderline', 'price']]
    rdb_files = ['data/test_case3/big5k/table_asin_price_OrderId.dat', 'data/test_case3/big5k/table_invoice_asin.dat',
                 'data/test_case3/big5k/table_invoice_orderline.dat', 'data/test_case3/big5k/table_orderline_asin.dat']
    joining_order = ['asin', 'price', 'orderline']
    commom_indexes = ['asin', 'price', 'orderline']

    # print(join_twig(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs)))
    # print(join_by_result_no_cut(load_data(rdb_files)))  # only rdb
    # filter
    # print (naive_join_twig_rdb_filter(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order,commom_indexes))
    # unfilter
    # print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order,commom_indexes))

    # filtered
    # print (join_twig_rdb_filtered(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))
    # unfiltered
    print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))
    print("--- %s total time  seconds ---" % (time.time() - start_time))
    return

def evaluation_small_naive_q2():
    start_time = time.time()
    id_value_pairs = [['data/test_case3/small/asin_id.dat', 'data/test_case3/small/asin_v.dat'],
                      ['data/test_case3/small/price_id.dat', 'data/test_case3/small/price_v.dat'],
                      ['data/test_case3/small/orderline_id.dat','data/test_case3/small/orderline_v.dat'],
                      ['data/test_case3/small/invoice_id.dat', 'data/test_case3/small/invoice_v.dat'],
                      ['data/test_case3/small/OrderId_id.dat', 'data/test_case3/small/OrderId_v.dat']
                      ]
    attribute_pairs = [['invoice','OrderId'], ['invoice','orderline', 'asin'], ['invoice','orderline', 'price']]
    rdb_files = ['data/test_case3/small/table_asin_price_OrderId.dat', 'data/test_case3/small/table_invoice_asin.dat',
                 'data/test_case3/small/table_invoice_orderline.dat', 'data/test_case3/small/table_invoice_asin.dat',
                 'data/test_case3/small/table_orderline_asin.dat']
    joining_order = ['asin', 'price', 'orderline','invoice', 'OrderId']
    commom_indexes = ['asin', 'price', 'orderline']


    # print(join_twig(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs)))
    # print(join_by_result_no_cut(load_data(rdb_files)))  # only rdb
    # filter
    # print (naive_join_twig_rdb_filter(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order,commom_indexes))
    # unfilter
    print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order,commom_indexes))

    # filtered
    # print (join_twig_rdb_filtered(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))
    # unfiltered
    # print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))
    print("--- %s total time  seconds ---" % (time.time() - start_time))
    return


def evaluation_mediate_naive_q2():
    start_time = time.time()
    id_value_pairs = [['data/test_case3/mediate/asin_id.dat', 'data/test_case3/mediate/asin_v.dat'],
                      ['data/test_case3/mediate/price_id.dat', 'data/test_case3/mediate/price_v.dat'],
                      ['data/test_case3/mediate/orderline_id.dat','data/test_case3/mediate/orderline_v.dat'],
                      ['data/test_case3/mediate/invoice_id.dat', 'data/test_case3/mediate/invoice_v.dat'],
                      ['data/test_case3/mediate/OrderId_id.dat', 'data/test_case3/mediate/OrderId_v.dat']
                      ]
    attribute_pairs = [['invoice','OrderId'], ['invoice','orderline', 'asin'], ['invoice','orderline', 'price']]
    rdb_files = ['data/test_case3/mediate/table_asin_price_OrderId.dat', 'data/test_case3/mediate/table_invoice_asin.dat',
                 'data/test_case3/mediate/table_invoice_orderline.dat', 'data/test_case3/mediate/table_invoice_asin.dat',
                 'data/test_case3/mediate/table_orderline_asin.dat']
    joining_order = ['asin', 'price', 'orderline','invoice', 'OrderId']
    commom_indexes = ['asin', 'price', 'orderline']


    # print(join_twig(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs)))
    # print(join_by_result_no_cut(load_data(rdb_files)))  # only rdb
    # filter
    # print (naive_join_twig_rdb_filter(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order,commom_indexes))
    # unfilter
    print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order,commom_indexes))

    # filtered
    # print (join_twig_rdb_filtered(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))
    # unfiltered
    # print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))
    print("--- %s total time  seconds ---" % (time.time() - start_time))
    return


def evaluation_big_naive_q2():
    start_time = time.time()
    start_time = time.time()
    id_value_pairs = [['data/test_case3/big5k/asin_id.dat', 'data/test_case3/big5k/asin_v.dat'],
                      ['data/test_case3/big5k/price_id.dat', 'data/test_case3/big5k/price_v.dat'],
                      ['data/test_case3/big5k/orderline_id.dat','data/test_case3/big5k/orderline_v.dat'],
                      ['data/test_case3/big5k/invoice_id.dat', 'data/test_case3/big5k/invoice_v.dat'],
                      ['data/test_case3/big5k/OrderId_id.dat', 'data/test_case3/big5k/OrderId_v.dat']
                      ]
    attribute_pairs = [['invoice','OrderId'], ['invoice','orderline', 'asin'], ['invoice','orderline', 'price']]
    rdb_files = ['data/test_case3/big5k/table_asin_price_OrderId.dat', 'data/test_case3/big5k/table_invoice_asin.dat',
                 'data/test_case3/big5k/table_invoice_orderline.dat', 'data/test_case3/big5k/table_invoice_asin.dat',
                 'data/test_case3/big5k/table_orderline_asin.dat']

    joining_order = ['asin', 'price', 'orderline','invoice', 'OrderId']
    commom_indexes = ['asin', 'price', 'orderline']


    # print(join_twig(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs)))
    # print(join_by_result_no_cut(load_data(rdb_files)))  # only rdb
    # filter
    # print (naive_join_twig_rdb_filter(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order,commom_indexes))
    # unfilter
    print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order,commom_indexes))

    # filtered
    # print (join_twig_rdb_filtered(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))
    # unfiltered
    # print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))
    print("--- %s total time  seconds ---" % (time.time() - start_time))
    return


def evaluation_small_naive_q3():
    start_time = time.time()
    id_value_pairs = [['data/test_case3/small/asin_id.dat', 'data/test_case3/small/asin_v.dat'],

                      ['data/test_case3/small/invoice_id.dat', 'data/test_case3/small/invoice_v.dat'],
                      ['data/test_case3/small/OrderId_id.dat', 'data/test_case3/small/OrderId_v.dat']
                      ]
    attribute_pairs = [['invoice','OrderId']]
    A_D_pair = [[[2],['invoice','asin']]]
    rdb_files = ['data/test_case3/small/table_asin_price_OrderId.dat', 'data/test_case3/small/table_invoice_asin.dat',
                 'data/test_case3/small/table_invoice_orderline.dat', 'data/test_case3/small/table_invoice_asin.dat',
                 'data/test_case3/small/table_orderline_asin.dat']
    joining_order = ['asin', 'price', 'orderline','invoice', 'OrderId']
    commom_indexes = ['invoice', 'OrderId', 'asin']


    # print(join_twig(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs)))
    # print(join_by_result_no_cut(load_data(rdb_files)))  # only rdb
    # filter
    # print (naive_join_twig_rdb_filter(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order,commom_indexes))
    # unfilter
    print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order,commom_indexes))

    # filtered
    # print (join_twig_rdb_filtered(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))
    # unfiltered
    # print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs, A_D_pair),load_data(rdb_files), joining_order))
    print("--- %s total time  seconds ---" % (time.time() - start_time))
    return


def evaluation_mediate_naive_q3():
    start_time = time.time()
    id_value_pairs = [['data/test_case3/mediate/asin_id.dat', 'data/test_case3/mediate/asin_v.dat'],
                      ['data/test_case3/mediate/invoice_id.dat', 'data/test_case3/mediate/invoice_v.dat'],
                      ['data/test_case3/mediate/OrderId_id.dat', 'data/test_case3/mediate/OrderId_v.dat']
                      ]
    attribute_pairs = [['invoice','OrderId']]
    A_D_pair = [[[2],['invoice','asin']]]
    rdb_files = ['data/test_case3/mediate/table_asin_price_OrderId.dat', 'data/test_case3/mediate/table_invoice_asin.dat',
                 'data/test_case3/mediate/table_invoice_orderline.dat', 'data/test_case3/mediate/table_invoice_asin.dat',
                 'data/test_case3/mediate/table_orderline_asin.dat']
    joining_order = ['asin', 'price', 'orderline','invoice', 'OrderId']
    commom_indexes = ['asin', 'price', 'orderline']


    # print(join_twig(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs)))
    # print(join_by_result_no_cut(load_data(rdb_files)))  # only rdb
    # filter
    # print (naive_join_twig_rdb_filter(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order,commom_indexes))
    # unfilter
    # print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order,commom_indexes))

    # filtered
    # print (join_twig_rdb_filtered(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))
    # unfiltered
    print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs, A_D_pair),load_data(rdb_files), joining_order))
    print("--- %s total time  seconds ---" % (time.time() - start_time))
    return


def evaluation_big_naive_q3():
    start_time = time.time()
    start_time = time.time()
    id_value_pairs = [['data/test_case3/big5k/asin_id.dat', 'data/test_case3/big5k/asin_v.dat'],
                      ['data/test_case3/big5k/invoice_id.dat', 'data/test_case3/big5k/invoice_v.dat'],
                      ['data/test_case3/big5k/OrderId_id.dat', 'data/test_case3/big5k/OrderId_v.dat']
                      ]
    attribute_pairs = [['invoice','OrderId']]
    A_D_pair = [[[2],['invoice','asin']]]
    rdb_files = ['data/test_case3/big5k/table_asin_price_OrderId.dat', 'data/test_case3/big5k/table_invoice_asin.dat',
                 'data/test_case3/big5k/table_invoice_orderline.dat', 'data/test_case3/big5k/table_invoice_asin.dat',
                 'data/test_case3/big5k/table_orderline_asin.dat']

    joining_order = ['asin', 'price', 'orderline','invoice', 'OrderId']
    commom_indexes = ['asin', 'price', 'orderline']


    # print(join_twig(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs)))
    # print(join_by_result_no_cut(load_data(rdb_files)))  # only rdb
    # filter
    # print (naive_join_twig_rdb_filter(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order,commom_indexes))
    # unfilter
    print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order,commom_indexes))

    # filtered
    # print (join_twig_rdb_filtered(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))
    # unfiltered
    # print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order))
    print("--- %s total time  seconds ---" % (time.time() - start_time))
    return

def evaluation_supersmall_naive_q3():
    start_time = time.time()
    start_time = time.time()
    id_value_pairs = [['data/test_case3/supersmall/asin_id.dat', 'data/test_case3/supersmall/asin_v.dat'],
                      ['data/test_case3/supersmall/invoice_id.dat', 'data/test_case3/supersmall/invoice_v.dat'],
                      ['data/test_case3/supersmall/OrderId_id.dat', 'data/test_case3/supersmall/OrderId_v.dat']
                      ]
    attribute_pairs = [['invoice','OrderId']]
    A_D_pair = [[[3],['invoice','asin']]]
    rdb_files = ['data/test_case3/supersmall/table_asin_price_OrderId.dat', 'data/test_case3/supersmall/table_invoice_asin.dat',
                 'data/test_case3/supersmall/table_invoice_orderline.dat', 'data/test_case3/supersmall/table_invoice_asin.dat',
                 'data/test_case3/supersmall/table_orderline_asin.dat']

    joining_order = ['asin', 'price', 'orderline','invoice', 'OrderId']
    commom_indexes = ['invoice', 'OrderId']



    # print(join_twig(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs)))
    # print(join_by_result_no_cut(load_data(rdb_files)))  # only rdb
    # filter
    # print (naive_join_twig_rdb_filter(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order,commom_indexes))
    # unfilter
    # print(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair))
    # print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order,commom_indexes))

    # filtered
    # print (join_twig_rdb_filtered(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))
    # unfiltered
    print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order))
    print("--- %s total time  seconds ---" % (time.time() - start_time))
    return

def evaluation_naive_q1():
    start_time = time.time()
    folder = 'test_case3'
    data_size ='big5k'
    id_value_pairs = [['data/'+folder+'/'+data_size+'//asin_id.dat', 'data/'+folder+'/'+data_size+'//asin_v.dat'],
                      ['data/'+folder+'/'+data_size+'//price_id.dat', 'data/'+folder+'/'+data_size+'//price_v.dat'],
                      ['data/'+folder+'/'+data_size+'//orderline_id.dat',
                       'data/'+folder+'/'+data_size+'//orderline_v.dat']]
    attribute_pairs = [['orderline', 'asin'], ['orderline', 'price']]
    A_D_pair = []
    rdb_files = ['data/'+folder+'/'+data_size+'//table_asin_price_OrderId.dat', 'data/'+folder+'/'+data_size+'//table_invoice_asin.dat',
                 'data/'+folder+'/'+data_size+'//table_invoice_orderline.dat', 'data/'+folder+'/'+data_size+'//table_orderline_asin.dat']
    joining_order = ['asin', 'price', 'orderline']
    commom_indexes = ['asin', 'price', 'orderline']

    # print(join_twig(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs)))
    # print(join_by_result_no_cut(load_data(rdb_files)))  # only rdb
    # filter
    # print (naive_join_twig_rdb_filter(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order,commom_indexes))
    # unfilter
    print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order,commom_indexes))

    # filtered
    # print (join_twig_rdb_filtered(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))
    # unfiltered
    # print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order))
    print("--- %s total time  seconds ---" % (time.time() - start_time))
    return

def evaluation_naive_q2():
    start_time = time.time()
    folder = 'test_case3'
    data_size ='big5k'
    A_D_pair = []
    id_value_pairs = [['data/'+folder+'/'+data_size+'//asin_id.dat', 'data/'+folder+'/'+data_size+'//asin_v.dat'],
                      ['data/'+folder+'/'+data_size+'//price_id.dat', 'data/'+folder+'/'+data_size+'//price_v.dat'],
                      ['data/'+folder+'/'+data_size+'//orderline_id.dat','data/'+folder+'/'+data_size+'//orderline_v.dat'],
                      ['data/'+folder+'/'+data_size+'//invoice_id.dat', 'data/'+folder+'/'+data_size+'//invoice_v.dat'],
                      ['data/'+folder+'/'+data_size+'//OrderId_id.dat', 'data/'+folder+'/'+data_size+'//OrderId_v.dat']
                      ]
    attribute_pairs = [['invoice','OrderId'], ['invoice','orderline', 'asin'], ['invoice','orderline', 'price']]
    rdb_files = ['data/'+folder+'/'+data_size+'//table_asin_price_OrderId.dat', 'data/'+folder+'/'+data_size+'//table_invoice_asin.dat',
                 'data/'+folder+'/'+data_size+'//table_invoice_orderline.dat', 'data/'+folder+'/'+data_size+'//table_invoice_asin.dat',
                 'data/'+folder+'/'+data_size+'//table_orderline_asin.dat']

    joining_order = ['asin', 'price', 'orderline','invoice', 'OrderId']
    commom_indexes = ['asin', 'price', 'orderline']


    # print(join_twig(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs)))
    # print(join_by_result_no_cut(load_data(rdb_files)))  # only rdb
    # filter
    # print (naive_join_twig_rdb_filter(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order,commom_indexes))
    # unfilter
    # print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order,commom_indexes))

    # filtered
    # print (join_twig_rdb_filtered(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))
    # unfiltered
    print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order))
    print("--- %s total time  seconds ---" % (time.time() - start_time))
    return

def evaluation_naive_q3():
    start_time = time.time()
    folder = 'test_case3'
    data_size ='big5k'
    id_value_pairs = [['data/'+folder+'/'+data_size+'//asin_id.dat', 'data/'+folder+'/'+data_size+'//asin_v.dat'],
                      ['data/'+folder+'/'+data_size+'//invoice_id.dat', 'data/'+folder+'/'+data_size+'//invoice_v.dat'],
                      ['data/'+folder+'/'+data_size+'//OrderId_id.dat', 'data/'+folder+'/'+data_size+'//OrderId_v.dat']
                      ]
    attribute_pairs = [['invoice','OrderId']]
    A_D_pair = [[[3],['invoice','asin']]]
    rdb_files = ['data/'+folder+'/'+data_size+'//table_asin_price_OrderId.dat', 'data/'+folder+'/'+data_size+'//table_invoice_asin.dat',
                 'data/'+folder+'/'+data_size+'//table_invoice_orderline.dat', 'data/'+folder+'/'+data_size+'//table_invoice_asin.dat',
                 'data/'+folder+'/'+data_size+'//table_orderline_asin.dat']

    joining_order = ['asin', 'price', 'orderline','invoice', 'OrderId']
    commom_indexes = ['invoice', 'OrderId']

    # print(join_twig(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs)))
    # print(join_by_result_no_cut(load_data(rdb_files)))  # only rdb
    # filter
    # print (naive_join_twig_rdb_filter(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order,commom_indexes))
    # unfilter
    # print(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair))
    print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order,commom_indexes))

    # filtered
    # print (join_twig_rdb_filtered(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))
    # unfiltered
    # print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order))
    print("--- %s total time  seconds ---" % (time.time() - start_time))
    return

def evaluation_naive_q1():
    start_time = time.time()
    folder = 'treebank_zf_20'
    data_size ='big5k'
    id_value_pairs = [['data/'+folder+'/'+data_size+'/NP_id.dat', 'data/'+folder+'/'+data_size+'/NP_v.dat'],
                      ['data/' + folder + '/' + data_size + '/NNP_id.dat',
                       'data/' + folder + '/' + data_size + '/NNP_v.dat'],
                      ['data/'+folder+'/' + data_size + '/VP_id.dat', 'data/'+folder+'/' + data_size + '/VP_v.dat'],
                      ['data/'+folder+'/' + data_size + '/S_id.dat', 'data/'+folder+'/' + data_size + '/S_v.dat']
                      ]
    # attribute_pairs = [['S','NP'], ['S','VP']]
    attribute_pairs = [['S','NNP']]
    A_D_pairs = []
    rdb_files = ['data/'+folder+'/' + data_size + '/table_S_VP_NN.dat', 'data/'+folder+'/'+data_size+'/table_NP_VP.dat',
                 'data/'+folder+'/' + data_size + '/table_NP_VP_NNP.dat','data/'+folder+'/'+data_size+'/table_S_NP_NNP.dat',
                 'data/'+folder+'/'+data_size+'/table_S_VP_VB.dat','data/'+folder+'/'+data_size+'/table_NNP_VB.dat',]
    # rdb_files = ['data/'+folder+'/'+data_size+'/table_NP_VP.dat','data/'+folder+'/' + data_size + '/table_S_VP_NN.dat']
    joining_order = ['NP', 'VP', 'S' , 'NNP', 'NN','VB']
    # joining_order = ['NP', 'VP', 'NN', 'S']

    # rdb_files = ['data/'+folder+'/' + data_size + '/table_S_VP_NN.dat', 'data/'+folder+'/'+data_size+'/table_NP_VP.dat',
    #              'data/'+folder+'/' + data_size + '/table_NP_VP_NNP.dat','data/'+folder+'/'+data_size+'/table_S_NP_NNP.dat']
    # joining_order = ['NP', 'VP', 'S' , 'NNP']


    # for i in range(20,21):
    #     fraction = i/20
    #     result_file = 'data/result/q1/resultnj.dat'
    #     print(naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pairs),load_data(rdb_files), joining_order, fraction, result_file))

    for i in range(20,21):
        fraction = i/20
        result_file = 'data/result/q1/resultvj.dat'
        print (
            join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs, A_D_pairs), load_data(rdb_files),
                          joining_order, fraction, result_file))

    # print(join_twig(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair)))
    # print(join_by_result_no_cut(load_data(rdb_files)))  # only rdb
    # filter
    # print (naive_join_twig_rdb_filter(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order,commom_indexes))
    # unfilter
    # print(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair))
    # print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order))

    # for a in twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair):
    #     print(a)

    # filtered
    # print (join_twig_rdb_filtered(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))
    # unfiltered
    # print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order))
    print("--- %s total time  seconds ---" % (time.time() - start_time))
    return

def evaluation_naive_q2():
    start_time = time.time()
    folder = 'treebank_zf_20'
    data_size ='big5k'
    id_value_pairs = [['data/'+folder+'/'+data_size+'/NP_id.dat', 'data/'+folder+'/'+data_size+'/NP_v.dat'],
                      ['data/' + folder + '/' + data_size + '/NNP_id.dat', 'data/' + folder + '/' + data_size + '/NNP_v.dat'],
                      ['data/'+folder+'/' + data_size + '/VP_id.dat', 'data/'+folder+'/' + data_size + '/VP_v.dat'],
                      ['data/' + folder + '/' + data_size + '/VB_id.dat', 'data/' + folder + '/' + data_size + '/VB_v.dat'],
                      ['data/'+folder+'/' + data_size + '/S_id.dat', 'data/'+folder+'/' + data_size + '/S_v.dat']
                      ]
    attribute_pairs = [['S','NP','NNP'], ['S','VP','VB']]
    A_D_pairs = []
    rdb_files = ['data/'+folder+'/' + data_size + '/table_S_VP_NN.dat', 'data/'+folder+'/'+data_size+'/table_NP_VP.dat',
                 'data/'+folder+'/' + data_size + '/table_NP_VP_NNP.dat','data/'+folder+'/'+data_size+'/table_S_NP_NNP.dat',
                 'data/'+folder+'/'+data_size+'/table_S_VP_VB.dat','data/'+folder+'/'+data_size+'/table_NNP_VB.dat',]

    joining_order = ['S' ,'NP', 'VP', 'NNP', 'VB']

    for i in range(20,21):
        fraction = i/20
        result_file = 'data/result/q2/resultnj.dat'
        print(naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pairs),load_data(rdb_files), joining_order, fraction, result_file))

    for i in range(20,21):
        fraction = i/20
        result_file = 'data/result/q2/resultvj.dat'
        print (
            join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs, A_D_pairs), load_data(rdb_files),
                          joining_order, fraction, result_file))

    # print(join_twig(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs)))
    # print(join_by_result_no_cut(load_data(rdb_files)))  # only rdb
    # filter
    # print (naive_join_twig_rdb_filter(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order,commom_indexes))
    # unfilter
    # print(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair))
    # print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order))

    # for a in twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair):
    #     print(a)

    # filtered
    # print (join_twig_rdb_filtered(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))
    # unfiltered
    # print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order))
    print("--- %s total time  seconds ---" % (time.time() - start_time))
    return


def evaluation_naive_q3():
    start_time = time.time()
    folder = 'treebank_zf_20'
    data_size ='big5k'
    id_value_pairs = [['data/'+folder+'/'+data_size+'/NP_id.dat', 'data/'+folder+'/'+data_size+'/NP_v.dat'],
                      ['data/' + folder + '/' + data_size + '/NNP_id.dat', 'data/' + folder + '/' + data_size + '/NNP_v.dat'],
                      ['data/'+folder+'/' + data_size + '/VP_id.dat', 'data/'+folder+'/' + data_size + '/VP_v.dat'],
                      ['data/' + folder + '/' + data_size + '/VB_id.dat', 'data/' + folder + '/' + data_size + '/VB_v.dat'],
                      ['data/'+folder+'/' + data_size + '/S_id.dat', 'data/'+folder+'/' + data_size + '/S_v.dat']
                      ]
    attribute_pairs = [['S', 'NP'], ['S', 'VP']]
    A_D_pairs = [[[5],['NP','NNP']],[[5],['VP','VB']]]
    rdb_files = ['data/'+folder+'/' + data_size + '/table_S_VP_NN.dat', 'data/'+folder+'/'+data_size+'/table_NP_VP.dat',
                 'data/'+folder+'/' + data_size + '/table_NP_VP_NNP.dat','data/'+folder+'/'+data_size+'/table_S_NP_NNP.dat',
                 'data/'+folder+'/'+data_size+'/table_S_VP_VB.dat','data/'+folder+'/'+data_size+'/table_NNP_VB.dat',]
    # rdb_files = ['data/'+folder+'/'+data_size+'/table_NP_VP.dat','data/'+folder+'/' + data_size + '/table_S_VP_NN.dat']
    joining_order = ['NP', 'VP', 'S' , 'NNP', 'VB','NN']
    # joining_order = ['NP', 'VP', 'NN', 'S']


    # for i in range(1,21):
    #     fraction = i/20
    #     result_file = 'data/result/q3/resultnj.dat'
    #     print(naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pairs),load_data(rdb_files), joining_order, fraction, result_file))

    for i in range(1,21):
        fraction = i/20
        result_file = 'data/result/q3/resultvj.dat'
        print (
            join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs, A_D_pairs), load_data(rdb_files),
                          joining_order, fraction, result_file))

    # print(join_twig(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs)))
    # print(join_by_result_no_cut(load_data(rdb_files)))  # only rdb

    # print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order))
    # unfilter
    # print(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair))
    # for a in  twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair):
    #     print (a)
    # print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order))

    # for a in twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair):
    #     print(a)

    # filtered
    # print (join_twig_rdb_filtered(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs),load_data(rdb_files), joining_order))
    # unfiltered
    # print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order))
    print("--- %s total time  seconds ---" % (time.time() - start_time))
    return


def evaluation_naive_q4():
    start_time = time.time()
    folder = 'xmark_nm'
    data_size ='big5k'
    xml_id_file ='xml'
    id_value_pairs = [['data/'+folder+'/'+xml_id_file+'/incategory_id.dat', 'data/'+folder+'/'+data_size+'/incategory_v.dat'],
                      ['data/' + folder + '/' + xml_id_file + '/item_id.dat', 'data/' + folder + '/' + data_size + '/item_v.dat'],
                      ['data/'+folder+'/' + xml_id_file + '/location_id.dat', 'data/'+folder+'/' + data_size + '/location_v.dat'],
                      ['data/' + folder + '/' + xml_id_file + '/mail_id.dat', 'data/' + folder + '/' + data_size + '/mail_v.dat'],
                      ['data/'+folder+'/' + xml_id_file + '/quantity_id.dat', 'data/'+folder+'/' + data_size + '/quantity_v.dat']
                      ]
    attribute_pairs = [['item', 'incategory'], ['item', 'quantity']]
    # attribute_pairs2 = [['S', 'NNP']]
    A_D_pairs = []
    rdb_files = ['data/'+folder+'/' + data_size + '/table_item_quantity_other.dat',
                 'data/'+folder+'/'+data_size+'/table_item_incategory_other.dat',
                 'data/'+folder+'/' + data_size + '/table_incategory_quantity_other.dat']
    joining_order = ['item', 'incategory', 'quantity', 'mail']

    # for i in range(1,21):
    #     fraction = i/20
    #     result_file = 'data/result/q4/resultnj.dat'
    #     print(naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pairs),load_data(rdb_files), joining_order, fraction, result_file))

    for i in range(1,21):
        fraction = i/20
        result_file = 'data/result/q4/resultvj.dat'
        print (
            join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs, A_D_pairs), load_data(rdb_files),
                          joining_order, fraction, result_file))


    # print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pairs),load_data(rdb_files), joining_order))

    # print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pairs),load_data(rdb_files), joining_order))
    print("--- %s total time  seconds ---" % (time.time() - start_time))
    return

def evaluation_naive_q5():
    start_time = time.time()
    folder = 'xmark_nm'
    data_size ='big5k'
    xml_id_file ='xml'
    id_value_pairs = [['data/'+folder+'/'+xml_id_file+'/incategory_id.dat', 'data/'+folder+'/'+data_size+'/incategory_v.dat'],
                      ['data/' + folder + '/' + xml_id_file + '/item_id.dat', 'data/' + folder + '/' + data_size + '/item_v.dat'],
                      ['data/'+folder+'/' + xml_id_file + '/location_id.dat', 'data/'+folder+'/' + data_size + '/location_v.dat'],
                      ['data/' + folder + '/' + xml_id_file + '/mail_id.dat', 'data/' + folder + '/' + data_size + '/mail_v.dat'],
                      ['data/'+folder+'/' + xml_id_file + '/quantity_id.dat', 'data/'+folder+'/' + data_size + '/quantity_v.dat']
                      ]
    attribute_pairs = []
    attribute_pairs = [['item', 'incategory'], ['item', 'quantity']]
    A_D_pairs = [[[2],['item', 'mail']]]
    rdb_files = ['data/'+folder+'/' + data_size + '/table_item_quantity_other.dat',
                 'data/'+folder+'/'+data_size+'/table_item_incategory_other.dat',
                 'data/'+folder+'/' + data_size + '/table_incategory_quantity_other.dat']
    joining_order = [ 'item', 'incategory', 'quantity', 'mail']

    # for i in range(1, 21):
    #     fraction = i / 20
    #     result_file = 'data/result/q5/resultnj.dat'
    #     print(naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs, A_D_pairs),
    #                               load_data(rdb_files), joining_order, fraction, result_file))

    for i in range(1,21):
        fraction = i/20
        result_file = 'data/result/q5/resultvj.dat'
        print (
            join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs, A_D_pairs), load_data(rdb_files),
                          joining_order, fraction, result_file))

    # fraction = 1
    # result_file = 'data/result/q5/resultnj.dat'
    # print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pairs),load_data(rdb_files), joining_order, fraction, result_file))

    # print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pairs),load_data(rdb_files), joining_order))
    print("--- %s total time  seconds ---" % (time.time() - start_time))
    return

def evaluation_naive_q6():
    start_time = time.time()
    folder = 'xmark_nm'
    data_size ='big5k'
    xml_id_file ='xml'
    id_value_pairs = [['data/'+folder+'/'+xml_id_file+'/incategory_id.dat', 'data/'+folder+'/'+data_size+'/incategory_v.dat'],
                      ['data/' + folder + '/' + xml_id_file + '/item_id.dat', 'data/' + folder + '/' + data_size + '/item_v.dat'],
                      ['data/'+folder+'/' + xml_id_file + '/location_id.dat', 'data/'+folder+'/' + data_size + '/location_v.dat'],
                      ['data/' + folder + '/' + xml_id_file + '/mail_id.dat', 'data/' + folder + '/' + data_size + '/mail_v.dat'],
                      ['data/'+folder+'/' + xml_id_file + '/quantity_id.dat', 'data/'+folder+'/' + data_size + '/quantity_v.dat']
                      ]
    attribute_pairs = []
    # attribute_pairs2 = [['S', 'NNP']]
    A_D_pairs = [[[2],['item', 'mail']]]
    rdb_files = ['data/'+folder+'/' + data_size + '/table_item_quantity_other.dat',
                 'data/'+folder+'/'+data_size+'/table_item_incategory_other.dat',
                 'data/'+folder+'/' + data_size + '/table_incategory_quantity_other.dat']
    joining_order = [ 'incategory', 'quantity', 'item', 'mail','location']

    # for i in range(1, 21):
    #     fraction = i / 20
    #     result_file = 'data/result/q6/resultnj.dat'
    #     print(naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs, A_D_pairs),
    #                               load_data(rdb_files), joining_order, fraction, result_file))

    for i in range(1,21):
        fraction = i/20
        result_file = 'data/result/q6/resultvj.dat'


        print (
            join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs, A_D_pairs), load_data(rdb_files),
                          joining_order, fraction, result_file))


    # print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pairs),load_data(rdb_files), joining_order))

    # print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pairs),load_data(rdb_files), joining_order))
    print("--- %s total time  seconds ---" % (time.time() - start_time))
    return

def evaluation_naive_q7():
    start_time = time.time()
    folder = 'unibench2'
    data_size ='big200k'
    id_value_pairs = []
    attribute_pairs = []
    A_D_pairs = []
    rdb_files = ['data/'+folder+'/'+data_size+'/table_asin_productid_orderid.dat', 'data/'+folder+'/'+data_size+'/table_productid_product_info.dat',
                 'data/'+folder+'/'+data_size+'/table_personid_lastname.dat']
    rdb_files2 = ['data/'+folder+'/'+data_size+'/table_orderid_personid.dat']
    joining_order = ['asin', 'orderId', 'personId', 'productId']


    for i in range(1, 21):
        fraction = i / 20
        result_file = 'data/result/q7/resultnj.dat'
        # for a in load_data(rdb_files)+load_data(rdb_files2):
        #     print(a[:10])
        print(naive_join_rdb(load_data(rdb_files2)+load_data(rdb_files), joining_order, fraction, result_file))

    for i in range(1,21):
        fraction = i/20
        result_file = 'data/result/q7/resultvj.dat'

        # for a in twig_tables(twig_node_pair(id_value_pairs), attribute_pairs, A_D_pairs):
        #     print(a[:10])

        print (join_twig_rdb([], load_data(rdb_files2)+load_data(rdb_files),
                          joining_order, fraction, result_file))

    # print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order,commom_indexes))
    # print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order))
    print("--- %s total time  seconds ---" % (time.time() - start_time))
    return


def evaluation_naive_q8():
    start_time = time.time()
    folder = 'unibench2'
    data_size ='big300k'
    id_value_pairs = []
    attribute_pairs = []
    A_D_pairs = []
    rdb_files = ['data/'+folder+'/'+data_size+'/table_asin_productid_orderid.dat', 'data/'+folder+'/'+data_size+'/table_productid_product_info.dat',
                 'data/'+folder+'/'+data_size+'/table_personid_lastname.dat']
    rdb_files2 = ['data/'+folder+'/'+data_size+'/table_orderid_personid_productid.dat']
    joining_order = ['asin', 'orderId', 'productId', 'product_info', 'personId', 'lastname']
    joining_order = ['asin', 'orderId', 'personId', 'productId']


    for i in range(1, 21):
        fraction = i / 20
        result_file = 'data/result/q8/resultnj.dat'
        # for a in load_data(rdb_files)+load_data(rdb_files2):
        #     print(a[:10])
        print(naive_join_rdb(load_data(rdb_files2)+load_data(rdb_files), joining_order, fraction, result_file))

    for i in range(1,21):
        fraction = i/20
        result_file = 'data/result/q8/resultvj.dat'

        # for a in twig_tables(twig_node_pair(id_value_pairs), attribute_pairs, A_D_pairs):
        #     print(a[:10])

        print (join_twig_rdb([], load_data(rdb_files2)+load_data(rdb_files),
                          joining_order, fraction, result_file))

    # print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order,commom_indexes))
    # print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order))
    print("--- %s total time  seconds ---" % (time.time() - start_time))
    return

def evaluation_naive_q9():
    start_time = time.time()
    folder = 'unibench2'
    data_size ='big200k'
    id_value_pairs = []
    attribute_pairs = []
    A_D_pairs = []
    rdb_files = ['data/'+folder+'/'+data_size+'/table_asin_productid_orderid.dat', 'data/'+folder+'/'+data_size+'/table_productid_product_info.dat',
                 'data/'+folder+'/'+data_size+'/table_personid_lastname.dat']
    rdb_files2 = ['data/' + folder + '/' + data_size + '/table_personid_productid_asin.dat']
    joining_order = ['asin', 'orderId', 'productId', 'product_info', 'personId', 'lastname']
    joining_order = ['asin', 'orderId', 'personId', 'productId']


    for i in range(1, 21):
        fraction = i / 20
        result_file = 'data/result/q9/resultnj.dat'
        # for a in load_data(rdb_files)+load_data(rdb_files2):
        #     print(a[:10])
        print(naive_join_rdb(load_data(rdb_files2)+load_data(rdb_files), joining_order, fraction, result_file))

    for i in range(1,21):
        fraction = i/20
        result_file = 'data/result/q9/resultvj.dat'

        # for a in twig_tables(twig_node_pair(id_value_pairs), attribute_pairs, A_D_pairs):
        #     print(a[:10])

        print (join_twig_rdb([], load_data(rdb_files2)+load_data(rdb_files),
                          joining_order, fraction, result_file))

    # print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order,commom_indexes))
    # print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order))
    print("--- %s total time  seconds ---" % (time.time() - start_time))
    return

def evaluation_naive_q10():
    start_time = time.time()
    folder = 'unibench'
    data_size ='big1m2m'
    id_value_pairs = [['data/'+folder+'/'+data_size+'/asin_id.dat', 'data/'+folder+'/'+data_size+'/asin_v.dat'],
                      ['data/'+folder+'/'+data_size+'/price_id.dat', 'data/'+folder+'/'+data_size+'/price_v.dat'],
                      ['data/'+folder+'/'+data_size+'/orderline_id.dat',
                       'data/'+folder+'/'+data_size+'/orderline_v.dat']]
    attribute_pairs = [['orderline', 'asin'], ['orderline', 'price']]
    A_D_pairs = []
    rdb_files = [ 'data/'+folder+'/'+data_size+'/table_orderid_personid_asin.dat', 'data/'+folder+'/'+data_size+'/table_asin_orderId.dat',
                 'data/'+folder+'/'+data_size+'/table_personid_lastname.dat']
    joining_order = ['orderline', 'asin', 'personId','orderId']



    # for i in range(20, 21):
    #     fraction = i / 20
    #     result_file = 'data/result/q10/resultnj.dat'
    #     print(naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs, A_D_pairs),
    #                               load_data(rdb_files), joining_order, fraction, result_file))

    for i in range(20,21):
        fraction = i/20
        result_file = 'data/result/q10/resultvj.dat'

        # for a in twig_tables(twig_node_pair(id_value_pairs), attribute_pairs, A_D_pairs):
        #     print(a[:10])

        print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs, A_D_pairs), load_data(rdb_files),
                          joining_order, fraction, result_file))

    # print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order,commom_indexes))
    # print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order))
    print("--- %s total time  seconds ---" % (time.time() - start_time))
    return

def evaluation_naive_q11():
    start_time = time.time()
    folder = 'unibench'
    data_size ='big1m2m'
    A_D_pair = []
    id_value_pairs = [['data/'+folder+'/'+data_size+'//asin_id.dat', 'data/'+folder+'/'+data_size+'//asin_v.dat'],
                      ['data/'+folder+'/'+data_size+'//price_id.dat', 'data/'+folder+'/'+data_size+'//price_v.dat'],
                      ['data/'+folder+'/'+data_size+'//orderline_id.dat','data/'+folder+'/'+data_size+'//orderline_v.dat'],
                      ['data/'+folder+'/'+data_size+'//invoice_id.dat', 'data/'+folder+'/'+data_size+'//invoice_v.dat'],
                      ['data/'+folder+'/'+data_size+'//OrderId_id.dat', 'data/'+folder+'/'+data_size+'//orderId_v.dat']
                      ]
    attribute_pairs = [['invoice','orderId'], ['invoice','orderline', 'asin'], ['invoice','orderline', 'price']]
    A_D_pairs = []
    rdb_files = [ 'data/'+folder+'/'+data_size+'/table_asin_orderId.dat','data/'+folder+'/'+data_size+'/table_orderid_personid_asin.dat',
                 'data/'+folder+'/'+data_size+'/table_personid_lastname.dat']
    joining_order = ['invoice', 'orderId', 'personId']

    for i in range(1, 21):
        fraction = i / 20
        result_file = 'data/result/q11/resultnj.dat'
        print(naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs, A_D_pairs),
                                  load_data(rdb_files), joining_order, fraction, result_file))

    for i in range(1,21):
        fraction = i/20
        result_file = 'data/result/q11/resultvj.dat'

        # for a in twig_tables(twig_node_pair(id_value_pairs), attribute_pairs, A_D_pairs):
        #     print(a[:10])

        print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs, A_D_pairs), load_data(rdb_files),
                          joining_order, fraction, result_file))

    # print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order,commom_indexes))
    # print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order))
    print("--- %s total time  seconds ---" % (time.time() - start_time))
    return

def evaluation_naive_q12():
    start_time = time.time()
    folder = 'unibench'
    data_size ='big1m2m'
    id_value_pairs = [['data/'+folder+'/'+data_size+'/asin_id.dat', 'data/'+folder+'/'+data_size+'/asin_v.dat'],
                      ['data/'+folder+'/'+data_size+'/invoice_id.dat', 'data/'+folder+'/'+data_size+'/invoice_v.dat'],
                      ['data/'+folder+'/'+data_size+'/OrderId_id.dat', 'data/'+folder+'/'+data_size+'/orderId_v.dat']
                      ]
    attribute_pairs = [['invoice', 'orderId']]
    A_D_pairs = [[[3], ['invoice', 'asin']]]
    rdb_files = ['data/' + folder + '/' + data_size + '/table_asin_orderId.dat',
                 'data/' + folder + '/' + data_size + '/table_orderid_personid_asin.dat',
                 'data/' + folder + '/' + data_size + '/table_personid_lastname.dat']

    joining_order = ['asin', 'price', 'orderline','invoice', 'orderId']

    for i in range(1, 21):
        fraction = i / 20
        result_file = 'data/result/q12/resultnj.dat'
        print(naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs, A_D_pairs),
                                  load_data(rdb_files), joining_order, fraction, result_file))

    for i in range(1,21):
        fraction = i/20
        result_file = 'data/result/q12/resultvj.dat'

        # for a in twig_tables(twig_node_pair(id_value_pairs), attribute_pairs, A_D_pairs):
        #     print(a[:10])

        print (join_twig_rdb([], load_data(rdb_files)+load_data(rdb_files2),
                          joining_order, fraction, result_file))

    # print (naive_join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order,commom_indexes))
    # print (join_twig_rdb(twig_tables(twig_node_pair(id_value_pairs), attribute_pairs,A_D_pair),load_data(rdb_files), joining_order))
    print("--- %s total time  seconds ---" % (time.time() - start_time))
    return

if __name__ == "__main__":
    evaluation_naive_q7()
    evaluation_naive_q8()
    evaluation_naive_q9()
    # evaluation_naive_q12()
    # evaluation_naive_q6()

    # print (deweyID_parent('1.1.5.2'))