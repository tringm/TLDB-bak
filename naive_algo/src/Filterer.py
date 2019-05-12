from naive_algo.src import Loader
import timeit
import copy
# import pandas as pd


def binary_search_entries(entries, value, dimension):
    """
    return entries which coordinate at d equal to value
    :param entries:
    :param value:
    :param dimension:
    :return:
    """
    idx = 0

    while entries[idx].coordinates[dimension] < value:
        idx += 1

    if entries[idx].coordinates[dimension] > value:
        return None

    results = []
    while entries[idx].coordinates[dimension] == value:
        results.append(entries[idx])
        idx += 1

    return results


class Filterer():
    def __init__(self, loader: Loader):
        self.loader = loader

    def join_rdb_naive(self):
        start_join_rdb_naive = timeit.default_timer()
        join_results = []
        tables = sorted(list(self.loader.all_tables.keys()), key=lambda tbl: len(tbl), reverse=True)

        for tbl in tables:
            print(f"Joining table {tbl}")
            tbl_elements = tbl.split('_')
            tbl_entries = self.loader.all_tables[tbl]
            if not join_results:
                for tbl_e in tbl_entries:
                    join_results.append(dict(zip(tbl_elements, tbl_e.coordinates)))
            else:
                updated_join_results = []
                commons_e = list(set(tbl_elements).intersection(set(list(join_results[0].keys()))))

                # TODO: No commons_e
                if commons_e:
                    print('Common E: ', commons_e[0])
                # print(hash_join([join_rslt[commons_e[0]] for join_rslt in join_results],
                #                 [tbl_e.coordinates[tbl_elements.index(commons_e[0])] for tbl_e in tbl_entries]))

                for join_rslt in join_results:
                    for tbl_e in tbl_entries:
                        interx = {e: join_rslt[e] for e in join_rslt}
                        all_e_ok = True
                        for d, e in enumerate(tbl_elements):
                            if e not in join_rslt:
                                interx[e] = tbl_e.coordinates[d]
                            else:
                                if interx[e] != tbl_e.coordinates[d]:
                                    all_e_ok = False
                                    break
                        if all_e_ok:
                            updated_join_results.append(interx)

                join_results = updated_join_results
                if not updated_join_results:
                    break
            print(f"Intermediate results after joining table {tbl}: {len(join_results)}")

        print('JOIN RDB NAIVE RESULTS: ', len(join_results))
        # for join_rslt in join_results:
        #     print(join_rslt)
        print('JOIN RDB NAIVE TIME ', timeit.default_timer() - start_join_rdb_naive)
        print('')
        return join_results

    # def join_rdb_pandas(self):
    #     # Convert to pd dataframe
    #     start_creating_df = timeit.default_timer()
    #     tables_df = {}
    #     for tbl in self.loader.all_tables:
    #         tbl_e = tbl.split('_')
    #         tbl_data = {}
    #         for d, e in enumerate(tbl_e):
    #             tbl_data[e] = [e.coordinates[d] for e in self.loader.all_tables[tbl]]
    #         tables_df[tbl] = pd.DataFrame(tbl_data)
    #
    #     # Start join
    #     start_join_rdb_pandas = timeit.default_timer()
    #
    #     tables = sorted(list(tables_df.keys()), key=lambda tbl: len(tbl), reverse=True)
    #     print('BUILD TABLES TOOK', timeit.default_timer() - start_creating_df)
    #
    #
    #     merged_df = tables_df[tables[0]]
    #     for idx in range(1, len(tables)):
    #         this_tbl = tables[idx]
    #         commons_e = list(set(this_tbl.split('_')).intersection(set(list(merged_df.columns))))
    #         # print('Common', commons_e)
    #         # print('THIS TABLE\n', tables_df[this_tbl].to_string)
    #         # print('Previous TABLE\n', merged_df.to_string)
    #         merged_df = merged_df.merge(tables_df[this_tbl], on=commons_e, how='inner')
    #         # print('Merged result\n', merged_df.to_string)
    #         # print('')
    #
    #     print('JOIN RDB PANDAS RESULTS: ', merged_df.shape[0])
    #     # print(merged_df.to_string)
    #     print('JOIN RDB PANDAS TIME ', timeit.default_timer() - start_join_rdb_pandas)
    #     print('')
    #     return merged_df

    def join_xml_naive(self, join_rdb_results):
        start_join_xml = timeit.default_timer()
        matches = []

        for e in join_rdb_results[0]:
            if not matches:
                for join_idx, join_rslt in enumerate(join_rdb_results):
                    v_satisfy_entries = binary_search_entries(self.loader.all_elements[e], join_rslt[e], 1)
                    if v_satisfy_entries is not None:
                        for entry in v_satisfy_entries:
                            matches.append({e: entry, 'join_rslt_idx': join_idx})
            else:
                updated_matches = []
                for match in matches:
                    v_satisfy_entries = binary_search_entries(self.loader.all_elements[e],
                                                              join_rdb_results[match['join_rslt_idx']][e], 1)
                    if v_satisfy_entries is not None:
                        for entry in v_satisfy_entries:
                            new_match = match.copy()
                            new_match[e] = entry
                            updated_matches.append(new_match)

                matches = updated_matches
            # print('Matches after ', e)
            # for match in matches:
            #     print('\t', str(match))

        # print('MATCHES SATISFY JOIN RDB:')
        # for match in matches:
        #     print('\t', str(match))

        if len(join_rdb_results[0].keys()) < len(self.loader.all_elements_name):
            updated_matches = []
            for e in self.loader.all_elements:
                if e not in join_rdb_results[0]:
                    for match in matches:
                        for entry in self.loader.all_elements[e]:
                            updated_match = copy.deepcopy(match)
                            updated_match[e] = entry
                            updated_matches.append(updated_match)

            matches = updated_matches

        # print('###')
        # print("Initial matches after joining xml")
        # for match in matches:
        #     print(match)

        for idx_p, e_p in enumerate(self.loader.all_elements_name):
            for idx_c in range(idx_p + 1, len(self.loader.all_elements_name)):
                updated_matches = []
                e_c = self.loader.all_elements_name[idx_c]
                rls = self.loader.relationship_matrix[idx_p, idx_c]
                if rls != 0:
                    for match in matches:
                        if match[e_p].coordinates[0].relationship_satisfied(match[e_c].coordinates[0], rls):
                            updated_matches.append(match)
                    matches = updated_matches

        print("Final matches")
        for match in matches:
            print(match)

        print('JOIN XML TIME: ', timeit.default_timer() - start_join_xml)

    # def join_xml_pandas(self, merged_rdb):
    #     start_build_df = timeit.default_timer()
    #     elements_df = {}
    #     merged_rdb_xml = merged_rdb
    #     for e in self.loader.all_elements:
    #         index, v = zip(*[(str(e.coordinates[0]), e.coordinates[1]) for e in self.loader.all_elements[e]])
    #         elements_df[e] = pd.DataFrame({e + '_index': index, e + '_value': v})
    #         merged_rdb_xml = merged_rdb_xml.merge(elements_df[e], how='inner', left_on=[e], right_on=[e + '_value'])
    #         merged_rdb_xml = merged_rdb_xml.drop(columns=[e + '_value'])
    #
    #         # print('MERGED AFTER ', e)
    #         # print(merged_rdb_xml.to_string)
    #
    #     print('Build table took: ', timeit.default_timer() - start_build_df)
    #
    #     def is_ancestor(str1, str2) -> bool:
    #         components1 = str1.split('.')
    #         components2 = str2.split('.')
    #         # id2 is shorter -> can't be descendant
    #         if len(components1) >= len(components2):
    #             return False
    #         # Compare element wise
    #         for i in range(len(components1)):
    #             if components1[i] != components2[i]:
    #                 return False
    #         return True
    #
    #     def is_parent(str1, str2) -> bool:
    #         components1 = str1.split('.')
    #         components2 = str2.split('.')
    #         if len(components2) != (len(components1) + 1):
    #             return False
    #         # Compare element wise
    #         for i in range(len(components1)):
    #             if components1[i] != components2[i]:
    #                 return False
    #         return True
    #
    #     start_check_rsl = timeit.default_timer()
    #     # print(self.loader.relationship_matrix)
    #     for idx_a in range(len(self.loader.all_elements_name)):
    #         for idx_d in range(idx_a + 1, len(self.loader.all_elements_name)):
    #             if self.loader.relationship_matrix[idx_a, idx_d] != 0:
    #                 remove_rows = []
    #                 e_a_col = self.loader.all_elements_name[idx_a] + '_index'
    #                 e_d_col = self.loader.all_elements_name[idx_d] + '_index'
    #                 for idx, row in merged_rdb_xml.iterrows():
    #                     if self.loader.relationship_matrix[idx_a, idx_d] == 1:
    #                         if not is_parent(row[e_a_col], row[e_d_col]):
    #                             remove_rows.append(idx)
    #                     if self.loader.relationship_matrix[idx_a, idx_d] == 2:
    #                         if not is_ancestor(row[e_a_col], row[e_d_col]):
    #                             remove_rows.append(idx)
    #                 merged_rdb_xml = merged_rdb_xml.drop(remove_rows)
    #                 # print(e_a_col, e_d_col, 'results: ', merged_rdb_xml.to_string)
    #
    #     print('CHECK RLS TOOK: ', timeit.default_timer() - start_check_rsl)
    #     print('FINAL RESULT: ')
    #     print(merged_rdb_xml.to_string)

    def perform(self):

        start_naive = timeit.default_timer()
        join_results_naive = self.join_rdb_naive()
        if join_results_naive:
            self.join_xml_naive(join_results_naive)
        else:
            print('No result found after join_rdb')
        print('TOTAL TIME NAIVE: ', timeit.default_timer() - start_naive)
        #
        # print("###")
        # print("###")
        # print("###")

        # start_pandas = timeit.default_timer()
        # join_results_pandas = self.join_rdb_pandas()
        # self.join_xml_pandas(join_results_pandas)
        #
        # print('TOTAL TIME PANDAS: ', timeit.default_timer() - start_pandas)
