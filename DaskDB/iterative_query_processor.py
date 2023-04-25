import re
from typing import Callable

import dask.dataframe as dd
from DaskDB.dask_learned_index import create_index_and_distribute_data, merge_tables, \
    is_good_to_create_db_index_on_column
from DaskDB.setup_configuration import get_hdfs_master_node_IP, get_hdfs_master_node_port


class IterativeQueryProcessor:
    column_mappings = {}

    def __init__(self, client, base_code_block, iterative_code_block, final_code_block, **dataframes):
        self.client = client
        self.dataframes = dataframes
        cte_name_match = re.search(r'(\w+)\s*=\s*\w+\.\w+\(.*\)\s*$', final_code_block, flags=re.MULTILINE)
        if cte_name_match:
            cte_name = cte_name_match.group(1)
        else:
            cte_name = None

        #self.create_function("base_query", base_code_block, [])
        #self.create_function("recursive_query", iterative_code_block.replace("data_ml", cte_name), [cte_name])
        #self.create_function("final_query", final_code_block.replace("data_ml", cte_name), [cte_name])

    def create_function(self, func_name, code_block, param_names):
        default_args = ', '.join([f'{key}=self.dataframes["{key}"]' for key in self.dataframes.keys()])
        if len(param_names) > 0:
            func_header = f'def {func_name}(self, ' + ', '.join(param_names) + ', ' + default_args + '):\n'
        else:
            func_header = f'def {func_name}(self, ' + default_args + '):\n'

        statements = code_block.splitlines(True)

        last_assignment_match = re.search(r'(\w+)\s*=\s*\w+\.\w+\(.*\)\s*$', code_block, flags=re.MULTILINE)

        if last_assignment_match:
            return_var = last_assignment_match.group(1)
            statements[-1] = statements[-1].replace(".compute()", "")
            statements.append('return ' + return_var)

        indented_code = '    '.join(statements)
        func_definition = func_header + '    ' + indented_code

        exec(func_definition, globals(), locals())
        setattr(self, func_name, locals()[func_name])

    def process_iterative_query(self, max_iterations=10):
        #base_query: Callable = getattr(self, "base_query")
        #recursive_query: Callable = getattr(self, "recursive_query")
        #final_query: Callable = getattr(self, "final_query")

        cte = self.base_query()
        iteration = 0
        while True:
            new_cte = self.recursive_query(cte)
            if len(new_cte) == 0 or iteration >= max_iterations:
                break

            cte = dd.concat([cte, new_cte])
            iteration += 1
        return self.final_query(cte)

    def add_columns_index(self, df, df_string):
        self.column_mappings[df_string] = df.columns

    def base_query(self):
        distances = self.dataframes["distances"]
        self.add_columns_index(distances, "distances")
        distances = distances[distances[self.column_mappings["distances"][0]] == 1]
        distances = distances.rename(columns={self.column_mappings["distances"][0]: "cte_src",
                                              self.column_mappings["distances"][1]: "cte_target",
                                              self.column_mappings["distances"][2]: "cte_distance"})
        self.add_columns_index(distances, "distances")
        distances["cte_lvl"] = 1
        distances = distances.loc[:, ["cte_src", "cte_target", "cte_distance", "cte_lvl"]]
        self.add_columns_index(distances, "distances")
        distances = distances

        return distances

    def recursive_query(self, cte_paths):
        distances = self.dataframes["distances"]
        self.add_columns_index(distances, "distances")
        self.add_columns_index(cte_paths, "cte_paths")
        cte_paths = cte_paths[cte_paths[self.column_mappings["cte_paths"][3]] < 8]
        cte_paths = cte_paths.rename(columns={self.column_mappings["cte_paths"][1]: "cte_target",
                                              self.column_mappings["cte_paths"][2]: "cte_distance",
                                              self.column_mappings["cte_paths"][3]: "cte_lvl"})
        self.add_columns_index(cte_paths, "cte_paths")
        cte_paths = cte_paths.loc[:, ["cte_target", "cte_distance", "cte_lvl"]]
        self.add_columns_index(cte_paths, "cte_paths")
        join_col_1_list = [self.column_mappings["cte_paths"][0]]
        join_col_2_list = [self.column_mappings["distances"][0]]
        merged_table_distances = merge_tables('cte_paths', cte_paths, join_col_1_list, 'distances', distances,
                                              join_col_2_list)
        merged_table_distances = self.client.persist(merged_table_distances)
        extract_col_1_list = [self.column_mappings["cte_paths"][1], self.column_mappings["cte_paths"][2]]
        extract_col_2_list = [self.column_mappings["distances"][0], self.column_mappings["distances"][1],
                              self.column_mappings["distances"][2]]
        extract_list = extract_col_1_list + extract_col_2_list
        merged_table_distances = merged_table_distances.loc[:, extract_list]
        self.add_columns_index(merged_table_distances, "merged_table_distances")
        merged_table_distances = merged_table_distances.rename(
            columns={self.column_mappings["merged_table_distances"][2]: "cte_src",
                     self.column_mappings["merged_table_distances"][3]: "cte_target"})
        self.add_columns_index(merged_table_distances, "merged_table_distances")
        merged_table_distances["cte_distance"] = merged_table_distances[
                                                     self.column_mappings["merged_table_distances"][0]] + \
                                                 merged_table_distances[
                                                     self.column_mappings["merged_table_distances"][4]]
        merged_table_distances["cte_lvl"] = merged_table_distances[
                                                self.column_mappings["merged_table_distances"][1]] + 1
        merged_table_distances = merged_table_distances.loc[:, ["cte_src", "cte_target", "cte_distance", "cte_lvl"]]
        self.add_columns_index(merged_table_distances, "merged_table_distances")
        merged_table_distances = merged_table_distances

        return merged_table_distances

    def final_query(self, cte_paths):
        self.add_columns_index(cte_paths, "cte_paths")
        cte_paths = cte_paths[cte_paths[self.column_mappings["cte_paths"][1]] == 5]
        cte_paths = cte_paths.nlargest(1, columns=["cte_distance"])
        return cte_paths
