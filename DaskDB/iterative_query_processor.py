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
        cte_name_match = re.search(r'(\w+)\s*=\s*\w+\s*\([^()]*\)\s*$', final_code_block, flags=re.MULTILINE)
        if cte_name_match:
            cte_name = cte_name_match.group(1)
        else:
            cte_name = None

        self.create_function("base_query", base_code_block, [])
        self.create_function("recursive_query", iterative_code_block.replace("data_ml", cte_name), [cte_name])
        self.create_function("final_query", final_code_block.replace("data_ml", cte_name), [cte_name])

    def create_function(self, func_name, code_block, param_names):
        default_args = ', '.join([f'{key}=self.dataframes["{key}"]' for key in self.dataframes.keys()])
        if len(param_names) > 0:
            func_header = f'def {func_name}(self, ' + ', '.join(param_names) + ', ' + default_args + '):\n'
        else:
            func_header = f'def {func_name}(self, ' + default_args + '):\n'

        statements = code_block.splitlines(True)

        last_assignment_match = re.search(r'(\w+)\s*=\s*\w+\s*\.\w+\s*\(\s*\)$', code_block, flags=re.MULTILINE)

        if last_assignment_match:
            return_var = last_assignment_match.group(1)
            statements.pop()
            statements.append('return ' + return_var)

        indented_code = '    '.join(statements)
        func_definition = func_header + '    ' + indented_code

        exec(func_definition, globals(), locals())
        setattr(self, func_name, locals()[func_name])

    def process_iterative_query(self, max_iterations=2):
        base_query: Callable = getattr(self, "base_query")
        recursive_query: Callable = getattr(self, "recursive_query")
        final_query: Callable = getattr(self, "final_query")

        cte_customer_tree = base_query(self)
        print(cte_customer_tree.dtypes)
        iteration = 0
        while True:
            new_cte_customer_tree = recursive_query(self, cte_customer_tree)
            if len(new_cte_customer_tree) == 0 or iteration >= max_iterations:
                break

            cte_customer_tree = dd.concat([cte_customer_tree, new_cte_customer_tree])
            iteration += 1
        return final_query(self, cte_customer_tree)

    def add_columns_index(self, df, df_string):
        self.column_mappings[df_string] = df.columns
