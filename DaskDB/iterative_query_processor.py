import re
from typing import Callable

import dask.dataframe as dd
from DaskDB.dask_learned_index import create_index_and_distribute_data, merge_tables, \
    is_good_to_create_db_index_on_column


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

        self.create_function("base_query", base_code_block, [])
        self.create_function("recursive_query", iterative_code_block.replace("data_ml", cte_name), [cte_name])
        self.create_function("final_query", final_code_block.replace("data_ml", cte_name), [cte_name])

    def create_function(self, func_name, code_block, param_names):
        if len(param_names) > 0:
            func_header = f'def {func_name}(self, ' + ', '.join(param_names) + '):\n'
        else:
            func_header = f'def {func_name}(self):\n'

        statements = code_block.splitlines(True)

        for key in self.dataframes.keys():
            statements.insert(0, key + ' = ' + "self.dataframes['" + key + "']\n")

        last_assignment_match = re.search(r'(\w+)\s*=\s*.+\s*$', statements[-1], flags=re.MULTILINE | re.DOTALL)

        if last_assignment_match:
            return_var = last_assignment_match.group(1)
            statements[-1] = statements[-1].replace(".compute()", "")
            statements.append('\n')
            statements.append('return ' + return_var)

        indented_code = '    '.join(statements)
        func_definition = func_header + '    ' + indented_code

        print(func_definition)

        exec(func_definition, globals(), locals())
        setattr(self, func_name, locals()[func_name])

    def process_iterative_query(self, max_iterations=10):
        base_query: Callable = getattr(self, "base_query")
        recursive_query: Callable = getattr(self, "recursive_query")
        final_query: Callable = getattr(self, "final_query")

        cte = base_query(self)
        iteration = 0
        while True:
            result = recursive_query(self, cte)
            temp_cte = dd.concat([cte, result]).drop_duplicates()
            if self.is_similar(cte, temp_cte) or iteration >= max_iterations:
                break

            cte = temp_cte
            iteration += 1
            print(f'Iteration {iteration} completed.')
        return final_query(self, cte)

    @staticmethod
    def is_similar(df1, df2):
        return len(df1) == len(df2)

    def add_columns_index(self, df, df_string):
        self.column_mappings[df_string] = df.columns
