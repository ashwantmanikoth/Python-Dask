import dask.dataframe as dd

tables_sizes = {}
table_division = {}


def set_table_size(df_string, partition_size):
    tables_sizes[df_string] = partition_size
    if not df_string in table_division:
        table_division[df_string] = {}

def get_table_size(df_string):
    return tables_sizes[df_string]

def good_to_set_index(df_string):
    if df_string in tables_sizes:
        if tables_sizes[df_string] > 3:
            return True
        return True

def set_table_division(df_string,df_col,division):
    table_division[df_string][df_col] = division


def get_table_division(df_string,df_col):
    if df_col in table_division[df_string]:
        return table_division[df_string][df_col]
    return None

def print_table_sizes():
    print(tables_sizes)
    
def print_table_division():
    print(table_division)


