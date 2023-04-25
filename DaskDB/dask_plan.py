import dask.dataframe as dd
import pandas as pd
import re
import dask
from timeit import default_timer as timer
# import pyarrow
#from machine_learning_functions import log_regression,random_forest, k_means,lin_regress_fit,grid_search #sray
# from pmlb import fetch_data, classification_dataset_names #sray
from sklearn.model_selection import train_test_split

from DaskDB.iterative_query_processor import IterativeQueryProcessor
from DaskDB.table_information  import set_table_size, print_table_sizes, get_table_size,good_to_set_index,get_table_division,set_table_division
#from dask_learned_index import write_relation_to_hdfs_and_create_sparse_index, read_relation_from_hdfs, merge_tables, is_good_to_create_db_index_on_column
from DaskDB.dask_learned_index import create_index_and_distribute_data, merge_tables, is_good_to_create_db_index_on_column
from DaskDB.setup_configuration import get_hdfs_master_node_IP, get_hdfs_master_node_port

col_lineitem = None
col_customer = None
col_orders = None
col_nation = None
col_region = None
col_part = None
col_supplier = None
col_partsupp = None
col_countries = None
col_distances = None

partition_size = "32MB"

def set_col_name_info(col_names_lineitem, col_names_customer, col_names_orders, col_names_part,
                     col_names_supplier, col_names_partsupp, col_names_nation, col_names_region,
                      col_names_countries, col_names_distances):
    global col_lineitem
    col_lineitem = col_names_lineitem 
    global col_customer
    col_customer = col_names_customer
    global col_orders
    col_orders = col_names_orders 
    global col_nation
    col_nation = col_names_nation 
    global col_region
    col_region = col_names_region 
    global col_part
    col_part = col_names_part 
    global col_supplier
    col_supplier = col_names_supplier 
    global col_partsupp
    col_partsupp = col_names_partsupp
    global col_countries
    col_countries = col_names_countries
    global col_distances
    col_distances = col_names_distances
    
    
def get_column_name_from_relations(relName, colPos):
    if relName == 'lineitem':
        return col_lineitem[colPos]
    if relName == 'customer':
        return col_customer[colPos]
    if relName == 'orders':
        return col_orders[colPos]
    if relName == 'nation':
        return col_nation[colPos]
    if relName == 'region':
        return col_region[colPos]
    if relName == 'part':
        return col_part[colPos]
    if relName == 'supplier':
        return col_supplier[colPos]
    if relName == 'partsupp':
        return col_partsupp[colPos]
    if relName == 'countries':
        return col_countries[colPos]
    if relName == 'distances':
        return col_distances[colPos]
    return ""

    
class DaskPlan:

    column_mappings = {}
    index_count = {}
    set_indexes = {}
    raco_indexes = {}
    one_order_by = False
    one_limit = False
    scale_factor='1'
    tpch_list = ['lineitem','customer','orders','part','supplier','partsupp','nation','region', 'countries', 'distances']
    column_headers = """col_names_lineitem = ['l_orderkey','l_partkey','l_suppkey','l_linenumber','l_quantity','l_extendedprice','l_discount','l_tax','l_returnflag','l_linestatus','l_shipdate','l_commitdate','l_receiptdate','l_shipinstruct','l_shipmode', 'l_comment']
col_names_customer = ['c_custkey','c_name','c_address','c_nationkey','c_phone','c_acctbal','c_mktsegment','c_comment']
col_names_orders = ['o_orderkey','o_custkey','o_orderstatus','o_totalprice','o_orderdate','o_orderpriority','o_clerk','o_shippriority','o_comment']
col_names_part = ['p_partkey','p_name','p_mfgr','p_brand','p_type','p_size','p_container','p_retailprice','p_comment']
col_names_supplier = ['s_suppkey','s_name','s_address','s_nationkey','s_phone','s_acctball','s_comment']
col_names_partsupp = ['ps_partkey','ps_suppkey','ps_availqty','ps_supplycost','ps_comment']
col_names_nation = ['n_nationkey','n_name','n_regionkey','n_comment']
col_names_region = ['r_regionkey','r_name','r_comment']
col_names_countries = ['id', 'c_name']
col_names_distances = ['src', 'target', 'distance']\n"""

    linetiem_read = "lineitem = dd.read_csv('data/lineitem.csv',delimiter='|',names=col_names_lineitem, parse_dates=[10,11,12])\n"
    customer_read = "customer = dd.read_csv('data/customer.csv',delimiter='|',names=col_names_customer)\n"
    orders_read = "orders =dd.read_csv('data/orders.csv',delimiter='|',names=col_names_orders, parse_dates=[4])\n"
    part_read = "part = dd.read_csv('data/part.csv',delimiter='|',names=col_names_part)\n"
    supplier_read = "supplier = dd.read_csv('data/supplier.csv',delimiter='|',names=col_names_supplier)\n"
    partsupp_read = "partsupp = dd.read_csv('data/partsupp.csv',delimiter='|',names=col_names_partsupp)\n"
    nation_read = "nation = dd.read_csv('data/nation.csv',delimiter='|',names=col_names_nation)\n"
    region_read = "region = dd.read_csv('data/region.csv',delimiter='|',names=col_names_region)\n"
    countries_read = "countries = dd.read_csv('data/countries.csv',delimiter='|',names=col_names_countries)\n"
    distances_read = "distances = dd.read_csv('data/distances.csv',delimiter='|',names=col_names_distances)\n"
    #dask_ml = 'data_ml = dd.read_csv("data_ml/data_ml.csv")'
    init = column_headers + linetiem_read + customer_read  + orders_read + part_read + supplier_read + partsupp_read + nation_read + region_read + countries_read + distances_read
    exec(init)
	
    # sray skdas
    hdfs_node=get_hdfs_master_node_IP()
    hdfs_port=get_hdfs_master_node_port()
    storage_opts = "storage_options={'host': '"+hdfs_node+"', 'port': "+str(hdfs_port)+"}"
    #print (storage_opts)
    
    udf_dict = {}
    
    def __init__(self, client): # sray
        self.client = client 
        
    def only_use_columns(self,used_cols,scale_factor):
        self.one_limit=False
        self.one_order_by = False
        col_headers = ""
        part_read=""
        linetiem_read=""
        customer_read=""
        orders_read=""
        supplier_read=""
        partsupp_read=""
        region_read=""
        nation_read=""
        countries_read=""
        distances_read=""
        string = ""
        col_headers = ""

        for key,values in used_cols.items():
            string += "\n"
#             print (key)
#             print (values)
            if(key=='lineitem'):
                use_cols_lineitem = values
                l_parse_dates=""
                l_dates= []
                for i in values:
                    if 'date' in i:
                        l_dates.append(i)
                if l_dates:
                    l_parse_dates = ",parse_dates=l_dates"
                #lineitem = dd.read_csv('hdfs:///input/datasets_for_dask_DB/data_'+scale_factor+'/lineitem.csv',delimiter='|', blocksize=partition_size, storage_options={'host': self.hdfs_node, 'port': self.hdfs_port}, names=self.col_names_lineitem,usecols=use_cols_lineitem,parse_dates=l_dates)
                lineitem = dd.read_csv('/home/ashwanta75/datasets_for_dask_DB/data_'+scale_factor+'/lineitem.csv',delimiter='|', blocksize=partition_size,  names=self.col_names_lineitem,usecols=use_cols_lineitem,parse_dates=l_dates)

                set_table_size('lineitem',lineitem.npartitions)
                self.lineitem = create_index_and_distribute_data('lineitem',lineitem)
                
            elif key=='customer':
                use_cols_customer = values
                # customer = dd.read_csv('hdfs:///input/datasets_for_dask_DB/data_'+scale_factor+'/customer.csv',delimiter='|', blocksize=partition_size, storage_options={'host': self.hdfs_node, 'port': self.hdfs_port}, names=self.col_names_customer,usecols=use_cols_customer)
                customer = dd.read_csv('/home/ashwanta75/datasets_for_dask_DB/data_'+scale_factor+'/customer.csv',delimiter='|', blocksize=partition_size,  names=self.col_names_customer,usecols=use_cols_customer)

                set_table_size('customer',customer.npartitions)
                self.customer = create_index_and_distribute_data('customer',customer)
                
            elif key=='orders':
                use_cols_orders= values
                _parse_dates=""
                o_dates= []
                for i in values:
                    if 'date' in i:
                        o_dates.append(i)
                if o_dates:
                    _parse_dates = ",parse_dates=o_dates"
                # orders = dd.read_csv('hdfs:///input/datasets_for_dask_DB/data_'+scale_factor+'/orders.csv',delimiter='|', blocksize=partition_size, storage_options={'host': self.hdfs_node, 'port': self.hdfs_port}, names=self.col_names_orders,usecols=use_cols_orders, parse_dates=o_dates)
                orders = dd.read_csv('/home/ashwanta75/datasets_for_dask_DB/data_'+scale_factor+'/orders.csv',delimiter='|', blocksize=partition_size, names=self.col_names_orders,usecols=use_cols_orders, parse_dates=o_dates)

                set_table_size('orders',orders.npartitions)
                self.orders = create_index_and_distribute_data('orders',orders)

            elif key=='part':
                use_cols_part = values
                # part = dd.read_csv('hdfs:///input/datasets_for_dask_DB/data_'+scale_factor+'/part.csv',delimiter='|', blocksize=partition_size, storage_options={'host': self.hdfs_node, 'port': self.hdfs_port}, names=self.col_names_part, usecols=use_cols_part)
                part = dd.read_csv('/home/ashwanta75/datasets_for_dask_DB/data_'+scale_factor+'/part.csv',delimiter='|', blocksize=partition_size,  names=self.col_names_part, usecols=use_cols_part)
                set_table_size('part',part.npartitions)
                self.part = create_index_and_distribute_data('part',part)

            elif key=='supplier':
                use_cols_supplier = values
                # supplier = dd.read_csv('hdfs:///input/datasets_for_dask_DB/data_'+scale_factor+'/supplier.csv',delimiter='|', blocksize=partition_size, storage_options={'host': self.hdfs_node, 'port': self.hdfs_port}, names=self.col_names_supplier,usecols=use_cols_supplier)
                supplier = dd.read_csv('/home/ashwanta75/datasets_for_dask_DB/data_'+scale_factor+'/supplier.csv',delimiter='|', blocksize=partition_size, names=self.col_names_supplier,usecols=use_cols_supplier)
                set_table_size('supplier',supplier.npartitions)
                self.supplier = create_index_and_distribute_data('supplier',supplier)

            elif key=='partsupp':
                use_cols_partsupp = values
                #partsupp = dd.read_csv('hdfs:///input/datasets_for_dask_DB/data_'+scale_factor+'/partsupp.csv',delimiter='|', blocksize=partition_size, storage_options={'host': self.hdfs_node, 'port': self.hdfs_port}, names=self.col_names_partsupp,usecols=use_cols_partsupp)
                partsupp = dd.read_csv('/home/ashwanta75/datasets_for_dask_DB/data_'+scale_factor+'/partsupp.csv',delimiter='|', blocksize=partition_size,  names=self.col_names_partsupp,usecols=use_cols_partsupp)
                set_table_size('partsupp',partsupp.npartitions)
                self.partsupp = create_index_and_distribute_data('partsupp',partsupp)

            elif key=='nation':
                use_cols_nation = values
                #nation = dd.read_csv('hdfs:///input/datasets_for_dask_DB/data_'+scale_factor+'/nation.csv',delimiter='|', blocksize=partition_size, storage_options={'host': self.hdfs_node, 'port': self.hdfs_port}, names=self.col_names_nation,usecols=use_cols_nation)
                nation = dd.read_csv('/home/ashwanta75/datasets_for_dask_DB/data_'+scale_factor+'/nation.csv',delimiter='|', blocksize=partition_size, names=self.col_names_nation,usecols=use_cols_nation)
                set_table_size('nation',nation.npartitions)
                self.nation = create_index_and_distribute_data('nation',nation)

            elif key=='region':
                use_cols_region = values
                #region = dd.read_csv('hdfs:///input/datasets_for_dask_DB/data_'+scale_factor+'/region.csv',delimiter='|', blocksize=partition_size, storage_options={'host': self.hdfs_node, 'port': self.hdfs_port}, names=self.col_names_region,usecols=use_cols_region)
                region = dd.read_csv('/home/ashwanta75/datasets_for_dask_DB/data_'+scale_factor+'/region.csv',delimiter='|', blocksize=partition_size,  names=self.col_names_region,usecols=use_cols_region)
                set_table_size('region',region.npartitions)
                self.region = create_index_and_distribute_data('region',region)

            elif key=='countries':
                use_cols_countries = values
                #region = dd.read_csv('hdfs:///input/datasets_for_dask_DB/data_'+scale_factor+'/region.csv',delimiter='|', blocksize=partition_size, storage_options={'host': self.hdfs_node, 'port': self.hdfs_port}, names=self.col_names_region,usecols=use_cols_region)
                countries = dd.read_csv('/home/ashwanta75/datasets_for_dask_DB/data_'+scale_factor+'/countries.csv',delimiter='|', blocksize=partition_size,  names=self.col_names_countries,usecols=use_cols_countries)
                set_table_size('countries',countries.npartitions)
                self.countries = create_index_and_distribute_data('countries',countries)

            elif key == 'distances':
                use_cols_distances = values
                # region = dd.read_csv('hdfs:///input/datasets_for_dask_DB/data_'+scale_factor+'/region.csv',delimiter='|', blocksize=partition_size, storage_options={'host': self.hdfs_node, 'port': self.hdfs_port}, names=self.col_names_region,usecols=use_cols_region)
                distances = dd.read_csv('/home/ashwanta75/datasets_for_dask_DB/data_' + scale_factor + '/distances.csv',
                                     delimiter='|', blocksize=partition_size, names=self.col_names_distances,
                                     usecols=use_cols_distances)
                set_table_size('distances', distances.npartitions)
                self.distances = create_index_and_distribute_data('distances', distances)
               
        
    def create_filter_strings(self,data_table,task,offset):
        if 'left' not in task or 'right' not in task:
            if task['type'] == 'CONSTANT':
                return task['value']

        if task['left']['type'] == 'VARIABLE':
            left_arg = (data_table + '[' + 'self.column_mappings["' + data_table + '"][' 
            + str(task['left']['columnIdx']-offset) + ']]')
        elif task['left']['type'] == 'CONSTANT':
            left_arg = task['left']['value']
            if 'date' in task['left']['value']:
                left_arg = self.parse_date(task['left']['value'])
            elif 'interval' in task['left']['value']:
                left_arg = self.parse_interval(task['left']['value'])
            elif task['left']['valueType'] == 'STRING_TYPE':
                left_arg = '"' + task['left']['value'] + '"'
        else:
            left_arg = '('+ self.create_filter_strings(data_table,task['left'],offset) + ')'
        if task['right']['type'] == 'VARIABLE':
            right_arg = (data_table + '[' + 'self.column_mappings["' + data_table + '"][' 
            + str(task['right']['columnIdx']-offset) + ']]')
        elif task['right']['type'] == 'CONSTANT':
            right_arg = task['right']['value']
            if 'date' in task['right']['value']:
                right_arg = self.parse_date(task['right']['value'])
            elif 'interval' in task['right']['value']:
                right_arg = self.parse_interval(task['right']['value'])
            elif task['right']['valueType'] == 'STRING_TYPE':
                right_arg = '"' + task['right']['value'] + '"'
        else:
            right_arg = '('+ self.create_filter_strings(data_table,task['right'],offset) + ')'
        return left_arg + self.parse_filter_actions(task['type']) + right_arg 
    
    def parse_filter_actions(self,filter_type):
        if filter_type == 'GT':
            return '>'
        elif filter_type == 'EQ':
            return '=='
        elif filter_type == 'LTEQ':
            return '<='
        elif filter_type == 'GTEQ':
            return '>='
        elif filter_type == 'LT':
            return '<'
        elif filter_type == 'AND':
            return '&'
        elif filter_type == 'OR':
            return '|'
        elif filter_type == 'NOT':
            return '~'
        elif filter_type == 'MINUS':
            return '-'
        elif filter_type == 'TIMES':
            return '*'
        elif filter_type == 'PLUS':
            return '+'
        elif filter_type == 'DIVIDE':
            return '/'
        elif filter_type == 'BETWEEN':
            return 'BETWEEN'
    
    def parse_date(self,line):
        line = re.sub(r"date_", "", line)
        return 'pd.to_datetime("' + line +'")'
    
    def parse_interval(self,line):
        line = re.sub(r"interval\s", "", line)
        interval_list = line.split(" ")
        int(interval_list[0])
        if interval_list[1] == 'DAY':
            arg1 = 'days'
        elif interval_list[1] == 'MONTH':
            arg1 = 'months'
        elif interval_list[1] == 'YEAR':
            arg1 = 'years'
        return 'pd.DateOffset(' + arg1 + '=' + interval_list[0] + ')'
    
    def add_columns_index(self,df,df_string):
        self.column_mappings[df_string] = df.columns
    
    def add_dataframe_index_count(self,df_string,count,indexes):
        self.index_count[df_string] = count
        self.set_indexes[df_string] = indexes
        
    def get_dataframe_index(self,df_string):
        if df_string in self.set_indexes:
            return self.set_indexes[df_string]
        return []
        
    def get_dataframe_index_count(self,df_string):
        if df_string not in self.index_count:
            return 0
        return self.index_count[df_string]
    
    def parse_groupbys(self,task):
        grouby_string = ''
        self.add_dataframe_index_count(task['data_table'],len(task['grouby_fields']),task['grouby_fields'])
        for i in task['grouby_fields']:
            grouby_string += 'self.column_mappings["' + task['data_table'] + '"][' + str(i) + '],'
        grouby_string = grouby_string[:len(grouby_string)-1] + grouby_string[len(grouby_string):]
        grouby_string = 'gb' + '=' + task['data_table'] +'.groupby([' + grouby_string + '])\n'
        agg_string =''
        for i in task['arguments']:
            for j in range(len(i['aggOps'])):
                if i['aggOps'][j] == 'AVG':
                    i['aggOps'][j] = 'mean'
                i['aggOps'][j] = i['aggOps'][j].lower()
            if i == task['arguments'][0]:
                    agg_string += ( task['data_table'] + '=' + 'gb.agg({self.column_mappings["' + task['data_table'] 
                                   + '"][' + str(i['column']) + ']' + ':"' + i['aggOps'][0] + '"})\n' )
                    agg_string += task['data_table'] + '= self.client.persist(' + task['data_table'] + ')\n'
            else:
                agg_string +=('.join(gb.agg({self.column_mappings["' + task['data_table'] 
                               + '"][' + str(i['column']) + ']'  + ':"' + i['aggOps'][0] + '"})'
                               + '.rename(columns={' + 'self.column_mappings["' + task['data_table'] + '"][' 
                               + str(i['column']) + ']:' + 'self.column_mappings["' + task['data_table'] + '"][' 
                               + str(i['column']) + ']+' + '"_' + i['aggOps'][0] + '"' 
                               + '}))'
                             )
        return grouby_string + agg_string

    def parse_order_by(self,task):
        orderby = ''
        sort_column = ''
        acsending = ''
        sort_index = ''
        reset_index = ''
        indexed_colummns = self.get_dataframe_index(task['data_table'])
        updated_index = []
        for key in self.raco_indexes:
            for i in indexed_colummns:
                if i == self.raco_indexes[key][1]:
                    updated_index.append(key)
        sorting_on_index = False
    #     for i in task['SortColumn']:
    #         #if sorting on index
    #         if i not in updated_index:
    #             sorting_on_index = True
        if not sorting_on_index: 
            for i in range(len(task['SortColumn'])):
                #sort_column += 'self.column_mappings["' + task['data_table'] + '"][' + str(task['SortColumn'][i]) +']' + ','
                sort_column += '"' + task['SortColumn'][i]  + '",'
     # sray           acsending += str(task['ascending'][i]) + ','
            if sort_column:    
                sort_column = sort_column[:len(sort_column)-1] + sort_column[len(sort_column):] 
     # sray           acsending = acsending[:len(acsending)-1] + acsending[len(acsending):]
    
                orderby = (task['data_table'] + '=' + task['data_table'] + 
                                    '.map_partitions(lambda x: x.sort_values([' + sort_column
                 # sray:                   +'],ascending=[' + acsending + ']))\n')
                                     +']))\n')
        else:
            #reset_index = task['data_table']  + '=' + task['data_table'] + '.reset_index()\n'
            j = 0
            for i in task['SortColumn']:
                #print(raco_indexes[i][0])
                sort_column += 'self.raco_indexes[' + str(i) + '][' + str(0) +']' + ','
     # sray           acsending += str(task['ascending'][j]) + ','
                j+=1
            orderby = (task['data_table'] + '=' + task['data_table'] + 
                                    '.map_partitions(lambda x: x.sort_values([' + sort_column
             # sray                        +'],ascending=[' + acsending + ']))\n')
                                            +']))\n')
        return sort_column,acsending,task['SortColumn'],task['ascending']
    
    def parse_agg(self,task):
        agg_string = ''
        for i in task['arguments']:
            for j in range(len(i['aggOps'])):
                if i['aggOps'][j] == 'AVG':
                    i['aggOps'][j] = 'mean'
                i['aggOps'][j] = i['aggOps'][j].lower()
                agg_string += (task['data_table'] + '[self.column_mappings["' + task['data_table'] 
                                   + '"][' + str(i['column']) + ']].' + i['aggOps'][0] + '().compute()\n' )
        return  agg_string
    
    def convert_to_dask_code(self,dask_plan):
        if len(dask_plan) > 1:
            base_code_block, _ = self.convert_plan(dask_plan[0])
            iterative_code_block, _ = self.convert_plan(dask_plan[1])
            final_query_block, _ = self.convert_plan(dask_plan[2])

            dataframes = {
                "lineitem": self.lineitem,
                "customer": self.customer,
                "orders": self.orders,
                "part": self.part,
                "partsupp": self.partsupp,
                "nation": self.nation,
                "region": self.region,
                "supplier": self.supplier,
                "countries": self.countries,
                "distances": self.distances
            }

            iterative_query_processor = IterativeQueryProcessor(
                self.client, base_code_block, iterative_code_block, final_query_block, **dataframes
            )

            result = iterative_query_processor.process_iterative_query()
            return result.compute()
        else:
            code_block, table = self.convert_plan(dask_plan[0])
            init_meth = """
lineitem = self.lineitem
customer = self.customer
orders = self.orders
part = self.part
partsupp = self.partsupp
nation = self.nation
region = self.region
supplier = self.supplier
countries = self.countries
distances = self.distances
            """
            exec(init_meth)
            exec(code_block)
            return vars()[table]

    def convert_plan(self, dask_plan):
        code_to_execute = ""
        orderby_limit = "" 
        mt_count = 0
        is_compute_invoked = False
        is_python_udf_invoked = False

        for kreturn_dask_codeey, value in sorted(dask_plan['operators'].items()):
            for task in value:
                if task['operation'] == 'read_csv':
                    #code_to_execute += task['table_name'] + ' = dd.read_csv("' + task['table_name'] + '.csv")\n'
#                     print(task['table_name'],'dask_ml' in task['table_name'])
                    if task['table_name'] not in self.tpch_list:
                         code_to_execute += 'self.add_columns_index('+ 'data_ml' + ',"' + task['table_name'] + '")\n'
                    else:
                        code_to_execute += 'self.add_columns_index('+ task['table_name'] + ',"' + task['table_name'] + '")\n'
                elif  task['operation'] == 'Filter':
                    code_to_execute += (task['data_table'] + '=' + task['data_table'] 
                                        + '[' + self.create_filter_strings(task['data_table'],task,0) + ']\n')
                elif task['operation'] == 'Apply':
                    code_to_execute += (task['data_table'] + '=' + task['data_table'] 
                                        + '.loc[:,["' + '","'.join(task['output_columns']) + '"]]\n')
                    code_to_execute += 'self.add_columns_index('+ task['data_table'] + ',"' + task['data_table'] + '")\n'
    
                elif task['operation'] == 'HashJoin':
                    if len(task['join_col_1']) == 1 and len(task['join_col_2']) == 1:
                        code_to_execute += 'join_col_1_list = [self.column_mappings["' + task['data_table1'] + '"][' + str(task['join_col_1'][0]) +']]\n'
                        code_to_execute += 'join_col_2_list = [self.column_mappings["' + task['data_table2'] + '"][' + str(task['join_col_2'][0]) +']]\n'
                    else:
                        join_col_1 = ''
                        for i in range(len(task['join_col_1'])):
                            join_col_1 += 'self.column_mappings["' + task['data_table1'] + '"][' + str(task['join_col_1'][i]) +']' + ','
                        join_col_1 = '[' + join_col_1[:len(join_col_1)-1] + join_col_1[len(join_col_1):] + ']'
                        
                        join_col_2 = ''
                        for i in range(len(task['join_col_2'])):
                            join_col_2 += 'self.column_mappings["' + task['data_table2'] + '"][' + str(task['join_col_2'][i]) +']' + ','
                        join_col_2 = '[' + join_col_2[:len(join_col_2)-1] + join_col_2[len(join_col_2):] + ']'
                        
                        code_to_execute += 'join_col_1_list = ' + join_col_1 + '\n'
                        code_to_execute += 'join_col_2_list = ' + join_col_2 + '\n'
                        
                    code_to_execute += ( task['data_table'] + '=' + 'merge_tables(' + "'" + task['data_table1'] +"',"+ task['data_table1'] + ","
                                        + 'join_col_1_list' + ",'" + task['data_table2'] +"',"+ task['data_table2'] + ',' + 'join_col_2_list' + ')\n' )    

                    code_to_execute += task['data_table'] + '= self.client.persist(' + task['data_table'] + ')\n'
                    extract_from_1 = False
                    if len(task['select_col_1']) == 1:
                        extract_from_1 = True
                        code_to_execute += 'extract_col_1_list = [self.column_mappings["' + task['data_table1'] + '"][' + str(task['select_col_1'][0]) +']]\n'
                    elif len(task['select_col_1']) > 1:
                        extract_from_1 = True
                        extract_col_1 = ''
                        for i in range(len(task['select_col_1'])):
                            extract_col_1 += 'self.column_mappings["' + task['data_table1'] + '"][' + str(task['select_col_1'][i]) +']' + ','
                        extract_col_1 = '[' + extract_col_1[:len(extract_col_1)-1] + extract_col_1[len(extract_col_1):] + ']\n'
                        code_to_execute += 'extract_col_1_list = ' + extract_col_1
                        
                    extract_from_2 = False    
                    if len(task['select_col_2']) == 1:
                        extract_from_2 = True
                        code_to_execute += 'extract_col_2_list = [self.column_mappings["' + task['data_table2'] + '"][' + str(task['select_col_2'][0]) +']]\n'
                    elif len(task['select_col_2']) > 1:
                        extract_from_2 = True
                        extract_col_2 = ''
                        for i in range(len(task['select_col_2'])):
                            extract_col_2 += 'self.column_mappings["' + task['data_table2'] + '"][' + str(task['select_col_2'][i]) +']' + ','
                        extract_col_2 = '[' + extract_col_2[:len(extract_col_2)-1] + extract_col_2[len(extract_col_2):] + ']\n'
                        code_to_execute += 'extract_col_2_list = ' + extract_col_2
                
                    if extract_from_1 and extract_from_2:
                        code_to_execute += 'extract_list = extract_col_1_list + extract_col_2_list\n'
                    elif extract_from_1:
                        code_to_execute += 'extract_list = extract_col_1_list\n'
                    elif extract_from_2:
                        code_to_execute += 'extract_list = extract_col_2_list\n'  
                        
                    if extract_from_1 or extract_from_2:
                        code_to_execute += (task['data_table'] + '=' +  task['data_table']
                                        + '.loc[:,extract_list]\n')
                    
                    code_to_execute += 'self.add_columns_index('+ task['data_table'] + ',"' + task['data_table'] + '")\n'
                elif task['operation'] == 'StoreTempResult':
                    code_to_execute += 'temp_storage' + '=' + task['data_table']
                elif task['operation'] == 'Column_Mapping':
                    self.raco_indexes = {}
                    dict_string = ''
                    col_to_keep = []
                    #col_that_are_index = get_dataframe_index(task['data_table'])
                    col_that_are_index = []
                    for i in range(len(task['rename_columns'])):
                        self.raco_indexes[i] = (task['rename_columns'][i][1],task['rename_columns'][i][0])
                    offset = len(col_that_are_index)
                    offset_new_col = len(col_that_are_index)
                    before = len(task['rename_columns'])
                    task['rename_columns_small'] = []
                    task_rename_columns = task['rename_columns']
                    for i in col_that_are_index:
                        task_rename_columns = [(key,value) for key,value in task_rename_columns if key != i]
                        task['rename_columns_small'] = [(key,value) for key, value in task_rename_columns if key < i]
                    for key,value in task['rename_columns_small']:
                        task_rename_columns.remove((key, value))
    
                    after = len(task_rename_columns) + len(task['rename_columns_small'])
                    offset = (after + offset) - before
                    for key,value in task_rename_columns:
                        if (key-offset_new_col) >= 0:
                            dict_string +='self.column_mappings["' + task['data_table'] + '"][' + str(key-offset_new_col) + ']' + ':"' + value + '",' 
                            col_to_keep.append(value)
    
                    for key,value in task['rename_columns_small']:
                            dict_string +='self.column_mappings["' + task['data_table'] + '"][' + str(key) + ']' + ':"' + value + '",' 
                            col_to_keep.append(value)
                    dict_string = dict_string[:len(dict_string)-1] + dict_string[len(dict_string):]
    
                    code_to_execute += (task['data_table'] + '=' + task['data_table'] 
                                        + '.rename(columns={'+  dict_string 
                                        + '})\n')
    
                    code_to_execute += 'self.add_columns_index('+ task['data_table'] + ',"' + task['data_table'] + '")\n'
                    for key in task['new_columns']:
                        code_to_execute += (task['data_table'] + '["' + key +  '"]=' 
                                        + self.create_filter_strings(task['data_table'],task['new_columns'][key],offset_new_col) + '\n')
                        col_to_keep.append(key)
                    code_to_execute += (task['data_table'] + '=' + task['data_table'] 
                                        + '.loc[:,["' + '","'.join(col_to_keep) 
                                        + '"]]\n')
                    code_to_execute += 'self.add_columns_index('+ task['data_table'] + ',"' + task['data_table'] + '")\n'
                elif task['operation'] == 'MyriaDupElim':
                    code_to_execute += ('removed_dup' + '=' + task['data_table'] + '[' + task['data_table'] 
                                        +'.columns[0]]' + '.unique()\n')
                elif task['operation'] == 'Groupby':
                    if task['grouby_fields']:
                        code_to_execute += self.parse_groupbys(task) + '\n'
                        code_to_execute += task['data_table'] + '=' + task['data_table'] + '.reset_index()\n'
                        code_to_execute += 'self.add_columns_index('+ task['data_table'] + ',"' + task['data_table'] + '")\n'
                    else:
                        code_to_execute +=  'df_agg=' + task['data_table']+'[self.column_mappings["' + task['data_table'] + '"][' + str(task['arguments'][0]['column']) + ']].'+task['arguments'][0]['aggOps'][0].lower() +'().compute()\n'
                        self.is_scalar = True
#                         print(code_to_execute)
                        exec(code_to_execute)
                        return vars()['df_agg']
                elif task['operation'] == 'Orderby':
                    if not self.one_order_by:
                        orderby_limit = self.parse_order_by(task)
                        #orderby = result
                        self.one_order_by=True
                elif task['operation'] == 'Limit':
                    if not self.one_limit: 
                        if orderby_limit:
                            #sray: code_to_execute +=task['data_table'] + '=' + task['data_table'] + '.nlargest(' + str(task['num_of_rows']) + ",columns=["+orderby_limit[0]+'],ascending=['+orderby_limit[1]+"]).compute()"
                            if orderby_limit[3][0] == False:
                                code_to_execute +=task['data_table'] + '=' + task['data_table'] + '.nlargest(' + str(task['num_of_rows']) + ",columns=["+orderby_limit[0]+'])'
                            else:
                                code_to_execute +=task['data_table'] + '=' + task['data_table'] + '.nsmallest(' + str(task['num_of_rows']) + ",columns=["+orderby_limit[0]+'])'
                        else:
                            code_to_execute +=  task['data_table'] + '=' + task['data_table'] + '.head(' + str(task['num_of_rows']) + ')\n'
                        self.one_limit=True
                        is_compute_invoked = True
                elif task['operation'] == 'PytyhonUDF':                    
                    param_list = []
                    udf_name = task['function_name']
                    table_name = task['data_table'] 
                    is_python_udf_invoked = True
                    p = task['parameters']
                    for d in p:
                        param_list.append(d['columnIdx'])     
                    
#                     code_to_execute += self.get_udf_string(task)
#                     code_to_execute += 'udf_name = ' + "'" + task['function_name'] + "'\n"
#                     code_to_execute += task['data_table'] + '=self.call_udf_func(udf_name,'+ task['data_table']+',col1,col2)\n'          
                    #code_to_execute += task['data_table'] + '=' + task['function_name'] + '(' + params + ')\n'
        #code_to_execute += '\nprint (' + task['data_table'] + ')'
                elif task['operation'] == 'final_output':
                    if not is_compute_invoked and not is_python_udf_invoked:
                        code_to_execute += task['data_table'] + '=' + task['data_table'] + ".compute()\n"
        if is_python_udf_invoked:
            #code_to_execute += self.get_udf_string(python_udf, table_name, param_list, is_compute_invoked)
            code_to_execute += 'udf_name = ' + "'" + udf_name  + "'\n"
            code_to_execute += table_name + '=self.call_udf_func(udf_name,'+ table_name+',' + str(param_list)+',is_compute_invoked)\n'          

#         print("TO EXECUTE:\n"+ code_to_execute)
       # print self.column_mappings
        #exec(code_to_execute)
        #return vars()[task['data_table']]
        return code_to_execute, task['data_table']
    
    def call_udf_func(self, udf_name, ddf, param_pos_list, is_compute_invoked):
#         if udf_name == 'LinearRegression':
#             ddf = ddf.compute()
#             x = np.array(ddf[col1]).reshape(-1,1)
#             y = ddf[col2]
#             m = LinearRegression().fit(x,y)
#             return m

        if not is_compute_invoked:
            ddf = ddf.compute()
            
        func, param_count_list = self.get_udf(udf_name)
        num_params = len(param_count_list)
        ddf = ddf.iloc[:,param_pos_list]
        param_list = []
        start = 0
        for val in param_count_list:
            end = start + val
            df = ddf.iloc[:,start:end]
            param_list.append(df)
            start = val
        params = tuple(param_list)
        m = func(*params)
        return m
        
    
#     def get_udf_string(self, python_udf, table_name, param_list, is_compute_invoked):
#         
# #         python_udf = task['function_name']
# #         table_name = task['data_table']
#         if python_udf == 'LinearRegression':
# #             param_list = task['parameters']
# #             first_col_pos = param_list[0]['columnIdx']
# #             second_col_pos = param_list[1]['columnIdx']
#             if not is_compute_invoked:
#                 string = table_name +'='+table_name + '.compute()\n'
#             else:
#                 string=""
#             string += 'x = np.array('+table_name+'[self.column_mappings["'+table_name+'"]['+str(first_col_pos)+']]).reshape(-1,1)\n'
#             string += 'y = '+table_name+'[self.column_mappings["'+table_name+'"]['+str(second_col_pos)+']]\n'
#             #string += 'y = y[self.column_mappings["'+table_name+'"]['+str(second_col_pos)+']]\n'
#             string += table_name + '=' + python_udf + '().fit(x,y)\n'
#             return string
        
        
    def register_udf(self,func, param_count_list):
        d={}
        d['func'] = func
        d['param_count_list'] = param_count_list
        self.udf_dict[func.__name__] = d
        
    def unregister_all_udf(self):
        self.udf_dict.clear()    
        
    def get_udf(self, udf_name):
        try:
            d = self.udf_dict[udf_name]
            return d['func'], d['param_count_list']
        except KeyError:    
            raise ValueError("UDF " + udf_name + "is not registered")       
        
    def get_udf_list(self):
        return self.udf_list
        
