import numpy as np
import pandas as pd
import dask.dataframe as dd
import re
from timeit import default_timer as timer
#import DaskDB.distpartd as distpartd

#from memory_profiler import profile

#import pyarrow as pa
#import pyarrow.plasma as plasma
import time


from dask.distributed import Client
from numpy import piecewise
clientHadoop = Client('192.168.0.108:8786')
clientHadoop.restart()

# from dask.cache import Cache
# cache = Cache(2e9)
# cache.register()
# sray skdas
hdfs_node='192.168.0.108'
hdfs_port=9000
storage_opts = {'host': hdfs_node, 'port': hdfs_port}
print (storage_opts)
    
tables_sizes = {}
table_division = {}
col_names_lineitem = ['l_orderkey','l_partkey','l_suppkey','l_linenumber','l_quantity','l_extendedprice','l_discount','l_tax','l_returnflag','l_linestatus','l_shipdate','l_commitdate','l_receiptdate','l_shipinstruct','l_shipmode', 'l_comment']
col_names_orders = ['o_orderkey','o_custkey','o_orderstatus','o_totalprice','o_orderdate','o_orderpriority','o_clerk','o_shippriority','o_comment']
col_names_customer = ['c_custkey','c_name','c_address','c_nationkey','c_phone','c_acctbal','c_mktsegment','c_comment']     
col_names_nation = ['n_nationkey','n_name','n_regionkey','n_comment']
col_names_region = ['r_regionkey','r_name','r_comment']
col_names_part = ['p_partkey','p_name','p_mfgr','p_brand','p_type','p_size','p_container','p_retailprice','p_comment']
col_names_supplier = ['s_suppkey','s_name','s_address','s_nationkey','s_phone','s_acctball','s_comment']
col_names_partsupp = ['ps_partkey','ps_suppkey','ps_availqty','ps_supplycost','ps_comment']

primary_key = {
    'orders' : 'o_orderkey',
    'customer':'c_custkey',
    'nation' : 'n_nationkey',
    'region' : 'r_regionkey',
    'part' : 'p_partkey',
    'supplier' : 's_suppkey'
    }

foreign_key = {
    'lineitem' : [['l_partkey','part'], ['l_suppkey','supplier'], ['l_orderkey','orders']],
    'orders' : [['o_custkey','customer']],
    'customer' : [['c_nationkey','nation']],
    'nation' : [['n_regionkey','region']],
    'supplier' : [['s_nationkey','nation']],
    'partsupp' : [['ps_partkey','part'], ['ps_suppkey','supplier']]
    }

primary_indices = {}
clustered_indices = {}
non_clustered_indices = {}
temp_join_rel_name = 'JoinedRelation'

sorted_relations = {}
heaviside_func = {}

dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')

# FROM local path
#DATA_FILE_PATH = '/home1/grads/sdas6/dask-plinq/datasets_for_dask_DB/data_4/'
# DATA_FILE_PATH = '/home/dbuser/datasets/DaskDB/TPC-H/data_4/'

# from HDFS
# DATA_FILE_PATH = 'hdfs:///data__1/'
# DATA_FILE_PATH = 'hdfs:///data_4/'
DATA_FILE_PATH = 'hdfs:///input/datasets_for_dask_DB/data_1/'

lineitem = None
orders = None


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

def get_default_chunksize():
    import dask
    chunksize = dask.config.config['array']['chunk-size'] #Returns chunksize in string like '128MiB'
    return dask.utils.parse_bytes(chunksize)       
        

class DaskPlan:
    
    column_mappings = {}
    index_count = {}
    set_indexes = {}
    raco_indexes = {}
    one_order_by = False
    one_limit = False
    scale_factor='1'
    
    li_persist = False
    ord_persist = False


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
        
    def create_heaviside_function(self,relName, pdf):
        
        def create_heaviside(x):
            y = 0
            for i in range(len(pdf)):
                begin = pdf.iat[i,0]
                end = pdf.iat[i,1]
                partition = pdf.iat[i,2]
                y += np.heaviside((end - x)*(x - begin), 1) * partition
            return int(y)
        
        heaviside_func[relName] = create_heaviside 


    def add_row_to_primary_index(self,pdf,col, partition):
        begin = pdf.iat[0,col]
        end = pdf.iat[-1,col]
        df = pd.DataFrame([[begin,end,partition]], columns = ['Begin','End','Partition'])
        return df
    
    def create_all_index(self, relName, relDataFrame):
        primary_index = None
         
        primary_key_col_name = primary_key.get(relName)
        if primary_key_col_name and (primary_key_col_name in relDataFrame.columns):
            primary_key_col_pos = relDataFrame.columns.get_loc(primary_key_col_name)
         
        if not primary_key_col_name:
            print ('Primary Key not found for ' + relName)
 
        for i in range(relDataFrame.npartitions):
            rel_pd = relDataFrame.get_partition(i).compute()
            self.p.append({relName +'-' + str(i):rel_pd})
            if i is 0:
                if primary_key_col_name:
                    primary_index = self.add_row_to_primary_index(rel_pd,primary_key_col_pos,i)
            else:
                if primary_key_col_name:
                    primary_index = primary_index.append(self.add_row_to_primary_index(rel_pd,primary_key_col_pos,i))
        
        self.create_heaviside_function(relName, primary_index) 
        primary_indices[relName] = primary_index;
        
    def cull_empty_partitions(self,df):
        ll = list(df.map_partitions(len).compute())
        df_delayed = df.to_delayed()
        df_delayed_new = list()
        pempty = None
        for ix, n in enumerate(ll):
            if 0 == n:
                pempty = df.get_partition(ix)
            else:
                df_delayed_new.append(df_delayed[ix])
        if pempty is not None:
            df = dd.from_delayed(df_delayed_new, meta=pempty)
        return df    

    def get_relation(self,relName,merged_index,columns = None):
        sorted_rel = sorted_relations.get(relName)
        if sorted_rel is not None:
            return sorted_rel
        
        pandadf = None
        key_col_name = primary_key.get(relName)
        key_col_pos_in_merged_index = list(merged_index.columns).index(key_col_name);
        copy = merged_index
        futures = []
        heaviside = heaviside_func[relName]
        while True:
            if copy.npartitions is 0:
                break 
            first_partition = copy.get_partition(0).compute()
            key_value = first_partition.iat[0, key_col_pos_in_merged_index]
            prime_index = primary_indices.get(relName)
            
            part_no = heaviside(key_value)
            pandadf = self.p.get(relName + '-' + str(part_no))
            if columns is not None:
                pandadf = pandadf[columns]
            future = clientHadoop.scatter(pandadf)
            futures.append(future)
            
            end = prime_index.iat[part_no,1]    #part_no is equal to row_no             
            copy = copy[copy[key_col_name] > end]
            copy = self.cull_empty_partitions(copy)

        rel =  dd.from_delayed(futures, meta=pandadf)
        if key_col_name:
            rel = rel.set_index(key_col_name, drop = False)
        rel = clientHadoop.persist(rel)
        sorted_relations[relName] = rel
#        del pandadf       
        return rel    

#    @profile
    def read_relation_from_hdfs(self, relName):
        
        futures = []
        for i in range(get_table_size(relName)):
            df = self.p.get(relName + '-' + str(i))
            future = clientHadoop.scatter(df)
            futures.append(future)
        
        ddf = dd.from_delayed(futures, meta=df)        
        ddf = ddf.repartition(npartitions=get_table_size(relName)).persist()  # split
        clientHadoop.rebalance(ddf) # spread around all of your workers 
        return ddf
    

    def get_primary_and_foreign_relations(self, relName_1, relName_2):
         
        foreign_key_list_1 = foreign_key.get(relName_1)
        foreign_key_list_2 = foreign_key.get(relName_2)
             
        for foreign_key_entry in foreign_key_list_1:
            col,rel = foreign_key_entry
            if rel == relName_2 :
                return relName_2, primary_key.get(relName_2), relName_1, col
 
        for foreign_key_entry in foreign_key_list_2:
            col,rel = foreign_key_entry
            if rel == relName_1 :
                return relName_1, primary_key.get(relName_1), relName_2, col
             
        return None, None, None, None
#         raise ValueError("Relations cannot be joined. No (Primary Key, Foreign Key) pair present")    


#     @profile
    def merge_relations_on_single_columns(self, relName_1, reldf_1, rel_1_join_col_name, relName_2, reldf_2, rel_2_join_col_name):
        if (relName_1 is not temp_join_rel_name) and (relName_2 is not temp_join_rel_name):
            primary_rel, primary_col, foreign_rel, foreign_col = self.get_primary_and_foreign_relations(relName_1, relName_2)
            
            if primary_rel is relName_1:
                relName_1, relName_2 = relName_2, relName_1
                reldf_1, reldf_2 = reldf_2, reldf_1
                rel_1_join_col_name, rel_2_join_col_name = rel_2_join_col_name, rel_1_join_col_name

            sorted_rel = sorted_relations.get(relName_1)
            if sorted_rel is not None:
                reldf_1 = sorted_rel
            else:
                reldf_1 = reldf_1.set_index(rel_1_join_col_name)
                reldf_1 = clientHadoop.persist(reldf_1)
                sorted_relations[relName_1] = reldf_1
#             reldf_1 = clientHadoop.persist(reldf_1)
            
        primary_col_name = primary_key.get(relName_2)
        if (not primary_col_name) or (primary_col_name != rel_2_join_col_name):
            sorted_rel = sorted_relations.get(relName_2)
            if sorted_rel is not None:
                reldf_2 = sorted_rel
            else:
                reldf_2 = reldf_2.set_index(rel_2_join_col_name)
                reldf_2 = clientHadoop.persist(reldf_2)
                sorted_relations[relName_2] = reldf_2
#             reldf_2 = clientHadoop.persist(reldf_2)
            if relName_1 is not temp_join_rel_name:
                merged_relation = reldf_1.merge(reldf_2,left_index=True,right_index=True)      
            else:
                merged_relation = reldf_1.merge(reldf_2,left_on=rel_1_join_col_name,right_index=True)
               
        else: #use index here
            primary_df, foreign_df = reldf_2, reldf_1
            primary_col = reldf_2[[primary_col_name]]
            
            if relName_1 is not temp_join_rel_name:
#                 primary_col = primary_col.reset_index(drop = True) 
                merged_relation=primary_col.merge(foreign_df,left_on=primary_col_name,right_index=True)
            else:
                merged_relation = primary_col.set_index(primary_col_name)          
                merged_relation = clientHadoop.persist(merged_relation)
                #merged_relation=primary_col.merge(foreign_df,left_index=True, right_on=rel_1_join_col_name)
                merged_relation=foreign_df.merge(primary_col,left_on=rel_1_join_col_name, right_index=True)
            
            #merged_relation = merged_relation.set_index(primary_col_name, drop=False)
            #merged_relation = clientHadoop.persist(merged_relation)
            #print merged_relation.compute()
            
            primary_df = self.get_relation(relName_2,merged_relation, columns=reldf_2.columns)
            primary_df = clientHadoop.persist(primary_df)
            if relName_1 is not temp_join_rel_name:
                merged_relation=primary_df.merge(merged_relation,left_index=True,right_on=primary_col_name)
            else:
                merged_relation=merged_relation.merge(primary_df,left_on=rel_1_join_col_name,right_index=True)
            #merged_relation=primary_df.merge(merged_relation,left_on=primary_col_name,right_index=True)
            #merged_relation = merged_relation.rename(columns={primary_col_name + '_x':primary_col_name})
            merged_relation = merged_relation.drop([primary_col_name + '_x', primary_col_name + '_y'],axis=1)

        return merged_relation
    

    def merge_tables(self, relName_1, reldf_1, rel_1_join_col_list, relName_2, reldf_2, rel_2_join_col_list):
        
        #    rel1: can be a temporary relation or a relation from the schema
        #    rel2: always a relation schema from the database  
        if len(rel_1_join_col_list) != len(rel_2_join_col_list):
            raise ValueError("Relations cannot be joined. Count of join columns are not equal")
        elif len(rel_1_join_col_list) > 1:
            merged_relation = reldf_1.merge(reldf_2,left_on=rel_1_join_col_list,right_on=rel_2_join_col_list) 
        else:
            rel_1_join_col_name, = rel_1_join_col_list
            rel_2_join_col_name, = rel_2_join_col_list
            merged_relation = self.merge_relations_on_single_columns(relName_1, reldf_1, rel_1_join_col_name, relName_2, reldf_2, rel_2_join_col_name)    
        return merged_relation
   
    
    def test_code_learned_index(self, no_of_runs):
        
        import distpartd

        self.p = distpartd.PandasColumns('hdfs:///tempDir', str(hdfs_node), hdfs_port)

        for i in range(no_of_runs):
            start = timer()
            #DATA_FILE_PATH_2 = '/home1/grads/sdas6/eclipse-workspace/Dask-DB/pymydaskdb/data/'
            if i is 0:
            
                orders = dd.read_csv(DATA_FILE_PATH+'orders.csv',delimiter='|',storage_options=storage_opts, names=col_names_orders, usecols=['o_orderkey', 'o_custkey', 'o_orderdate'], parse_dates=['o_orderdate'], date_parser=dateparse)
                customer = dd.read_csv(DATA_FILE_PATH+'customer.csv',delimiter='|', storage_options=storage_opts, names=col_names_customer, usecols=['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_comment'], parse_dates=[], date_parser=dateparse)
                lineitem = dd.read_csv(DATA_FILE_PATH+'lineitem.csv',delimiter='|', storage_options=storage_opts, names=col_names_lineitem, usecols=['l_orderkey', 'l_suppkey', 'l_extendedprice', 'l_discount'], parse_dates=[])
#                 nation = dd.read_csv(DATA_FILE_PATH+'nation.csv',delimiter='|', storage_options=storage_opts, names=col_names_nation, usecols=['n_name', 'n_nationkey', 'n_regionkey'], parse_dates=[])
#                 supplier = dd.read_csv(DATA_FILE_PATH+'supplier.csv',delimiter='|', storage_options=storage_opts, names=col_names_supplier, usecols=['s_suppkey', 's_nationkey'], parse_dates=[])
#                 region = dd.read_csv(DATA_FILE_PATH+'region.csv',delimiter='|', storage_options=storage_opts, names=col_names_region, usecols=['r_regionkey', 'r_name'], parse_dates=[])
      
                set_table_size('customer',customer.npartitions)
                set_table_size('orders',orders.npartitions)
                set_table_size('lineitem',lineitem.npartitions)
#                 set_table_size('nation',nation.npartitions)
#                 set_table_size('supplier',supplier.npartitions)
#                 set_table_size('region',region.npartitions)
                
                self.create_all_index('customer',customer)
                self.create_all_index('orders',orders)
                self.create_all_index('lineitem',lineitem)
#                 self.create_all_index('nation',nation)
#                 self.create_all_index('supplier',supplier)
#                 self.create_all_index('region',region)

                self.customer = self.read_relation_from_hdfs('customer')
                self.orders = self.read_relation_from_hdfs('orders')
                self.lineitem = self.read_relation_from_hdfs('lineitem')
#                 self.supplier = self.read_relation_from_hdfs('supplier')
#                 self.nation = self.read_relation_from_hdfs('nation')
#                 self.region = self.read_relation_from_hdfs('region')

            customer = self.customer 
            orders = self.orders 
            lineitem = self.lineitem 
#             supplier = self.supplier 
#             nation = self.nation 
#             region = self.region 
                
    
            self.add_columns_index(orders,"orders")
            self.add_columns_index(customer,"customer")
            self.add_columns_index(lineitem,"lineitem")
#             self.add_columns_index(nation,"nation")
#             self.add_columns_index(supplier,"supplier")
#             self.add_columns_index(region,"region")
            
            merged_index = self.merge_tables('orders', orders, ['o_custkey'], 'customer', customer, ['c_custkey'])
            merged_index = clientHadoop.persist(merged_index)
            clientHadoop.cancel(orders)
            clientHadoop.cancel(customer)
            merged_index = self.merge_tables(temp_join_rel_name, merged_index, ['o_orderkey'], 'lineitem', lineitem, ['l_orderkey'])
            merged_index = clientHadoop.persist(merged_index)
#             merged_index = self.merge_tables(temp_join_rel_name, merged_index, ['l_suppkey','c_nationkey'], 'supplier', supplier, ['s_suppkey','s_nationkey'])
#             merged_index = self.merge_tables(temp_join_rel_name, merged_index, ['s_nationkey'], 'nation', nation, ['n_nationkey'])
#             merged_index = self.merge_tables(temp_join_rel_name, merged_index, ['n_regionkey'], 'region', region, ['r_regionkey'])
            
            #merged_index = merged_index.set_index('c_custkey', drop=False)
            print (merged_index.compute())
            
    #         print "Merged"
    #         print merged_index.compute()
    
            end = timer()
            
            print ("test_code_learned_index : Run " + str(i) + " : " + str(end - start))
            

    def test_code_normal_index(self):
        import distpartd
        
        start = timer() 
        
        orders = dd.read_csv(DATA_FILE_PATH+'orders.csv',delimiter='|',storage_options=storage_opts, names=col_names_orders, usecols=['o_orderkey', 'o_custkey', 'o_orderdate'], parse_dates=['o_orderdate'], date_parser=dateparse)
        customer = dd.read_csv(DATA_FILE_PATH+'customer.csv',delimiter='|', storage_options=storage_opts, names=col_names_customer, usecols=['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_comment'], parse_dates=[], date_parser=dateparse)
        lineitem = dd.read_csv(DATA_FILE_PATH+'lineitem.csv',delimiter='|', storage_options=storage_opts, names=col_names_lineitem, usecols=['l_orderkey', 'l_suppkey', 'l_extendedprice', 'l_discount'], parse_dates=[])
#         nation = dd.read_csv(DATA_FILE_PATH+'nation.csv',delimiter='|',names=col_names_nation, usecols=['n_name', 'n_nationkey', 'n_regionkey'], parse_dates=[])
#         supplier = dd.read_csv(DATA_FILE_PATH+'supplier.csv',delimiter='|',names=col_names_supplier, usecols=['s_suppkey', 's_nationkey'], parse_dates=[])
#         region = dd.read_csv(DATA_FILE_PATH+'region.csv',delimiter='|',names=col_names_region, usecols=['r_regionkey', 'r_name'], parse_dates=[])
  
        set_table_size('customer',customer.npartitions)
        set_table_size('orders',orders.npartitions)
        set_table_size('lineitem',lineitem.npartitions)
#         set_table_size('nation',nation.npartitions)
#         set_table_size('supplier',supplier.npartitions)
#         set_table_size('region',region.npartitions)

        self.add_columns_index(orders,"orders")
        self.add_columns_index(customer,"customer")
        self.add_columns_index(lineitem,"lineitem")
#         self.add_columns_index(nation,"nation")
#         self.add_columns_index(supplier,"supplier")
#         self.add_columns_index(region,"region")
        

        self.p = distpartd.PandasColumns('hdfs:///tempDir', str(hdfs_node), hdfs_port)
        
        self.create_all_index_2('customer',customer)
        self.create_all_index_2('orders',orders)
        #self.create_all_index_2('lineitem',lineitem)
        #self.create_all_index('nation',nation)
        #self.create_all_index('supplier',supplier)
        #self.create_all_index('region',region)
        
        customer_col = clustered_indices.get('customer')
        orders_col = non_clustered_indices.get('orders')
        
        
        customer_col = customer_col.set_index('c_custkey', 0)
        orders_col = orders_col.set_index('o_custkey', 0)
        merged_index=orders_col.merge(customer_col,left_index=True,right_index=True)
        
        customer = self.read_relation_from_hdfs('customer')
        orders = self.read_relation_from_hdfs('orders')
         
        merged_index=merged_index.merge(customer,left_index=True,right_on='c_custkey')
        #merged_index=merged_index.merge(orders,left_on='c_custkey',right_on='o_custkey')
               
        print ("Merged")
        print (merged_index.compute())

        end = timer()
        
        print ("Time : " + str(end - start))


    
    def test_for_normal_dask_merge(self, no_of_runs):
        for i in range(no_of_runs):
            start = timer()
            if i is 0:
                self.orders = dd.read_csv(DATA_FILE_PATH+'orders.csv',delimiter='|',storage_options=storage_opts, names=col_names_orders, usecols=['o_orderkey', 'o_custkey', 'o_orderdate'], parse_dates=['o_orderdate'], date_parser=dateparse)
                self.customer = dd.read_csv(DATA_FILE_PATH+'customer.csv',delimiter='|', storage_options=storage_opts, names=col_names_customer, usecols=['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_comment'], parse_dates=[], date_parser=dateparse)
                self.lineitem = dd.read_csv(DATA_FILE_PATH+'lineitem.csv',delimiter='|', storage_options=storage_opts, names=col_names_lineitem, usecols=['l_orderkey', 'l_suppkey', 'l_extendedprice', 'l_discount'], parse_dates=[])
#                 self.supplier = dd.read_csv(DATA_FILE_PATH+'supplier.csv',delimiter='|', storage_options=storage_opts, names=col_names_supplier, usecols=['s_suppkey', 's_nationkey'], parse_dates=[])
#                 self.nation = dd.read_csv(DATA_FILE_PATH+'nation.csv',delimiter='|', storage_options=storage_opts, names=col_names_nation, usecols=['n_name', 'n_nationkey', 'n_regionkey'], parse_dates=[])
#                 self.region = dd.read_csv(DATA_FILE_PATH+'region.csv',delimiter='|', storage_options=storage_opts, names=col_names_region, usecols=['r_regionkey', 'r_name'], parse_dates=[])

            
            orders = self.orders
            customer = self.customer
            lineitem = self.lineitem
#             supplier = self.supplier
#             nation = self.nation
#             region = self.region
            
            #set_table_size('customer',customer.npartitions)
            #set_table_size('orders',orders.npartitions)
    
            self.add_columns_index(orders,"orders")
            self.add_columns_index(customer,"customer")
            self.add_columns_index(lineitem,"lineitem")
#             self.add_columns_index(supplier,"supplier")
#             self.add_columns_index(nation,"nation")
#             self.add_columns_index(region,"region")
    
            orders = orders.set_index(self.column_mappings["orders"][1], 0)
            merged_table_orders=customer.merge(orders,left_on=self.column_mappings["customer"][0],right_index=True)
            self.add_columns_index(merged_table_orders,"merged_table_orders")
            
            lineitem = lineitem.set_index('l_orderkey', 0)
            merged_table_lineitem=merged_table_orders.merge(lineitem,left_on='o_orderkey',right_index=True)
            
#             merged_table_supplier=merged_table_lineitem.merge(supplier,left_on=['l_suppkey','c_nationkey'],right_on=['s_suppkey','s_nationkey'])
#             
#             nation=nation.set_index('n_nationkey', 0)
#             merged_table_nation=merged_table_supplier.merge(nation,left_on='s_nationkey',right_index=True)
#             
#             region=region.set_index('r_regionkey', 0)
#             merged_table_region=merged_table_nation.merge(region,left_on='n_regionkey',right_index=True)
#             
#             merged_table_region = merged_table_region.set_index('c_custkey', drop=False)
            merged_table_lineitem = merged_table_lineitem.compute()
            print (merged_table_lineitem)
            end = timer()
            print ("test_for_normal_dask_merge : Run " + str(i) + ": " + str(end - start))


    def test_for_normal_pandas_merge(self, no_of_runs):
        DATA_FILE_PATH_2 = '/home1/grads/sdas6/dask-plinq/datasets_for_dask_DB/data_4/'
        for i in range(no_of_runs):
            start = timer()
            if i is 0:
                self.orders = pd.read_csv(DATA_FILE_PATH_2+'orders.csv',delimiter='|', names=col_names_orders, usecols=['o_orderkey', 'o_custkey', 'o_orderdate'], parse_dates=['o_orderdate'], date_parser=dateparse)
                self.customer = pd.read_csv(DATA_FILE_PATH_2+'customer.csv',delimiter='|', names=col_names_customer, usecols=['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_comment'], parse_dates=[], date_parser=dateparse)
#                 self.lineitem = pd.read_csv(DATA_FILE_PATH+'lineitem.csv',delimiter='|', names=col_names_lineitem, usecols=['l_orderkey', 'l_suppkey', 'l_extendedprice', 'l_discount'], parse_dates=[])
  
            orders = self.orders
            customer = self.customer
#             lineitem = self.lineitem
               
            self.add_columns_index(orders,"orders")
            self.add_columns_index(customer,"customer")
    
            orders = orders.set_index(self.column_mappings["orders"][1], 0)
            #customer = customer.set_index(self.column_mappings["customer"][0],0)
            merged_table=customer.merge(orders,left_on=self.column_mappings["customer"][0],right_index=True)
            #merged_table_orders=customer.merge(orders,left_index=True,right_on=self.column_mappings["orders"][1])
#             lineitem = lineitem.set_index('l_orderkey', 0)
#             merged_table=merged_table.merge(lineitem,left_on='o_orderkey',right_index=True)
            print (merged_table)
            end = timer()
            print ("test_for_normal_pandas_merge : Run " + str(i) + ": " + str(end - start))

    
        
#     @profile    
    def run_q1_with_persist(self, no_of_runs=4):

        self.p = distpartd.PandasColumns('hdfs:///tempDir', str(hdfs_node), hdfs_port)

        first_time = True
        for i in range(no_of_runs):
            start = timer()
            if i is 0:
            
                orders  = dd.read_csv(DATA_FILE_PATH+'orders.csv',delimiter='|', storage_options=storage_opts, names=col_names_orders, usecols=['o_orderkey', 'o_orderdate'], parse_dates=['o_orderdate'])
                lineitem = dd.read_csv(DATA_FILE_PATH+'lineitem.csv',delimiter='|', storage_options=storage_opts, names=col_names_lineitem, usecols=['l_orderkey', 'l_extendedprice', 'l_discount'], parse_dates=[])
      
                set_table_size('orders',orders.npartitions)
                set_table_size('lineitem',lineitem.npartitions)
                
                self.create_all_index('orders',orders)
                self.create_all_index('lineitem',lineitem)

                self.orders = self.read_relation_from_hdfs('orders')
                self.lineitem = self.read_relation_from_hdfs('lineitem')

            orders = self.orders 
            lineitem = self.lineitem 
    
            self.add_columns_index(orders,"orders")
            self.add_columns_index(lineitem,"lineitem")
            
            orders=orders[orders[self.column_mappings["orders"][1]]>=pd.to_datetime("1995-01-01")]
            #orders = clientHadoop.persist(orders)
            orders=orders.rename(columns={self.column_mappings["orders"][0]:"o_orderkey"})
            self.add_columns_index(orders,"orders")
            orders=orders.loc[:,["o_orderkey"]]
            self.add_columns_index(orders,"orders")
            divisions = get_table_division("lineitem",self.column_mappings["lineitem"][0])
#             if(first_time):
#                 f_lineitem=lineitem.set_index(self.column_mappings["lineitem"][0], divisions=divisions)
#                 f_lineitem = clientHadoop.persist(f_lineitem)
#             lineitem = f_lineitem
            
            
            set_table_division("lineitem",self.column_mappings["lineitem"][0],lineitem.divisions)
            indexed_col2 =self.column_mappings["lineitem"][0]
            
            merged_table_lineitem = self.merge_tables('lineitem', lineitem, [self.column_mappings["lineitem"][0]], 'orders', orders, [self.column_mappings["orders"][0]])
            #merged_table_lineitem=orders.merge(lineitem,left_on=self.column_mappings["orders"][0],right_index=True)
            
            merged_table_lineitem = clientHadoop.persist(merged_table_lineitem)
            len_col1 =len(self.column_mappings["orders"])
            self.add_columns_index(merged_table_lineitem,"merged_table_lineitem")
            merged_table_lineitem=merged_table_lineitem.loc[:,[self.column_mappings["merged_table_lineitem"][1+len_col1-1],self.column_mappings["merged_table_lineitem"][2+len_col1-1]]]
            merged_table_lineitem=merged_table_lineitem.reset_index()
            merged_table_lineitem=merged_table_lineitem.rename(columns={"index":indexed_col2})
            self.add_columns_index(merged_table_lineitem,"merged_table_lineitem")
            merged_table_lineitem=merged_table_lineitem.loc[:,[self.column_mappings["merged_table_lineitem"][0],self.column_mappings["merged_table_lineitem"][1],self.column_mappings["merged_table_lineitem"][2],]]
            self.add_columns_index(merged_table_lineitem,"merged_table_lineitem")
            merged_table_lineitem=merged_table_lineitem.rename(columns={self.column_mappings["merged_table_lineitem"][0]:"l_orderkey"})
            self.add_columns_index(merged_table_lineitem,"merged_table_lineitem")
            merged_table_lineitem["_COLUMN1_"]=merged_table_lineitem[self.column_mappings["merged_table_lineitem"][1]]*(1-merged_table_lineitem[self.column_mappings["merged_table_lineitem"][2]])
            merged_table_lineitem=merged_table_lineitem.loc[:,["l_orderkey","_COLUMN1_"]]
            self.add_columns_index(merged_table_lineitem,"merged_table_lineitem")
            gb=merged_table_lineitem.groupby([self.column_mappings["merged_table_lineitem"][0]])
            merged_table_lineitem=gb.agg({self.column_mappings["merged_table_lineitem"][1]:"sum"})
            merged_table_lineitem = clientHadoop.persist(merged_table_lineitem)
            
            merged_table_lineitem=merged_table_lineitem.reset_index()
            self.add_columns_index(merged_table_lineitem,"merged_table_lineitem")
            merged_table_lineitem=merged_table_lineitem.rename(columns={self.column_mappings["merged_table_lineitem"][0]:"l_orderkey",self.column_mappings["merged_table_lineitem"][1]:"revenue"})
            self.add_columns_index(merged_table_lineitem,"merged_table_lineitem")
            merged_table_lineitem=merged_table_lineitem.loc[:,["l_orderkey","revenue"]]
            self.add_columns_index(merged_table_lineitem,"merged_table_lineitem")
            merged_table_lineitem=merged_table_lineitem.nlargest(5,columns=["revenue"]).compute()            
            print (merged_table_lineitem)
            end = timer()
            print ("Total time taken: ", str(end-start) + "\n")
            #print "Time taken (except file loadng): ", str(end-start1) + "\n"
            
            first_time = False
        return merged_table_lineitem


    def run_q3_with_persist(self, no_of_runs=1):
        
        #self.p = distpartd.PandasColumns('hdfs:///tempDir', str(hdfs_node), hdfs_port)
        first_time = True
        for i in range(no_of_runs):
            start = timer()
            if i is 0:
            
                lineitem = dd.read_csv(DATA_FILE_PATH+'lineitem.csv',delimiter='|', storage_options=storage_opts, names=col_names_lineitem, usecols=['l_orderkey', 'l_extendedprice', 'l_discount', 'l_shipdate'], parse_dates=['l_shipdate'])
                orders  = dd.read_csv(DATA_FILE_PATH+'orders.csv',delimiter='|', storage_options=storage_opts, names=col_names_orders, usecols=['o_orderkey', 'o_custkey', 'o_orderdate', 'o_shippriority'], parse_dates=['o_orderdate'])
                customer  = dd.read_csv(DATA_FILE_PATH+'customer.csv',delimiter='|', storage_options=storage_opts, names=col_names_customer, usecols=['c_custkey', 'c_mktsegment'], parse_dates=[])     
      
                set_table_size('orders',orders.npartitions)
                set_table_size('lineitem',lineitem.npartitions)
                set_table_size('customer',customer.npartitions)
                
                #self.create_all_index('orders',orders)
                #self.create_all_index('lineitem',lineitem)
                #self.create_all_index('customer',customer)

                #self.orders = self.read_relation_from_hdfs('orders')
                #self.lineitem = self.read_relation_from_hdfs('lineitem')
                #self.customer = self.read_relation_from_hdfs('customer')
            
            #customer = self.customer
            #lineitem = self.lineitem
            #orders = self.orders
            
            self.add_columns_index(customer,"customer")
            customer=customer[customer[self.column_mappings["customer"][1]]=="HOUSEHOLD"]
            #customer = clientHadoop.persist(customer)
            
            self.add_columns_index(orders,"orders")
            orders=orders[orders[self.column_mappings["orders"][2]]<pd.to_datetime("1995-03-21")]
            #orders = clientHadoop.persist(orders)
            
            self.add_columns_index(lineitem,"lineitem")
            lineitem=lineitem[lineitem[self.column_mappings["lineitem"][3]]>pd.to_datetime("1995-03-21")]
            #lineitem = clientHadoop.persist(lineitem)
            
            lineitem=lineitem.rename(columns={self.column_mappings["lineitem"][0]:"l_orderkey",self.column_mappings["lineitem"][1]:"l_extendedprice",self.column_mappings["lineitem"][2]:"l_discount"})
            self.add_columns_index(lineitem,"lineitem")
            lineitem=lineitem.loc[:,["l_orderkey","l_extendedprice","l_discount"]]
            self.add_columns_index(lineitem,"lineitem")
            divisions = get_table_division("customer",self.column_mappings["customer"][0])
            
            #customer=customer.set_index(self.column_mappings["customer"][0], divisions=divisions)
#             if(first_time):
#                 f_customer=customer.set_index(self.column_mappings["customer"][0], divisions=divisions)
#             customer = f_customer
#             customer = clientHadoop.persist(customer)
            
            set_table_division("customer",self.column_mappings["customer"][0],customer.divisions)
            indexed_col2 =self.column_mappings["customer"][0]
            
            merged_table_customer = self.merge_tables('customer', customer, [self.column_mappings["customer"][0]], 'orders', orders, [self.column_mappings["orders"][1]])
#             merged_table_customer=orders.merge(customer,left_on=self.column_mappings["orders"][1],right_index=True)
            len_col1 =len(self.column_mappings["orders"])
            self.add_columns_index(merged_table_customer,"merged_table_customer")
            merged_table_customer=merged_table_customer.loc[:,[self.column_mappings["merged_table_customer"][2],self.column_mappings["merged_table_customer"][3],self.column_mappings["merged_table_customer"][4]]]
            merged_table_customer=merged_table_customer.reset_index(drop=True)
            self.add_columns_index(merged_table_customer,"merged_table_customer")
            divisions = get_table_division("lineitem",self.column_mappings["lineitem"][0])
            
            #lineitem=lineitem.set_index(self.column_mappings["lineitem"][0], divisions=divisions)
#             if(first_time):
#                 f_lineitem=lineitem.set_index(self.column_mappings["lineitem"][0], divisions=divisions)
#             lineitem = f_lineitem
#             lineitem = clientHadoop.persist(lineitem)
            
            set_table_division("lineitem",self.column_mappings["lineitem"][0],lineitem.divisions)
            indexed_col2 =self.column_mappings["lineitem"][0]
            
            merged_table_lineitem = self.merge_tables(temp_join_rel_name, merged_table_customer, [self.column_mappings["merged_table_customer"][0]], 'lineitem', lineitem, [self.column_mappings["lineitem"][0]])
#             merged_table_lineitem=merged_table_customer.merge(lineitem,left_on=self.column_mappings["merged_table_customer"][0],right_index=True)
            len_col1 =len(self.column_mappings["merged_table_customer"])
            self.add_columns_index(merged_table_lineitem,"merged_table_lineitem")
            merged_table_lineitem=merged_table_lineitem.loc[:,[self.column_mappings["merged_table_lineitem"][1],self.column_mappings["merged_table_lineitem"][2],self.column_mappings["merged_table_lineitem"][1+len_col1-1],self.column_mappings["merged_table_lineitem"][2+len_col1-1]]]
            merged_table_lineitem=merged_table_lineitem.reset_index()
            merged_table_lineitem=merged_table_lineitem.rename(columns={"index":indexed_col2})
            self.add_columns_index(merged_table_lineitem,"merged_table_lineitem")
            merged_table_lineitem=merged_table_lineitem.loc[:,[self.column_mappings["merged_table_lineitem"][1],self.column_mappings["merged_table_lineitem"][2],self.column_mappings["merged_table_lineitem"][0],self.column_mappings["merged_table_lineitem"][3],self.column_mappings["merged_table_lineitem"][4],]]
            self.add_columns_index(merged_table_lineitem,"merged_table_lineitem")
            merged_table_lineitem=merged_table_lineitem.rename(columns={self.column_mappings["merged_table_lineitem"][0]:"o_orderdate",self.column_mappings["merged_table_lineitem"][1]:"o_shippriority",self.column_mappings["merged_table_lineitem"][2]:"l_orderkey"})
            self.add_columns_index(merged_table_lineitem,"merged_table_lineitem")
            merged_table_lineitem["_COLUMN3_"]=merged_table_lineitem[self.column_mappings["merged_table_lineitem"][3]]*(1-merged_table_lineitem[self.column_mappings["merged_table_lineitem"][4]])
            merged_table_lineitem=merged_table_lineitem.loc[:,["o_orderdate","o_shippriority","l_orderkey","_COLUMN3_"]]
            self.add_columns_index(merged_table_lineitem,"merged_table_lineitem")
            
            gb=merged_table_lineitem.groupby([self.column_mappings["merged_table_lineitem"][2],self.column_mappings["merged_table_lineitem"][0],self.column_mappings["merged_table_lineitem"][1]])
            merged_table_lineitem=gb.agg({self.column_mappings["merged_table_lineitem"][3]:"sum"})
            merged_table_lineitem = clientHadoop.persist(merged_table_lineitem)
            
            merged_table_lineitem=merged_table_lineitem.reset_index()
            self.add_columns_index(merged_table_lineitem,"merged_table_lineitem")
            merged_table_lineitem=merged_table_lineitem.rename(columns={self.column_mappings["merged_table_lineitem"][0]:"l_orderkey",self.column_mappings["merged_table_lineitem"][3]:"revenue",self.column_mappings["merged_table_lineitem"][1]:"o_orderdate",self.column_mappings["merged_table_lineitem"][2]:"o_shippriority"})
            self.add_columns_index(merged_table_lineitem,"merged_table_lineitem")
            merged_table_lineitem=merged_table_lineitem.loc[:,["l_orderkey","revenue","o_orderdate","o_shippriority"]]
            self.add_columns_index(merged_table_lineitem,"merged_table_lineitem")
            merged_table_lineitem=merged_table_lineitem.nlargest(10,columns=["revenue","o_orderdate"]).compute()   
        
            print (merged_table_lineitem)
            end = timer()
            print ("Total time taken: ", str(end-start) + "\n")
            first_time = False
        
        return merged_table_lineitem
        

    def run_q5_with_persist(self, no_of_runs=4):
         
        self.p = distpartd.PandasColumns('hdfs:///tempDir', str(hdfs_node), hdfs_port)
        
        first_time = True
        for i in range(no_of_runs):
            
            start = timer()
            
            if i is 0:
            
                lineitem = dd.read_csv(DATA_FILE_PATH+'lineitem.csv',delimiter='|', storage_options=storage_opts, names=col_names_lineitem, usecols=['l_orderkey', 'l_suppkey', 'l_extendedprice', 'l_discount'], parse_dates=[])
                orders  = dd.read_csv(DATA_FILE_PATH+'orders.csv',delimiter='|', storage_options=storage_opts, names=col_names_orders, usecols=['o_orderkey', 'o_custkey', 'o_orderdate'], parse_dates=['o_orderdate'])
                customer  = dd.read_csv(DATA_FILE_PATH+'customer.csv',delimiter='|', storage_options=storage_opts, names=col_names_customer, usecols=['c_custkey', 'c_nationkey'], parse_dates=[])
                nation = dd.read_csv(DATA_FILE_PATH+'nation.csv',delimiter='|', storage_options=storage_opts, names=col_names_nation, usecols=['n_name', 'n_nationkey', 'n_regionkey'], parse_dates=[])
                supplier = dd.read_csv(DATA_FILE_PATH+'supplier.csv',delimiter='|', storage_options=storage_opts,names=col_names_supplier, usecols=['s_suppkey', 's_nationkey'], parse_dates=[])
                region = dd.read_csv(DATA_FILE_PATH+'region.csv',delimiter='|', storage_options=storage_opts, names=col_names_region, usecols=['r_regionkey', 'r_name'], parse_dates=[])
    
                set_table_size('lineitem',lineitem.npartitions)
                set_table_size('orders',orders.npartitions)
                set_table_size('customer',customer.npartitions)
                set_table_size('nation',nation.npartitions)
                set_table_size('supplier',supplier.npartitions)
                set_table_size('region',region.npartitions)
    
                self.create_all_index('lineitem',lineitem)
                self.create_all_index('orders',orders)
                self.create_all_index('customer',customer)
                self.create_all_index('nation',nation)
                self.create_all_index('supplier',supplier)
                self.create_all_index('region',region)                    
            
                self.lineitem = self.read_relation_from_hdfs('lineitem')
                self.orders = self.read_relation_from_hdfs('orders')
                self.customer = self.read_relation_from_hdfs('customer')
                self.nation = self.read_relation_from_hdfs('nation')
                self.supplier = self.read_relation_from_hdfs('supplier')
                self.region = self.read_relation_from_hdfs('region')


            lineitem = self.lineitem
            orders = self.orders
            customer = self.customer
            nation = self.nation
            supplier = self.supplier
            region = self.region
            
            self.add_columns_index(orders,"orders")
            orders=orders[(orders[self.column_mappings["orders"][2]]>=pd.to_datetime("1995-01-01"))&(orders[self.column_mappings["orders"][2]]<(pd.to_datetime("1995-01-01")+pd.DateOffset(years=1)))]
            #orders = clientHadoop.persist(orders)
            
            self.add_columns_index(customer,"customer")
            self.add_columns_index(lineitem,"lineitem")
            divisions = get_table_division("orders",self.column_mappings["orders"][1])
            
#             if(first_time):
#                 f_orders=orders.set_index(self.column_mappings["orders"][1], divisions=divisions)
#             orders = f_orders
#             orders = clientHadoop.persist(orders)
            
            set_table_division("orders",self.column_mappings["orders"][1],orders.divisions)
            indexed_col2 =self.column_mappings["orders"][1]

            merged_table_orders = self.merge_tables('orders', orders, [self.column_mappings["orders"][1]], 'customer', customer, [self.column_mappings["customer"][0]])
            #merged_table_orders=customer.merge(orders,left_on=self.column_mappings["customer"][0],right_index=True)
            len_col1 =len(self.column_mappings["customer"])
            self.add_columns_index(merged_table_orders,"merged_table_orders")
            merged_table_orders=merged_table_orders.loc[:,[self.column_mappings["merged_table_orders"][0],self.column_mappings["merged_table_orders"][1],self.column_mappings["merged_table_orders"][0+len_col1],self.column_mappings["merged_table_orders"][2+len_col1-1]]]
            merged_table_orders=merged_table_orders.reset_index()
            merged_table_orders=merged_table_orders.rename(columns={"index":indexed_col2})
            self.add_columns_index(merged_table_orders,"merged_table_orders")
            merged_table_orders=merged_table_orders.loc[:,[self.column_mappings["merged_table_orders"][1],self.column_mappings["merged_table_orders"][2],self.column_mappings["merged_table_orders"][3],self.column_mappings["merged_table_orders"][0],self.column_mappings["merged_table_orders"][4],]]
            self.add_columns_index(merged_table_orders,"merged_table_orders")
            self.add_columns_index(supplier,"supplier")
            divisions = get_table_division("lineitem",self.column_mappings["lineitem"][0])
            
#             if(first_time):
#                 f_lineitem=lineitem.set_index(self.column_mappings["lineitem"][0], divisions=divisions)
#             lineitem = f_lineitem
#             lineitem = clientHadoop.persist(lineitem)
            
            set_table_division("lineitem",self.column_mappings["lineitem"][0],lineitem.divisions)
            indexed_col2 =self.column_mappings["lineitem"][0]
            
            merged_table_lineitem = self.merge_tables(temp_join_rel_name, merged_table_orders, [self.column_mappings["merged_table_orders"][2]], 'lineitem', lineitem, [self.column_mappings["lineitem"][0]])
            #merged_table_lineitem=merged_table_orders.merge(lineitem,left_on=self.column_mappings["merged_table_orders"][2],right_index=True)
            len_col1 =len(self.column_mappings["merged_table_orders"])
            self.add_columns_index(merged_table_lineitem,"merged_table_lineitem")
            merged_table_lineitem=merged_table_lineitem.loc[:,[self.column_mappings["merged_table_lineitem"][0],self.column_mappings["merged_table_lineitem"][1],self.column_mappings["merged_table_lineitem"][2],self.column_mappings["merged_table_lineitem"][3],self.column_mappings["merged_table_lineitem"][4],self.column_mappings["merged_table_lineitem"][1+len_col1-1],self.column_mappings["merged_table_lineitem"][2+len_col1-1],self.column_mappings["merged_table_lineitem"][3+len_col1-1]]]
            merged_table_lineitem=merged_table_lineitem.reset_index()
            merged_table_lineitem=merged_table_lineitem.rename(columns={"index":indexed_col2})
            self.add_columns_index(merged_table_lineitem,"merged_table_lineitem")
            merged_table_lineitem=merged_table_lineitem.loc[:,[self.column_mappings["merged_table_lineitem"][1],self.column_mappings["merged_table_lineitem"][2],self.column_mappings["merged_table_lineitem"][3],self.column_mappings["merged_table_lineitem"][4],self.column_mappings["merged_table_lineitem"][5],self.column_mappings["merged_table_lineitem"][0],self.column_mappings["merged_table_lineitem"][6],self.column_mappings["merged_table_lineitem"][7],self.column_mappings["merged_table_lineitem"][8],]]
            self.add_columns_index(merged_table_lineitem,"merged_table_lineitem")
            self.add_columns_index(nation,"nation")
            
            merged_table_supplier = self.merge_tables(temp_join_rel_name, merged_table_lineitem, [self.column_mappings["merged_table_lineitem"][1],self.column_mappings["merged_table_lineitem"][6]], 'supplier', supplier, [self.column_mappings["supplier"][1],self.column_mappings["supplier"][0]])
            #merged_table_supplier=merged_table_lineitem.merge(supplier,left_on=[self.column_mappings["merged_table_lineitem"][1],self.column_mappings["merged_table_lineitem"][6]],right_on=[self.column_mappings["supplier"][1],self.column_mappings["supplier"][0]])
            len_col1 =len(self.column_mappings["merged_table_lineitem"])
            self.add_columns_index(merged_table_supplier,"merged_table_supplier")
            merged_table_supplier=merged_table_supplier.loc[:,[self.column_mappings["merged_table_supplier"][0],self.column_mappings["merged_table_supplier"][1],self.column_mappings["merged_table_supplier"][2],self.column_mappings["merged_table_supplier"][3],self.column_mappings["merged_table_supplier"][4],self.column_mappings["merged_table_supplier"][5],self.column_mappings["merged_table_supplier"][6],self.column_mappings["merged_table_supplier"][7],self.column_mappings["merged_table_supplier"][8],self.column_mappings["merged_table_supplier"][0+len_col1],self.column_mappings["merged_table_supplier"][1+len_col1]]]
            self.add_columns_index(merged_table_supplier,"merged_table_supplier")
            self.add_columns_index(region,"region")
            region=region[region[self.column_mappings["region"][1]]=="AMERICA"]
            region=region.rename(columns={self.column_mappings["region"][0]:"r_regionkey"})
            self.add_columns_index(region,"region")
            region=region.loc[:,["r_regionkey"]]
            self.add_columns_index(region,"region")
            divisions = get_table_division("nation",self.column_mappings["nation"][0])
            
#             if(first_time):
#                 f_nation=nation.set_index(self.column_mappings["nation"][0], divisions=divisions)
#             nation = f_nation
#             nation = clientHadoop.persist(nation)
            
            set_table_division("nation",self.column_mappings["nation"][0],nation.divisions)
            indexed_col2 =self.column_mappings["nation"][0]
            
            merged_table_nation = self.merge_tables(temp_join_rel_name, merged_table_supplier, [self.column_mappings["merged_table_supplier"][10]], 'nation', nation, [self.column_mappings["nation"][0]])
            #merged_table_nation=merged_table_supplier.merge(nation,left_on=self.column_mappings["merged_table_supplier"][10],right_index=True)
            len_col1 =len(self.column_mappings["merged_table_supplier"])
            self.add_columns_index(merged_table_nation,"merged_table_nation")
            merged_table_nation=merged_table_nation.loc[:,[self.column_mappings["merged_table_nation"][7],self.column_mappings["merged_table_nation"][8],self.column_mappings["merged_table_nation"][1+len_col1-1],self.column_mappings["merged_table_nation"][2+len_col1-1]]]
            merged_table_nation=merged_table_nation.reset_index(drop=True)
            self.add_columns_index(merged_table_nation,"merged_table_nation")
            divisions = get_table_division("region",self.column_mappings["region"][0])
            
#             if(first_time):
#                 f_region=region.set_index(self.column_mappings["region"][0], divisions=divisions)
#             region = f_region
#             region = clientHadoop.persist(region)
            
            set_table_division("region",self.column_mappings["region"][0],region.divisions)
            indexed_col2 =self.column_mappings["region"][0]
            
            merged_table_region = self.merge_tables(temp_join_rel_name, merged_table_nation, [self.column_mappings["merged_table_nation"][3]], 'region', region, [self.column_mappings["region"][0]])
            #merged_table_region=merged_table_nation.merge(region,left_on=self.column_mappings["merged_table_nation"][3],right_index=True)
            len_col1 =len(self.column_mappings["merged_table_nation"])
            self.add_columns_index(merged_table_region,"merged_table_region")
            merged_table_region=merged_table_region.loc[:,[self.column_mappings["merged_table_region"][0],self.column_mappings["merged_table_region"][1],self.column_mappings["merged_table_region"][2]]]
            merged_table_region=merged_table_region.reset_index(drop=True)
            self.add_columns_index(merged_table_region,"merged_table_region")
            merged_table_region=merged_table_region.rename(columns={self.column_mappings["merged_table_region"][2]:"n_name"})
            self.add_columns_index(merged_table_region,"merged_table_region")
            merged_table_region["_COLUMN1_"]=merged_table_region[self.column_mappings["merged_table_region"][0]]*(1-merged_table_region[self.column_mappings["merged_table_region"][1]])
            merged_table_region=merged_table_region.loc[:,["n_name","_COLUMN1_"]]
            self.add_columns_index(merged_table_region,"merged_table_region")
            gb=merged_table_region.groupby([self.column_mappings["merged_table_region"][0]])
            merged_table_region=gb.agg({self.column_mappings["merged_table_region"][1]:"sum"})
            merged_table_region = clientHadoop.persist(merged_table_region)
            
            merged_table_region=merged_table_region.reset_index()
            self.add_columns_index(merged_table_region,"merged_table_region")
            merged_table_region=merged_table_region.rename(columns={self.column_mappings["merged_table_region"][0]:"n_name",self.column_mappings["merged_table_region"][1]:"revenue"})
            self.add_columns_index(merged_table_region,"merged_table_region")
            merged_table_region=merged_table_region.loc[:,["n_name","revenue"]]
            self.add_columns_index(merged_table_region,"merged_table_region")
            merged_table_region=merged_table_region.nlargest(1,columns=["revenue"]).compute()

            print (merged_table_region)
            end = timer()
            print ("Total time taken: ", str(end-start) + "\n")
            first_time = False
        
        return merged_table_region
    
    
    def run_q6_with_persist(self, no_of_runs=1):
        
        self.p = distpartd.PandasColumns('hdfs:///tempDir', str(hdfs_node), hdfs_port)
        for i in range(no_of_runs):
            
            start = timer()
            
            if i is 0:
                lineitem = dd.read_csv(DATA_FILE_PATH+'lineitem.csv',delimiter='|', storage_options=storage_opts, names=col_names_lineitem, usecols=['l_quantity', 'l_extendedprice', 'l_discount', 'l_shipdate'], parse_dates=['l_shipdate'])
                set_table_size('lineitem',lineitem.npartitions)
                self.create_all_index('lineitem',lineitem)
                self.lineitem = self.read_relation_from_hdfs('lineitem')
                
            lineitem = self.lineitem
            
            self.add_columns_index(lineitem,"lineitem")
            
            lineitem=lineitem[((((lineitem[self.column_mappings["lineitem"][3]]>=pd.to_datetime("1995-01-01"))&(lineitem[self.column_mappings["lineitem"][3]]<(pd.to_datetime("1995-01-01")+pd.DateOffset(years=1))))&(lineitem[self.column_mappings["lineitem"][2]]>=0.08))&(lineitem[self.column_mappings["lineitem"][2]]<=1))&(lineitem[self.column_mappings["lineitem"][0]]<24)]
            #lineitem = clientHadoop.persist(lineitem)
            
            lineitem=lineitem.rename(columns={self.column_mappings["lineitem"][2]:"l_discount"})
            self.add_columns_index(lineitem,"lineitem")
            lineitem["_COLUMN1_"]=lineitem[self.column_mappings["lineitem"][1]]*lineitem[self.column_mappings["lineitem"][2]]
            lineitem=lineitem.loc[:,["l_discount","_COLUMN1_"]]
            self.add_columns_index(lineitem,"lineitem")
            
            gb=lineitem.groupby([self.column_mappings["lineitem"][0]])
            lineitem=gb.agg({self.column_mappings["lineitem"][1]:"sum"})
            lineitem = clientHadoop.persist(lineitem)
            
            lineitem=lineitem.reset_index()
            self.add_columns_index(lineitem,"lineitem")
            lineitem=lineitem.rename(columns={self.column_mappings["lineitem"][0]:"l_discount",self.column_mappings["lineitem"][1]:"revenue"})
            self.add_columns_index(lineitem,"lineitem")
            lineitem=lineitem.loc[:,["l_discount","revenue"]]
            self.add_columns_index(lineitem,"lineitem")
            lineitem=lineitem.head(1)

            print (lineitem)
            end = timer()
            print ("Total time taken: ", str(end-start) + "\n")
        
        return lineitem
    

    def run_q10(self, no_of_runs=1):
            
        self.p = distpartd.PandasColumns('hdfs:///tempDir', str(hdfs_node), hdfs_port)
        first_time = True;
        for i in range(no_of_runs):
            
            start = timer()
            if i is 0:
            
                orders = dd.read_csv(DATA_FILE_PATH+'orders.csv',delimiter='|', storage_options=storage_opts, names=col_names_orders, usecols=['o_orderkey', 'o_custkey', 'o_orderdate'], parse_dates=['o_orderdate'], date_parser=dateparse)
                nation = dd.read_csv(DATA_FILE_PATH+'nation.csv',delimiter='|', storage_options=storage_opts, names=col_names_nation, parse_dates=[], date_parser=dateparse)
                customer = dd.read_csv(DATA_FILE_PATH+'customer.csv',delimiter='|', storage_options=storage_opts, names=col_names_customer, usecols=['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_comment'], parse_dates=[], date_parser=dateparse)
                lineitem = dd.read_csv(DATA_FILE_PATH+'lineitem.csv',delimiter='|', storage_options=storage_opts, names=col_names_lineitem, usecols=['l_orderkey', 'l_extendedprice', 'l_discount', 'l_returnflag'], parse_dates=[], date_parser=dateparse)

                set_table_size('lineitem',lineitem.npartitions)
                set_table_size('orders',orders.npartitions)
                set_table_size('customer',customer.npartitions)
                set_table_size('nation',nation.npartitions)

                self.create_all_index('lineitem',lineitem)
                self.create_all_index('orders',orders)
                self.create_all_index('customer',customer)
                self.create_all_index('nation',nation)
            
                self.lineitem = self.read_relation_from_hdfs('lineitem')
                self.orders = self.read_relation_from_hdfs('orders')
                self.customer = self.read_relation_from_hdfs('customer')
                self.nation = self.read_relation_from_hdfs('nation')

            lineitem = self.lineitem
            orders = self.orders
            customer = self.customer
            nation = self.nation
            
            self.add_columns_index(orders,"orders")
            self.add_columns_index(customer,"customer")
            self.add_columns_index(lineitem,"lineitem")
            self.add_columns_index(nation,"nation")
            orders=orders[(orders[self.column_mappings["orders"][2]]>=pd.to_datetime("1995-01-01"))&(orders[self.column_mappings["orders"][2]]<(pd.to_datetime("1995-01-01")+pd.DateOffset(months=3)))]
            
            lineitem=lineitem[lineitem[self.column_mappings["lineitem"][3]]=="R"]
            
            divisions = get_table_division("orders",self.column_mappings["orders"][1])

#             if(first_time):
#                 f_orders=orders.set_index(self.column_mappings["orders"][1], 0)
#             orders = f_orders
#             orders = clientHadoop.persist(orders)
            
            indexed_col2 =self.column_mappings["orders"][1]

            merged_table_orders = self.merge_tables('orders', orders, [self.column_mappings["orders"][1]], 'customer', customer, [self.column_mappings["customer"][0]])
            #merged_table_orders=customer.merge(orders,left_on=self.column_mappings["customer"][0],right_index=True)
            #merged_table_orders = clientHadoop.persist(merged_table_orders)
           
            len_col1 =len(self.column_mappings["customer"])
            self.add_columns_index(merged_table_orders,"merged_table_orders")
            merged_table_orders=merged_table_orders.loc[:,[self.column_mappings["merged_table_orders"][0],self.column_mappings["merged_table_orders"][1],self.column_mappings["merged_table_orders"][2],self.column_mappings["merged_table_orders"][3],self.column_mappings["merged_table_orders"][4],self.column_mappings["merged_table_orders"][5],self.column_mappings["merged_table_orders"][6],self.column_mappings["merged_table_orders"][0+len_col1],self.column_mappings["merged_table_orders"][2+len_col1-1]]]
            merged_table_orders=merged_table_orders.reset_index()
            merged_table_orders=merged_table_orders.rename(columns={"index":indexed_col2})
            self.add_columns_index(merged_table_orders,"merged_table_orders")
            merged_table_orders=merged_table_orders.loc[:,[self.column_mappings["merged_table_orders"][1],self.column_mappings["merged_table_orders"][2],self.column_mappings["merged_table_orders"][3],self.column_mappings["merged_table_orders"][4],self.column_mappings["merged_table_orders"][5],self.column_mappings["merged_table_orders"][6],self.column_mappings["merged_table_orders"][7],self.column_mappings["merged_table_orders"][8],self.column_mappings["merged_table_orders"][0],self.column_mappings["merged_table_orders"][9],]]
            self.add_columns_index(merged_table_orders,"merged_table_orders")
            self.add_columns_index(nation,"nation")
            divisions = get_table_division("lineitem",self.column_mappings["lineitem"][0])

#             if(first_time):
#                 lineitem_f=lineitem.set_index(self.column_mappings["lineitem"][0], 0)
#             lineitem = lineitem_f
#             lineitem = clientHadoop.persist(lineitem) 
            
            indexed_col2 =self.column_mappings["lineitem"][0]
            
            merged_table_lineitem = self.merge_tables(temp_join_rel_name, merged_table_orders, [self.column_mappings["merged_table_orders"][7]], 'lineitem', lineitem, [self.column_mappings["lineitem"][0]])
            #merged_table_lineitem=merged_table_orders.merge(lineitem,left_on=self.column_mappings["merged_table_orders"][7],right_index=True)

            
            len_col1 =len(self.column_mappings["merged_table_orders"])
            self.add_columns_index(merged_table_lineitem,"merged_table_lineitem")
            merged_table_lineitem=merged_table_lineitem.loc[:,[self.column_mappings["merged_table_lineitem"][0],self.column_mappings["merged_table_lineitem"][1],self.column_mappings["merged_table_lineitem"][2],self.column_mappings["merged_table_lineitem"][3],self.column_mappings["merged_table_lineitem"][4],self.column_mappings["merged_table_lineitem"][5],self.column_mappings["merged_table_lineitem"][6],self.column_mappings["merged_table_lineitem"][1+len_col1-1],self.column_mappings["merged_table_lineitem"][2+len_col1-1]]]
            merged_table_lineitem=merged_table_lineitem.reset_index(drop=True)
            self.add_columns_index(merged_table_lineitem,"merged_table_lineitem")
            divisions = get_table_division("nation",self.column_mappings["nation"][0])

#             if(first_time):
#                 nation_f=nation.set_index(self.column_mappings["nation"][0], 0)
#             nation = nation_f    
#             nation = clientHadoop.persist(nation)
            
            indexed_col2 =self.column_mappings["nation"][0]
            
            merged_table_nation = self.merge_tables(temp_join_rel_name, merged_table_lineitem, [self.column_mappings["merged_table_lineitem"][3]], 'nation', nation, [self.column_mappings["nation"][0]])
            #merged_table_nation=merged_table_lineitem.merge(nation,left_on=self.column_mappings["merged_table_lineitem"][3],right_index=True)
            
            len_col1 =len(self.column_mappings["merged_table_lineitem"])
            self.add_columns_index(merged_table_nation,"merged_table_nation")
            merged_table_nation=merged_table_nation.loc[:,[self.column_mappings["merged_table_nation"][0],self.column_mappings["merged_table_nation"][1],self.column_mappings["merged_table_nation"][2],self.column_mappings["merged_table_nation"][4],self.column_mappings["merged_table_nation"][5],self.column_mappings["merged_table_nation"][6],self.column_mappings["merged_table_nation"][7],self.column_mappings["merged_table_nation"][8],self.column_mappings["merged_table_nation"][1+len_col1-1]]]
            merged_table_nation=merged_table_nation.reset_index(drop=True)
            self.add_columns_index(merged_table_nation,"merged_table_nation")
            merged_table_nation=merged_table_nation.rename(columns={self.column_mappings["merged_table_nation"][0]:"c_custkey",self.column_mappings["merged_table_nation"][1]:"c_name",self.column_mappings["merged_table_nation"][2]:"c_address",self.column_mappings["merged_table_nation"][3]:"c_phone",self.column_mappings["merged_table_nation"][4]:"c_acctbal",self.column_mappings["merged_table_nation"][5]:"c_comment",self.column_mappings["merged_table_nation"][8]:"n_name"})
            self.add_columns_index(merged_table_nation,"merged_table_nation")
            merged_table_nation["_COLUMN7_"]=merged_table_nation[self.column_mappings["merged_table_nation"][6]]*(1-merged_table_nation[self.column_mappings["merged_table_nation"][7]])
            merged_table_nation=merged_table_nation.loc[:,["c_custkey","c_name","c_address","c_phone","c_acctbal","c_comment","n_name","_COLUMN7_"]]
            self.add_columns_index(merged_table_nation,"merged_table_nation")
            gb=merged_table_nation.groupby([self.column_mappings["merged_table_nation"][0],self.column_mappings["merged_table_nation"][1],self.column_mappings["merged_table_nation"][4],self.column_mappings["merged_table_nation"][6],self.column_mappings["merged_table_nation"][2],self.column_mappings["merged_table_nation"][3],self.column_mappings["merged_table_nation"][5]])
            merged_table_nation=gb.agg({self.column_mappings["merged_table_nation"][7]:"sum"})
            merged_table_nation = clientHadoop.persist(merged_table_nation)

            merged_table_nation=merged_table_nation.reset_index()
            self.add_columns_index(merged_table_nation,"merged_table_nation")
            merged_table_nation=merged_table_nation.rename(columns={self.column_mappings["merged_table_nation"][0]:"c_custkey",self.column_mappings["merged_table_nation"][1]:"c_name",self.column_mappings["merged_table_nation"][7]:"revenue",self.column_mappings["merged_table_nation"][2]:"c_acctbal",self.column_mappings["merged_table_nation"][3]:"n_name",self.column_mappings["merged_table_nation"][4]:"c_address",self.column_mappings["merged_table_nation"][5]:"c_phone",self.column_mappings["merged_table_nation"][6]:"c_comment"})
            self.add_columns_index(merged_table_nation,"merged_table_nation")
            merged_table_nation=merged_table_nation.loc[:,["c_custkey","c_name","revenue","c_acctbal","n_name","c_address","c_phone","c_comment"]]
            self.add_columns_index(merged_table_nation,"merged_table_nation")
            
            
            merged_table_nation=merged_table_nation.nlargest(20,columns=["revenue"]).compute()
            print (merged_table_nation)
            
            end = timer()
            
            print ("Time taken : ", str(end - start))
            first_time = False
            #print "Total time taken: ", str(end-start) + "\n"
            #print "Time taken (except file loadng): ", str(end-start1) + "\n"
         
        return merged_table_nation

 
    def test_for_ML(self):
        
        def my_heaviside(x):
            y1 = np.heaviside((100-x)*(x-1),1)*0
            y2 = np.heaviside((150-x)*(x-101),1)*1
            y3 = np.heaviside((210-x)*(x-151),1)*2
            y4 = np.heaviside((310-x)*(x-215),1)*3
            return int(y1+y2+y3+y4)

        x_list = [25, 78, 200, 126, 176, 300, 1, 100, 151, 210, 215, 310, 131]
        
        f = my_heaviside
        for x in x_list:
            print ("x = " + str(x) + " : y = " + str(f(x)))     
dp = DaskPlan()
print ("************************************************run1************************************************")
#dp.test_for_normal_pandas_merge(1)
#dp.test_for_normal_dask_merge(1)
#dp.test_code_learned_index(3)
#dp.sample_run()
#dp.run_q1_with_persist(4)
dp.run_q3_with_persist(2)
#dp.run_q5_with_persist(4)
#dp.run_q6_with_persist(4)
dp.run_q10(4)
#dp.test_for_ML()
