import pandas as pd
import dask.dataframe as dd
#import distpartd
from DaskDB.table_information import get_table_size
import numpy as np
#from memory_profiler import profile
from DaskDB.setup_configuration import get_hdfs_master_node_IP, get_hdfs_master_node_port, get_python_dir
from dask.base import tokenize
from dask.utils import apply
from dask.highlevelgraph import HighLevelGraph
from dask.dataframe.core import new_dd_object
from builtins import isinstance
from dask.dataframe.shuffle import rearrange_by_column
import partd
import os
import importlib

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

posBegin = 0
posEnd = 1
posPartitionList = 2

heaviside_func = {}

sorted_relations = {}

hdfs_node = get_hdfs_master_node_IP()
hdfs_port = get_hdfs_master_node_port()

#dist = distpartd.PandasColumns('hdfs:///tempDir', hdfs_node, hdfs_port, 'w')
#dist = distpartd.Numpy('hdfs:///tempDir', hdfs_node, hdfs_port, 'w')
#hdfs=HDFileSystem(host=hdfs_node ,port=hdfs_port)
dist = partd.Numpy('/home/ashwanta75/data_for_Dask_DB')

clientHadoop = None

map_partition_func_both_sorted = None
map_partition_func_one_sorted = None
partition_join_func = None
partition_info_func_learned = None
partition_info_func_bloom = None
get_partition_info_func = None
filter_foreign_partition_func = None
partition_count_func = None
clean_data_func = None
get_data_func = None
dataframe_dict = {}
scattered_dataframe_dict = {}
#sparse_index_with_partition_list = {}
sparse_indexes = {}
row_size = -1
bloom_matrix_params = {}

temp_data_list = []
bloom_matrix_list = []

def clear_data_from_workers():
    return
    clientHadoop.restart()

def set_map_partition_func(both_sorted_func, one_sorted_func, join_func):
    global map_partition_func_both_sorted
    global map_partition_func_one_sorted
    map_partition_func_both_sorted = both_sorted_func
    map_partition_func_one_sorted = one_sorted_func
    global partition_join_func
    partition_join_func = join_func

def set_partition_info_func(partition_info_function_learned, partition_info_function_bloom):
    global partition_info_func_learned
    global partition_info_func_bloom
    partition_info_func_learned = partition_info_function_learned
    partition_info_func_bloom = partition_info_function_bloom
    
def set_filter_foreign_partition_func(foreign_part_func):
    global filter_foreign_partition_func
    filter_foreign_partition_func = foreign_part_func
    
def set_partition_count_func(part_count_func):
    global partition_count_func
    partition_count_func = part_count_func

def set_clean_data_func(func):
    global clean_data_func
    clean_data_func = func
       
def set_get_data(func):
    global get_data_func
    get_data_func = func   
        
def setDaskClient(client):
    global clientHadoop
    clientHadoop = client

def create_heaviside_function(relName, colName, pdf):
    
    begin = np.array(pdf.iloc[:,posBegin])
    end = np.array(pdf.iloc[:,posEnd])

    def create_heaviside(x):

        l = np.heaviside((end - x) * (x - begin), 1)
        if isinstance(x,int) or isinstance(x,float):
            pos = int(np.where(l==1)[0])
        else:     
            pos = np.where(l[:,None]==1)[2].astype(int)
        return pos        
    
    d = {}
    d[colName] = create_heaviside
    heaviside_func[relName] = d
        

def create_new_index_with_partition_list(relName, sparse_index_pdf):
    begin = sparse_index_pdf.iat[0,0]
    end = sparse_index_pdf.iat[0,1]
    part_list = [sparse_index_pdf.iat[0,2]]
    new_index = pd.DataFrame([[begin,end,part_list]], columns = ['Begin','End','PartitionList'])
    new_index_row_num = 0;
    
    for i in range(1,len(sparse_index_pdf)):
        last_end = sparse_index_pdf.iat[i-1,1]
        begin = sparse_index_pdf.iat[i,0]
        curr_part_no = sparse_index_pdf.iat[i,2] 
        if begin == last_end:
            last_begin = sparse_index_pdf.iat[i-1,0]
            part_list = list(new_index.iat[new_index_row_num,posPartitionList])

            if len(part_list) == 1:                       
                part_list.append(curr_part_no)
            else:
                part_list[1] = curr_part_no    

            if last_begin == last_end:
                new_index.iat[new_index_row_num,posPartitionList] = part_list
            else:
                new_index.iat[new_index_row_num,posEnd] = begin - 1
                new_row = pd.DataFrame([[begin,begin,part_list]], columns = ['Begin','End','PartitionList'])
                new_index = new_index.append(new_row)
                new_index_row_num = new_index_row_num + 1
                
            end = sparse_index_pdf.iat[i,1]
            if begin != end:     
                new_row = pd.DataFrame([[begin+1,end,[curr_part_no]]], columns = ['Begin','End','PartitionList'])
                new_index = new_index.append(new_row)                
                new_index_row_num = new_index_row_num + 1
                    
        else:
          
            end = sparse_index_pdf.iat[i,1]
            new_row = pd.DataFrame([[begin,end,[curr_part_no]]], columns = ['Begin','End','PartitionList'])
            new_index = new_index.append(new_row)                    
            new_index_row_num = new_index_row_num + 1
        
    return new_index                    

def create_index_and_distribute_data(relName, relDataFrame):
    ddf = scattered_dataframe_dict.get(relName)
    key_col_name = primary_key.get(relName)
    if ddf is None:
        futures = []
        #is_bloom_matrix_required = key_col_name and (relDataFrame.npartitions > 1) and (key_col_name in relDataFrame.columns)
        is_bloom_matrix_required = False
        if is_bloom_matrix_required:
            bm = create_bloom_matrix(relName, relDataFrame)
        for i in range(relDataFrame.npartitions):
            rel_pd = relDataFrame.get_partition(i).compute()
            #print("Part : {}, Begin : {}, End : {}".format(i, rel_pd[key_col_name].values[0], rel_pd[key_col_name].values[-1]))
            if is_bloom_matrix_required:
                key_values = rel_pd[key_col_name].values
                bm.add(key_values.astype(np.double), i)
                dist.append({relName + '_part' + str(i):key_values})
            future = clientHadoop.scatter(rel_pd)
            futures.append(future)
        if is_bloom_matrix_required:
            binary_file_name = bm.write_to_binary_file(relName)
            clientHadoop.upload_file(binary_file_name)
 
        ddf = dd.from_delayed(futures, meta=rel_pd)
        scattered_dataframe_dict[relName] = ddf

    return ddf

def can_merge_with_learned_index(relName_1, reldf_1, rel_1_join_col_list, relName_2, reldf_2, rel_2_join_col_list):
    #if relName_1 == 'lineitem' or relName_2 == 'lineitem':
        #return False
    return (is_coloumn_unique(relName_1, rel_1_join_col_list) or is_coloumn_unique(relName_2, rel_2_join_col_list))


def create_bloom_matrix(relName, relDataFrame):
    print('create_bloom_matrix: ' + relName)
    python_dir = get_python_dir()
    compilation_string = """g++ -std=c++11 -Wall -Wextra -fPIC -D NUM_PART={} -shared -I{} bloom_matrix_{}.cpp -o bloom_matrix_{}.so -lboost_python37 -lboost_numpy37 -pthread""".format(relDataFrame.npartitions, python_dir, relName, relName)
    os.system(compilation_string)
    clientHadoop.upload_file('bloom_matrix_{}.so'.format(relName))
    
    size = len(relDataFrame)
    bloom_matrix_module = importlib.import_module('bloom_matrix_{}'.format(relName))
    bm = bloom_matrix_module.BloomMatrix(size, 0.01, relDataFrame.npartitions)
    bloom_matrix_params[relName] = bm.getParams()
    print(bloom_matrix_params)
    return bm

    
def cull_empty_partitions(df):
    ll = list(df.map_partitions(len).compute())
    if 0 in ll:
        df_delayed = df.to_delayed()
        df_delayed_new = list()
        pempty = None
        first_time = True
        for ix, n in enumerate(ll):
            if 0 == n:
                if not first_time:
                    continue
                pempty = df.get_partition(ix)
                first_time = False
            else:
                df_delayed_new.append(df_delayed[ix])
        if pempty is not None:
            #df = dd.from_delayed(df_delayed_new, meta=pempty, divisions='sorted')
            df = dd.from_delayed(df_delayed_new, meta=pempty)
    return df    

def cull_empty_partitions_and_return_list(df):
    ll = np.array(df.map_partitions(len).compute())
    new_ll = np.where(ll > 0)[0]    
    df_delayed = df.to_delayed()
    if len(ll) != len(new_ll):
        df_delayed = np.array(df_delayed)
        return list(df_delayed[new_ll])        
    else:
        return df_delayed

def cull_empty_partitions_new(df):
    ll = np.array(df.map_partitions(len).compute())
    new_ll = np.where(ll > 0)[0]
    size = sum(ll[new_ll])    
    df_delayed = df.to_delayed()
    if len(ll) != len(new_ll):
        df_delayed = np.array(df_delayed)
        df = dd.from_delayed(df_delayed[new_ll])        
    return df, size


def is_good_to_create_db_index_on_column(relName, colName):
    primary_key_col_name = primary_key.get(relName)
    if (not primary_key_col_name) or (primary_key_col_name != colName):
        return False
    return True

def get_primary_and_foreign_relations(relName_1, relName_2):
     
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


def _sort_labels(uniques: np.ndarray, left, right):

    import pandas.core.algorithms as algos 
    
    llength = len(left)
    labels = np.concatenate([left, right])

    sorted_uniques, new_labels = algos.safe_sort(uniques, labels, na_sentinel=-1)
    new_left, new_right = new_labels[:llength], new_labels[llength:]

    return sorted_uniques, new_left, new_right


def _get_join_keys(llab, rlab, shape):

    # how many levels can be done without overflow
    nlev = len(llab)

    # get keys for the first `nlev` levels
    stride = np.prod(shape[1:nlev], dtype="i8")
    lkey = stride * llab[0].astype("i8", subok=False, copy=False)
    rkey = stride * rlab[0].astype("i8", subok=False, copy=False)

    for i in range(1, nlev):
        stride //= shape[i]
        lkey += llab[i] * stride
        rkey += rlab[i] * stride

    return lkey, rkey


def _factorize_keys(lk, rk, sort = False):
    from pandas._libs import hashtable as libhashtable
    
    klass = libhashtable.Int64Factorizer
    rizer = klass(max(len(lk), len(rk)))

    llab = rizer.factorize(lk)
    rlab = rizer.factorize(rk)

    count = rizer.get_count()
    uniques = rizer.uniques.to_array()
    if sort:
        uniques, llab, rlab = _sort_labels(uniques, llab, rlab)

    return llab, rlab, count, uniques
    

def learned_block_nested_loop_join(foreign_part, primary_ddf, foreign_col_list, primary_col_list):
    from pandas._libs.algos import groupsort_indexer

    foreign_part = foreign_part.compute()
    primary_ddf = primary_ddf.compute()
           
    if foreign_part.empty or primary_ddf.empty:
        print('One partition is empty, returning empty dataframe')
        df = pd.concat([foreign_part, primary_ddf], axis=1)
        df = df[0:0]    #remove all entries from the dataframe and make it empty
        return df
    
    lk = [np.array(foreign_part[i]) for i in foreign_col_list]
    rk = [np.array(primary_ddf[i]) for i in primary_col_list]
    
    mapped = (
        _factorize_keys(lk[n], rk[n])
        for n in range(len(lk))
    )
    zipped = zip(*mapped)
    
    llab, rlab, shape, _ = [list(x) for x in zipped]    
    
    lkey, rkey = _get_join_keys(llab, rlab, shape)
    
    #foreign_part['index'] = list(lkey)
    foreign_part['index'] = lkey
    foreign_part.set_index('index', inplace=True)
    #primary_ddf['index'] = list(rkey)
    primary_ddf['index'] = rkey
    primary_ddf.set_index('index', inplace=True)
    
    lkey, rkey, count, uniques = _factorize_keys(lkey, rkey)
    
    max_num_groups = max(np.max(lkey), np.max(rkey)) + 1
    
    l1, l2 = groupsort_indexer(lkey, max_num_groups)
    r1, r2 = groupsort_indexer(rkey, max_num_groups)
    

    l2 = np.delete(l2,[0])
    r2 = np.delete(r2,[0])
    m = l2 * r2
    m2 = np.where(m > 0)
    m2 = m2[0]
    
    l2 = l2[m2]
    r2 = r2[m2]
        
    lidx = np.repeat(m2,r2)
    ridx = np.repeat(m2,l2)
    left_values = uniques[lidx]    
    right_values = uniques[ridx]
            
    if foreign_part.index.is_monotonic and pd.Series(left_values).is_monotonic \
        and pd.Series(left_values).is_unique and len(foreign_col_list) == 1:
        #left_ddf1 = foreign_part.loc[left_values] is time consuming hence avoided
        #this method can be used since left_values are unique
        indices = np.in1d(foreign_part.index, left_values)
        left_ddf = foreign_part[indices]         
    
    else:
        left_ddf = foreign_part.loc[left_values]

    #cannot use the above method here as this does not gives the correct output since
    #right_values are not unique in case of a primary_relations
    if primary_ddf.index.is_unique and primary_ddf.index.is_monotonic and len(primary_col_list) == 1:
        indices = pd.Series(primary_ddf.index).searchsorted(right_values)
        right_ddf = primary_ddf.iloc[indices]         
    else:        
        right_ddf = primary_ddf.loc[right_values]

    for i in right_ddf.columns:
        left_ddf[i] = right_ddf[i]
    #df = pd.concat([left_ddf, right_ddf], axis=1)
    
    return left_ddf

    
def get_partition_list(relName, colName, keyValue):
    heaviside_dict = heaviside_func[relName]
    heaviside = heaviside_dict[colName]
    index_dict = sparse_indexes.get(relName)
    index = index_dict[colName]
    line_no_in_index = heaviside(keyValue) - 1
    if line_no_in_index < 0:
        return None
    partition_list = list(index.iat[line_no_in_index,posPartitionList])
    return partition_list


def get_heaviside_func(relName, colName):
    return heaviside_func[relName][colName]

def get_sparse_index(relName, colName):
    return sparse_indexes[relName][colName]


def get_sparse_index_entry(relName, colName, keyValue):
    heaviside_dict = heaviside_func[relName]
    heaviside = heaviside_dict[colName]
    index_dict = sparse_indexes.get(relName)
    index = index_dict[colName]
    line_no_in_index = heaviside(keyValue) - 1
    if line_no_in_index < 0:
        return None, None, None
    begin = index.iat[line_no_in_index,posBegin]
    end = index.iat[line_no_in_index,posEnd]
    partition_list = list(index.iat[line_no_in_index,posPartitionList])
    return begin, end, partition_list

def check_and_build_sparse_index(relName, reldf, col_name):
    print('check_and_build_sparse_index : ' + relName)
    rel_sparse_index_dict = sparse_indexes.get(relName)
    if rel_sparse_index_dict:
        rel_sparse_index_by_col = rel_sparse_index_dict.get(col_name)
    
    if (not rel_sparse_index_dict) or (rel_sparse_index_by_col is None):
        print('check_and_build_sparse_index invoked : Creating indexes and create heaviside')
        rel_join_col = reldf[col_name].compute()
        if pd.Index(rel_join_col).is_monotonic_increasing:
            reldf = reldf.set_index(col_name, sorted=True, drop=False)
        else:
            reldf = reldf.set_index(col_name, drop=False)
        reldf = clientHadoop.persist(reldf)
        d = {}
        d[col_name] = reldf        
        sorted_relations[relName] = d
        
        col_pos = reldf.columns.get_loc(col_name)
        sparse_index = pd.DataFrame()
        for i in range(reldf.npartitions):
            first_row = reldf.get_partition(i).head(1)
            last_row = reldf.get_partition(i).tail(1)
            begin = first_row.iat[0,col_pos]
            end = last_row.iat[0,col_pos]
            df = pd.DataFrame([[begin,end,i]], columns = ['Begin','End','Partition'])
            sparse_index = sparse_index.append(df)    

        new_index = create_new_index_with_partition_list(relName, sparse_index)    
        create_heaviside_function(relName, col_name, new_index) 
            
        if not rel_sparse_index_dict:     #No sparse index with this relation exists
            d = {}
        else:       #sparse index of this relation exists but not on this join column
            d = rel_sparse_index_dict[relName]
        d[col_name] = new_index
        sparse_indexes[relName] = d
        
    else:
        reldf = sorted_relations[relName][col_name]    
        
    return reldf            

def get_filtered_col_from_primary_relation(foreign_col_data, begin, end):
    temp = foreign_col_data[(foreign_col_data >= begin) & (foreign_col_data <= end)]
    if temp.size == 0:
        return None, None
    data_filtered_out = np.unique(temp)
    foreign_col_data = foreign_col_data[(foreign_col_data < begin) | (foreign_col_data > end)]
    return data_filtered_out, foreign_col_data
    

def merge_embarassingly_parallel_approach(foreign_to_join, primary_to_join, primary_part_list, foreign_col_list, primary_col_list):
    #print('merge_embarassingly_parallel_approach invoked')
    #foreign_to_join = clientHadoop.persist(foreign_to_join)
    #primary_to_join = clientHadoop.persist(primary_to_join)
    meta = partition_join_func(foreign_to_join._meta_nonempty, primary_to_join._meta_nonempty, foreign_col_list, primary_col_list)
       
    #learned_block_nested_loop_join(foreign_to_join, primary_to_join, foreign_col_list, primary_col_list)
  
    kwargs = dict(
        foreign_col_list=foreign_col_list,
        primary_col_list=primary_col_list
    )
    npartitions = foreign_to_join.npartitions
    token = tokenize(foreign_to_join, primary_to_join, npartitions, None, **kwargs)
    name = "partition-join-" + token
  
    dsk = {
        (name, i): (apply, partition_join_func, [(foreign_to_join._name, i), (primary_to_join._name, primary_part_list[i])], kwargs)
        for i in range(npartitions)
    }
        
    divisions = [None] * (npartitions + 1)
    graph = HighLevelGraph.from_collections(name, dsk, dependencies=[foreign_to_join, primary_to_join])
    results =  new_dd_object(graph, name, meta, divisions)  
      
    return results
    

def merge_relation_with_single_partition(single_partition_ddf, other_ddf, foreign_col_list, primary_col_list, reverse=False):
    print("merge_relation_with_single_partition")
    
    single_partition_ddf = clientHadoop.persist(single_partition_ddf)

#     if other_ddf.npartitions > 1 :
#         #other_ddf, size = cull_empty_partitions_new(other_ddf)
#         foreign_part_list = np.array(other_ddf.map_partitions(len).compute())
#         size = sum(foreign_part_list)
#         other_ddf = other_ddf.repartition(npartitions = ceil(size/row_size))    
    other_ddf = clientHadoop.persist(other_ddf)
  
    kwargs = dict(
        foreign_col_list=foreign_col_list,
        primary_col_list=primary_col_list
    )
    npartitions = other_ddf.npartitions
 
    if not reverse:
        meta = partition_join_func(single_partition_ddf._meta_nonempty, other_ddf._meta_nonempty, foreign_col_list, primary_col_list)
        token = tokenize(single_partition_ddf, other_ddf, npartitions, None, **kwargs)
        name = "partition-join-" + token
       
        dsk = {
            (name, i): (apply, partition_join_func, [(single_partition_ddf._name, 0), (other_ddf._name, i)], kwargs)
            for i in range(npartitions)
        }
             
        divisions = [None] * (npartitions + 1)
        graph = HighLevelGraph.from_collections(name, dsk, dependencies=[single_partition_ddf, other_ddf])
 
    else:
        meta = partition_join_func(other_ddf._meta_nonempty, single_partition_ddf._meta_nonempty, foreign_col_list, primary_col_list)   
        token = tokenize(other_ddf, single_partition_ddf, npartitions, None, **kwargs)
        name = "partition-join-" + token
       
        dsk = {
            (name, i): (apply, partition_join_func, [(other_ddf._name, i), (single_partition_ddf._name, 0),], kwargs)
            for i in range(npartitions)
        }
             
        divisions = [None] * (npartitions + 1)
        graph = HighLevelGraph.from_collections(name, dsk, dependencies=[other_ddf, single_partition_ddf])
     
    results =  new_dd_object(graph, name, meta, divisions)  
    temp_data_list.append(single_partition_ddf)
    temp_data_list.append(other_ddf)
  
    return results


def partition_nested_loop_merge(reldf_1, rel_1_join_col_list, reldf_2, rel_2_join_col_list):
    print('partition_nested_loop_merge invoked')
    reldf_1 = clientHadoop.persist(reldf_1)
    reldf_2 = clientHadoop.persist(reldf_2)
    meta = partition_join_func(reldf_2._meta_nonempty, reldf_1._meta_nonempty, rel_2_join_col_list, rel_1_join_col_list)
       
    #learned_block_nested_loop_join(foreign_to_join, primary_to_join, foreign_col_list, primary_col_list)
  
    kwargs = dict(
        foreign_col_list=rel_2_join_col_list,
        primary_col_list=rel_1_join_col_list
    )
    npartitions = reldf_1.npartitions * reldf_2.npartitions
    token = tokenize(reldf_2, reldf_1, npartitions, None, **kwargs)
    name = "partition-nested-loop-join-" + token
    
    reldf_1_partition_list = np.repeat(list(range(reldf_1.npartitions)),reldf_2.npartitions)
    reldf_2_partition_list = np.array(list(range(reldf_2.npartitions)) * reldf_1.npartitions)
  
    dsk = {
        (name, i): (apply, partition_join_func, [(reldf_2._name, reldf_2_partition_list[i]), (reldf_1._name, reldf_1_partition_list[i])], kwargs)
        for i in range(npartitions)
    }
        
    divisions = [None] * (npartitions + 1)
    graph = HighLevelGraph.from_collections(name, dsk, dependencies=[reldf_2, reldf_1])
    results =  new_dd_object(graph, name, meta, divisions)  
      
    return results
    
    


def merge_one_sorted_relations(foreign_df, foreign_col_list, primary_df, primary_col_list, primary_rel_name):
    print('merge_one_sorted_relations() invoked')
    primary_col = primary_col_list[0]
    foreign_col = foreign_col_list[0]
             
    sparse_index = get_sparse_index(primary_rel_name, primary_col)

    #Need to see if this can be avoided
    foreign_df = foreign_df.map_partitions(clean_data_func, foreign_col, sparse_index=sparse_index)

    #if primary_df.npartitions == 1:             
        #results = merge_relation_with_single_partition(primary_df, foreign_df, foreign_col_list, primary_col_list, reverse=True)
        #return results
     
    heaviside = get_heaviside_func(primary_rel_name, primary_col)


    #foreign_col_data = foreign_df[foreign_col]
    meta = foreign_df._meta_nonempty
    meta['Partition'] = 1
    foreign_to_join = foreign_df.map_partitions(partition_info_func_learned, foreign_col, heaviside, len(sparse_index), meta=meta)
    #foreign_to_join = foreign_to_join.set_index('Partition')
    foreign_to_join = clientHadoop.persist(foreign_to_join)
    div = []
    [div.append(i) for i in range(primary_df.npartitions)]
    div.append(primary_df.npartitions-1)
    foreign_to_join = foreign_to_join.set_index('Partition', divisions=div)
    #foreign_to_join = foreign_to_join.repartition(divisions=div)
    foreign_to_join = foreign_to_join.reset_index(drop=True)
    div.pop()
    primary_part_list = div
    results = merge_embarassingly_parallel_approach(foreign_to_join, primary_df, primary_part_list, foreign_col_list, primary_col_list)
    return results            
        

def merge_relations_on_multiple_columns(relName_1, reldf_1, rel_1_join_col_list, relName_2, reldf_2, rel_2_join_col_list):
    
    #if ('merged' not in relName_1):
        #reldf_1 = check_and_build_sparse_index(relName_1, reldf_1, rel_1_join_col_list[0]) 
        #reldf_1 = reldf_1.set_index(rel_1_join_col_list[0])
    
    #if ('merged' not in relName_2):
        #reldf_2 = check_and_build_sparse_index(relName_2, reldf_2, rel_2_join_col_list[0])
        #reldf_2 = reldf_2.set_index(rel_2_join_col_list[0])       

    #reldf_1 = cull_empty_partitions(reldf_1)
    #reldf_2 = cull_empty_partitions(reldf_2)        
    
    if ('merged' in relName_1) or (reldf_1.npartitions > reldf_2.npartitions):
        foreign_df = reldf_1
        primary_df = reldf_2
        primary_rel = relName_2
        primary_col_list = rel_2_join_col_list
        foreign_col_list = rel_1_join_col_list
    else:
        foreign_df = reldf_2
        primary_df = reldf_1
        primary_rel = relName_1
        primary_col_list = rel_1_join_col_list
        foreign_col_list = rel_2_join_col_list
   
    primary_df = check_and_build_sparse_index(primary_rel, primary_df, primary_col_list[0])
    results = merge_one_sorted_relations(foreign_df, foreign_col_list, primary_df, primary_col_list, primary_rel)
    return results

def is_coloumn_unique(relName, join_col_list):
    if len(join_col_list) > 1 or relName not in primary_key.keys():
        return False
    join_col = join_col_list[0]
    return primary_key[relName] == join_col  

def merge_relations_on_unique_columns(primary_rel_name, primary_df, primary_col_list, foreign_df, foreign_col_list):
    print('merge_relations_on_unique_columns() invoked')
    primary_col = primary_col_list[0]
    foreign_col = foreign_col_list[0]
             
    meta = foreign_df._meta_nonempty
    meta['Partition'] = 1
    foreign_to_join = foreign_df.map_partitions(partition_info_func_bloom, foreign_col, primary_rel_name, hdfs_node, hdfs_port, bloom_matrix_params[primary_rel_name], meta=meta)
    foreign_to_join = clientHadoop.persist(foreign_to_join)
    div = []
    [div.append(i) for i in range(primary_df.npartitions)]
    div.append(primary_df.npartitions-1)
    foreign_to_join = foreign_to_join.set_index('Partition', divisions=div)
    foreign_to_join = foreign_to_join.reset_index(drop=True)
    div.pop()
    #foreign_to_join = rearrange_by_column(foreign_to_join, 'Partition', npartitions=primary_df.npartitions, max_branch=32)
    primary_part_list = div
    results = merge_embarassingly_parallel_approach(foreign_to_join, primary_df, primary_part_list, foreign_col_list, primary_col_list)
    #temp_data_list.append(primary_df)
    #temp_data_list.append(foreign_to_join)
    return results            


def merge_tables(relName_1, reldf_1, rel_1_join_col_list, relName_2, reldf_2, rel_2_join_col_list):
    
    print("\nmerge_tables")
    print(relName_1 + " : " + str(rel_1_join_col_list) + " , " + relName_2 + " : " + str(rel_2_join_col_list))

    #    rel1: can be a temporary relation or a relation from the schema
    #    rel2: always a relation schema from the database  
    if len(rel_1_join_col_list) != len(rel_2_join_col_list):
        raise ValueError("Relations cannot be joined. Count of join columns are not equal")

    elif (reldf_1.npartitions == 1 or reldf_2.npartitions == 1):
        if reldf_1.npartitions == 1:
            results = merge_relation_with_single_partition(reldf_1, reldf_2, rel_2_join_col_list, rel_1_join_col_list, reverse=True)
        else:
            results = merge_relation_with_single_partition(reldf_2, reldf_1, rel_1_join_col_list, rel_2_join_col_list, reverse=True)
        return results

            
    elif can_merge_with_learned_index(relName_1, reldf_1, rel_1_join_col_list, relName_2, reldf_2, rel_2_join_col_list):
#         if is_coloumn_unique(relName_1, rel_1_join_col_list):
#             foreign_df = reldf_2
#             primary_df = reldf_1
#             primary_rel = relName_1
#             primary_col_list = rel_1_join_col_list
#             foreign_col_list = rel_2_join_col_list
#         else:
#             foreign_df = reldf_1
#             primary_df = reldf_2
#             primary_rel = relName_2
#             primary_col_list = rel_2_join_col_list
#             foreign_col_list = rel_1_join_col_list
         
        #merged_relation = merge_relations_on_unique_columns(primary_rel, primary_df, primary_col_list, foreign_df, foreign_col_list)    
        merged_relation = merge_relations_on_multiple_columns(relName_1, reldf_1, rel_1_join_col_list, relName_2, reldf_2, rel_2_join_col_list)
            
    else:
        merged_relation = partition_nested_loop_merge(reldf_1, rel_1_join_col_list, reldf_2, rel_2_join_col_list)
        #print("Dask merge invoked")
        #merged_relation = reldf_1.merge(reldf_2, left_on=rel_1_join_col_list, right_on=rel_2_join_col_list)
    return merged_relation

