import pandas as pd
from distributed import get_client
import numpy as np
import dask.dataframe as dd
import DaskDB.distpartd as distpartd
from distributed.client import wait
from timeit import default_timer as timer

posBegin = 0
posEnd = 1
posPartitionList  = 2

clustered_sparse_index = {}
sparse_index_with_partition_list = {}
heaviside_func = {}

hdfs_node='192.168.100.5'
hdfs_port=8020
storage_opts = {'host': hdfs_node, 'port': hdfs_port, 'driver':'libhdfs3'}
print storage_opts


p = distpartd.PandasColumns('hdfs:///tempDir', str(hdfs_node), hdfs_port)
clientHadoop = get_client()

def get_default_chunksize():
    import dask
    chunksize = dask.config.config['array']['chunk-size'] #Returns chunksize in string like '128MiB'
    return dask.utils.parse_bytes(chunksize)

def read_data(relNo, merge_col_name, num_of_rows, end):
    #print "dd.read_csv('hdfs:///input/datasets_for_dask_DB/sanzu/data1_"+num_of_rows+".csv',delimiter=',',"+storage_opts+")"
    print 'read_data() invoked : File = ' + 'hdfs:///input/datasets_for_dask_DB/sanzu/data' + str(relNo) +'_'+str(num_of_rows)+end+'.csv'
    # exec( "df = dd.read_csv('hdfs:///data/data1_"+num_of_rows+".csv',delimiter=',',"+storage_opts+")" )
    # df = dd.read_csv('hdfs:///data/data1_'+num_of_rows+'.csv',delimiter=',', storage_options=storage_opts)    
    df = dd.read_csv('hdfs:///input/datasets_for_dask_DB/sanzu/data' + str(relNo) +'_'+str(num_of_rows)+end+'.csv',delimiter=',',storage_options=storage_opts )
    #return df
    col_list = list(df.columns)
    merge_col_pos = col_list.index(merge_col_name)
    numPartition = df.npartitions
    
    relName = 'data' + str(relNo) +'_' + num_of_rows

    futures = []
    for i in range(numPartition):
        rel_pd = df.get_partition(i).compute()
        #p.append({relName + '-' + str(i):rel_pd})
        future = clientHadoop.scatter(rel_pd)
        futures.append(future)
        create_clustered_sparse_index(relName, merge_col_pos, rel_pd, i)
    
    create_new_index(relName)
    
#    df = None
#    for i in range(numPartition):
#        rel_pd = p.get(relName + '-' + str(i))
#         if df is None:
#             df = dd.from_pandas(rel_pd, chunksize=get_default_chunksize())
#         else:
#             df = df.append(dd.from_pandas(rel_pd, chunksize=get_default_chunksize()), interleave_partitions=True)
#        future = client.scatter(rel_pd)
#        futures.append(future)
        
    df =  dd.from_delayed(futures, meta=rel_pd)
    #df = df.repartition(npartitions=numPartition).persist()
    #wait(df)
    #clientHadoop.rebalance(df)    
    return df,relName

def create_heaviside_function(relName, pdf):
    
    #this function returns the position in the pandas dataframe which contains the key
    def create_heaviside(x):
        y = 0
        for i in range(len(pdf)):
            begin = pdf.iat[i,posBegin]
            end = pdf.iat[i,posEnd]
            y += np.heaviside((end - x)*(x - begin), 1) * i
        return int(y)
    
    heaviside_func[relName] = create_heaviside

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
        
    sparse_index_with_partition_list[relName] = new_index
    create_heaviside_function(relName, new_index)                    

def add_row_to_sparse_index(pdf,col, partition):
    begin = pdf.iat[0,col]
    end = pdf.iat[-1,col]
    df = pd.DataFrame([[begin,end,partition]], columns = ['Begin','End','Partition'])
    return df

def create_clustered_sparse_index(relName, merge_col_pos, relPDF, partition):
    sparse_index = clustered_sparse_index.get(relName)
    if sparse_index is None:
        sparse_index = add_row_to_sparse_index(relPDF, merge_col_pos, partition)
    else:
        sparse_index = sparse_index.append(add_row_to_sparse_index(relPDF, merge_col_pos, partition))
    clustered_sparse_index[relName] = sparse_index

def create_new_index(relName):
    sparse_index_pdf = clustered_sparse_index.get(relName)
    create_new_index_with_partition_list(relName, sparse_index_pdf)

def cull_empty_partitions(df):
    ll = list(df.map_partitions(len).compute())
    if 0 in ll:
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
    
def get_end_key_single_relation(relName, numPartition, start_key, last_partition_num_fetched):
    end_key = -1
    if (last_partition_num_fetched + 1) < numPartition:
        heaviside = heaviside_func[relName]
        index = sparse_index_with_partition_list.get(relName)
        sparse_index = clustered_sparse_index.get(relName)
        line_no_in_index = heaviside(start_key)
        partition_list = list(index.iat[line_no_in_index,posPartitionList])

        begin = partition_list[0]
        if len(partition_list) == 1:
            end = partition_list[0]
        else: #if partition list contains two elements i.e. (begin,end)
            end = partition_list[1]    

        for i in range(begin,end+1):
            if i <= last_partition_num_fetched:
                continue
            last_partition_num_fetched = end
            break
        
        end_key = sparse_index.iat[last_partition_num_fetched,1]    #part_no is equal to row_no
    return last_partition_num_fetched, end_key

def filter_both_relation(relName1, relName2, rel_ddf1, rel_ddf2, merge_col_name, merged_temp):
    
    last_partition_fetched_1 = -1
    last_partition_fetched_2 = -1
    key_col_pos_in_merged_index = 0;
    copy = merged_temp
    

    temp_df_1 = None
    temp_df_2 = None 
    while True:
        copy = cull_empty_partitions(copy)
        if copy.npartitions is 0:
            break 
        #first_partition = copy.get_partition(0).compute()
        first_row_of_merged_rel = copy.head(1)
        #key_value = first_partition.iat[0, key_col_pos_in_merged_index]
        key_value = first_row_of_merged_rel.iat[0, key_col_pos_in_merged_index]
        last_partition_fetched_1, end_1 = get_end_key_single_relation(relName1, rel_ddf1.npartitions, key_value, last_partition_fetched_1)
        last_partition_fetched_2, end_2 = get_end_key_single_relation(relName2, rel_ddf2.npartitions, key_value, last_partition_fetched_2)
        max_end = max(end_1, end_2) 
        merged_partition = copy[(copy[merge_col_name] >= key_value) & (copy[merge_col_name] <= max_end)]
        merged_partition = cull_empty_partitions(merged_partition)
        last_row_of_merged_partition = merged_partition.tail(1)
        copy = copy[copy[merge_col_name] > max_end]
        last_key = last_row_of_merged_partition.iat[0,key_col_pos_in_merged_index]
        x = rel_ddf1[(rel_ddf1[merge_col_name] >= key_value) & (rel_ddf1[merge_col_name] <= last_key)]
        #sx = cull_empty_partitions(x)
        if temp_df_1 is None:
            temp_df_1 = x
        else:               
            temp_df_1 = temp_df_1.append(x, interleave_partitions=True)
            
        x = rel_ddf2[(rel_ddf2[merge_col_name] >= key_value) & (rel_ddf2[merge_col_name] <= last_key)]
        #sx = cull_empty_partitions(x)
        if temp_df_2 is None:
            temp_df_2 = x
        else:               
            temp_df_2 = temp_df_2.append(x, interleave_partitions=True)
        #print 'start_key : ' + str(key_value)
        #print 'last_key : ' + str(last_key)
        #print x.compute()        
    #rel =  dd.from_delayed(futures, meta=pandadf)
    #rel = rel.set_index(merge_col_name, sorted = True)
    #rel = clientHadoop.persist(rel)
    
#     for i in rel_partition_list: #the partitions in the list are not required and hence can be removed from the dataframe
#         begin = sparse_index.iat[i,0]
#         end = sparse_index.iat[i,1]
#         rel_ddf = rel_ddf[(rel_ddf[merge_col_name] < begin) & (rel_ddf[merge_col_name] > end)]
#     rel_ddf = cull_empty_partitions(rel_ddf)
    temp_df_1 = cull_empty_partitions(temp_df_1)
    temp_df_2 = cull_empty_partitions(temp_df_2)
    temp_df_1 = temp_df_1.set_index(merge_col_name, sorted=True)
    temp_df_2 = temp_df_2.set_index(merge_col_name, sorted=True)
    rel_ddf1 = temp_df_1
    rel_ddf2 = temp_df_2
    #rel_ddf = rel_ddf.set_index(merge_col_name, sorted=True)
    rel_ddf1 = clientHadoop.persist(rel_ddf1)
    rel_ddf2 = clientHadoop.persist(rel_ddf2)
    return rel_ddf1, rel_ddf2    
    

def merge_relations_on_single_columns(relName_1, reldf_1, rel_1_join_col_name, relName_2, reldf_2, rel_2_join_col_name):    
    
    print 'merge_relations_on_single_columns() invoked'
    start = timer()
    reldf_1_join_col = reldf_1[[rel_1_join_col_name]]
    reldf_2_join_col = reldf_2[[rel_2_join_col_name]]
    
    reldf_1_join_col = reldf_1_join_col.set_index(rel_1_join_col_name, sorted = True)
    reldf_2_join_col = reldf_2_join_col.set_index(rel_2_join_col_name, sorted = True, drop=False)
    merged_rel = reldf_1_join_col.merge(reldf_2_join_col, left_index=True, right_index=True)
    merged_rel = clientHadoop.persist(merged_rel)
    #print merged_rel.compute()
    
    reldf_1_new, reldf_2_new = filter_both_relation(relName_1, relName_2, reldf_1, reldf_2, rel_1_join_col_name, merged_rel)
    #clientHadoop.cancel(reldf_1)
    #clientHadoop.cancel(reldf_2)
    #clientHadoop.cancel(merged_rel)
    merged_rel = reldf_1_new.merge(reldf_2_new, left_index=True, right_index=True)
    #merged_rel = reldf_1.merge(merged_rel, left_on='rand1', right_index=True)    
    merged_rel.compute()
    #merged_rel = merged_rel.merge(reldf_2, left_index = True, right_index = True)
    
    #print merged_rel.compute()
    
    end = timer()
    return start, end

# def test_code():
#     data = [[1,5,1], [5,8,2], [8,8,3], [8,8,4], [8,10,5], [11,12,6], [13,23,7], [28,30,8], [30,30,9], [30,35,10]]
#     df = pd.DataFrame(data, columns = ['Begin', 'End', 'Partition'])
#     index = create_new_index_with_partition_list(df)
#     print index
    
#test_code()    

                            
