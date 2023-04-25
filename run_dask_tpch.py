from timeit import default_timer as timer
import sys
import time
from DaskDB import query as qr
from DaskDB import dask_physical_plan as dpp
from DaskDB import dask_plan as dp
from DaskDB import dask_learned_index as dli

from DaskDB.setup_configuration import get_dask_scheduler_IP, get_dask_scheduler_port
import pandas as pd
import numpy as np
from dask.distributed import Client

import matplotlib.pyplot as plt

from sklearn.linear_model import LinearRegression
from sklearn.cluster import KMeans
from scipy import optimize
from math import ceil
from builtins import dir
import sys


scheduler_ip_port = get_dask_scheduler_IP() + ':' + str(get_dask_scheduler_port())
client = Client(scheduler_ip_port)
client.restart()
client.upload_file('mydistpartd.egg')
#client.upload_file('/home/suvam/eclipse-workspace/LearnedDaskDB_2/learned_DaskDB/DaskDB/BloomMatrix.py')

output = ""
da_pl = dp.DaskPlan(client)
dli.setDaskClient(client)

#client.get_versions(check=True)

# import os
# print (os.getcwd())
# print (client.run(os.getcwd))
# 
# client.get_versions(check=False)

#client.upload_file('DaskDB/dask_learned_index.py')
#da_pl.init(client)

relation_list = None

def get_default_chunksize():
    import dask
    chunksize = dask.config.config['array']['chunk-size'] #Returns chunksize in string like '128MiB'
    return dask.utils.parse_bytes(chunksize)

#def partition_join(foreign_part, foreign_col_list, primary_ddf, primary_col_list):
def partition_join(foreign_part, primary_ddf, foreign_col_list=None, primary_col_list=None):
    from pandas._libs.algos import groupsort_indexer
    from pandas._libs import hashtable as libhashtable


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

        klass = libhashtable.Int64Factorizer
        rizer = klass(max(len(lk), len(rk)))

        llab = rizer.factorize(lk)
        rlab = rizer.factorize(rk)

        count = rizer.get_count()
        uniques = rizer.uniques.to_array()
        if sort:
            uniques, llab, rlab = _sort_labels(uniques, llab, rlab)

        return llab, rlab, count, uniques


    #rel = foreign_part.merge(primary_ddf, left_index = True, right_index = True)
    #return rel

    #foreign_part = foreign_part.compute()
    #primary_ddf = primary_ddf.compute()


    foreign_part = foreign_part.copy()
    primary_ddf = primary_ddf.copy()

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

    foreign_part['index'] = lkey
    foreign_part.set_index('index', inplace=True)
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

#     for i in right_ddf.columns:
#         left_ddf[i] = right_ddf[i]
    left_ddf = pd.concat([left_ddf, right_ddf], axis=1)

    return left_ddf

def filter_primary_partition_for_both_sorted_relation(foreign_df, foreign_col_list, primary_col_list, primary_df=None):
    primary_col = primary_col_list[0]
    foreign_col = foreign_col_list[0]
    key_value_first = foreign_df[foreign_col].iloc[0]
    key_value_last = foreign_df[foreign_col].iloc[-1]
    primary_part = primary_df[(primary_df[primary_col] >= key_value_first)&(primary_df[primary_col] <= key_value_last)]
    results = partition_join(foreign_df, foreign_col_list, primary_part, primary_col_list)
    return results


def filter_primary_partition_for_one_sorted_relation(foreign_df, foreign_col_list, primary_col_list, heaviside, primary_df=None, sparse_index=None):
    import numpy as np
    primary_col = primary_col_list[0]

    foreign_part = foreign_df
    primary_part = primary_df
    foreign_col_data = foreign_part[foreign_col_list[0]]
    primary_split_ddf = None

    while not foreign_col_data.empty:
        key_value = foreign_col_data.head(1)
        line_no_in_index = heaviside(key_value)
        begin = sparse_index.iat[line_no_in_index,0]
        end = sparse_index.iat[line_no_in_index,1]
        partition_list = list(sparse_index.iat[line_no_in_index,2])
        primary_part_begin = partition_list[0]
        primary_part_end = partition_list[-1]
        temp = foreign_col_data[(foreign_col_data >= begin) & (foreign_col_data <= end)]
        if temp.empty:
            break
        temp = list(temp)
        data_filtered_out = np.unique(temp)
        data_filtered_out = np.intersect1d(primary_part.index, data_filtered_out)
        x = primary_part.loc[data_filtered_out]
        if primary_split_ddf is None:
            primary_split_ddf = x
        else:
            primary_split_ddf = primary_split_ddf.append(x)

        foreign_col_data = foreign_col_data[(foreign_col_data < begin) | (foreign_col_data > end)]

    results = partition_join(foreign_part, foreign_col_list, primary_split_ddf, primary_col_list)
    return results


def get_partition_info_bloom(foreign_df, foreign_col, primary_rel_name, hdfs_node, hdfs_port, bloom_matrix_params):


    import distpartd
    import os
    import importlib

    print("get_partition_info_bloom invoked")
    bloom_matrix_module = importlib.import_module('bloom_matrix_{}'.format(primary_rel_name))

    dir_list = os.listdir('/tmp/dask/dask-worker-space/')
    for dir in dir_list:
        if 'worker' in dir and 'dirlock' not in dir:
            path = '/tmp/dask/dask-worker-space/' + dir + '/'
            break
    #print(path)
    #print(bloom_matrix_params)
    dist = distpartd.Numpy('hdfs:///tempDir', hdfs_node, hdfs_port, 'r')
    bm = bloom_matrix_module.BloomMatrix(*bloom_matrix_params)
    bm.read_from_binary_file(path, primary_rel_name)

    new_df = foreign_df.copy()
    data = new_df[foreign_col].values
    part_info = bm.check(data.astype(np.double))
    multiple_part_info = bm.get_multiple_partition_info()
    del bm
    for lst in multiple_part_info:
        if len(lst) == 0:
            continue
        val = lst[0]
        pos = np.where(data==val)[0]
        if part_info[pos[0]] != -1:
            continue
        for part in lst[2:]:
            key_values = dist.get(primary_rel_name + '_part' + str(part))
            if val in key_values:
                part_info[pos] = part
                break
    new_df['Partition'] = part_info

    del part_info, data, multiple_part_info

    new_df = new_df[new_df['Partition'] != -1]
    return new_df


def get_partition_info_learned_index(foreign_df, foreign_col, heaviside_func, num_entries):
    print("get_partition_info_learned_index invoked")
    new_df = foreign_df.copy()
    data = new_df[foreign_col]
    data2 = np.transpose(np.tile(data.values,(num_entries,1)))
    d = heaviside_func(data2)
    #d = pd.to_datetime(0) + pd.to_timedelta(d, unit='D')
    new_df['Partition'] = d
    #new_df.set_index('Partition', inplace=True)
    return new_df



def filter_foreign_partition(primary_data, heaviside_func, foreign_df=None):
    keyValue = int(primary_data.values[0])
    line_no = heaviside_func(keyValue)
    foreign_to_join = foreign_df[foreign_df['Partition'] == line_no]
    return foreign_to_join


def partition_count_func(pdf, row_size):
    size = len(pdf)
    numPartitions = ceil(size/row_size)
    return numPartitions

def clean_foreign_data(foreign_pdf, foreign_col, sparse_index=None):
    filtered_data = pd.DataFrame()
    for i in range(len(sparse_index)):
        begin = sparse_index.iat[i,0]
        end = sparse_index.iat[i,1]
        data = foreign_pdf[(foreign_pdf[foreign_col] >= begin) & (foreign_pdf[foreign_col] <= end)]
        filtered_data = filtered_data.append(data)
    return filtered_data


dli.set_map_partition_func(filter_primary_partition_for_both_sorted_relation, filter_primary_partition_for_one_sorted_relation, partition_join)
dli.set_partition_info_func(get_partition_info_learned_index, get_partition_info_bloom)
dli.set_filter_foreign_partition_func(filter_foreign_partition)
dli.set_clean_data_func(clean_foreign_data)

udf_list = []
def query(sql,scale_factor='1'):
    plan,used_columns=qr.getPlan(sql, udf_list)
    #print "logical plan:", plan
    phys = dpp.PhysicalPlan()
    #print "physical plan:", phys
    dask_plan = phys.init(plan)
    #print "dask plan:", dask_plan
    da_pl.only_use_columns(used_columns,scale_factor)
    df = da_pl.convert_to_dask_code(dask_plan)
    #dli.clear_data_from_workers()
    return df

def register_udf(func, param_count_list):
    da_pl.register_udf(func, param_count_list)
    udf_list.append(func.__name__)

def unregister_all_udf():
    da_pl.unregister_all_udf()
    udf_list.clear()


# def myLinearFit(s1,s2):
#     model = LinearRegression().fit(np.array(s1).reshape(-1,1), s2)
#     pred = model.predict(np.array(s1).reshape(-1,1))
#     plt.scatter(s1, s2, color='black')
#     plt.plot(s1,pred,color='blue', linewidth=3)
#     plt.xlabel(list(s1.columns)[0])
#     plt.ylabel(list(s2.columns)[0])
#     plt.title("Plot for Linear Regression")
#     plt.show()    


def myLinearFit(s1:pd.Series, s2: pd.Series)->float:
    import statsmodels.api as sm
    from statsmodels.tools.eval_measures import rmse

    log_reg = sm.Logit(s1, s2).fit()
    pred = log_reg.predict(s1)
    return rmse(s2.iloc[:,0], pred)


def myKNN(df1, df2):
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import StandardScaler
    from sklearn.neighbors import KNeighborsClassifier
    from sklearn.metrics import confusion_matrix

    X_train, X_test, y_train, y_test = train_test_split(df1, df2, test_size = 0.25, random_state = 0)
    sc = StandardScaler()
    X_train = sc.fit_transform(X_train)
    X_test = sc.transform(X_test)
    classifier = KNeighborsClassifier(n_neighbors = 2)
    classifier.fit(X_train, y_train)
    y_pred = classifier.predict(X_test)
    print(y_test)
    print(y_pred)
    cm = confusion_matrix(y_test, y_pred)
    return cm


def myKMeans(df):
    kmeans = KMeans(n_clusters=4).fit(df)
    #centroids = kmeans.cluster_centers_

    col1 = list(df.columns)[0]
    col2 = list(df.columns)[1]
    plt.scatter(df[col1], df[col2], c= kmeans.labels_.astype(float), s=50, alpha=0.5)
    #plt.scatter(centroids[:, 0], centroids[:, 1], c='red', s=50)
    plt.xlabel(col1)
    plt.ylabel(col2)
    #plt.title("Plot for K-Means")
    #plt.show()


# def myKMeans(s1:pd.Series, s2: pd.Series)->float:
#     data = pd.concat([s1, s2], axis=1)
#     kmeans = KMeans(n_clusters=4).fit(data)
#     return kmeans.inertia_

def myQuantile(s:pd.Series)->float:
    return s.quantile(.25)


def myConjugateGradOpt(s1:pd.Series, s2: pd.Series)->float:
    #seek the minimum value of the expression a*u**2 + b*u*v + c*v**2 + d*u + e*v + f
    #for given values of the parameters and an initial guess (u, v) = (df[0,df[1])

    args = (2, 3, 7, 8, 9, 10)  # parameter values

    def f(x, *args):
        u, v = x
        a, b, c, d, e, f = args
        return a*u**2 + b*u*v + c*v**2 + d*u + e*v + f

    def gradf(x, *args):
        u, v = x
        a, b, c, d, e, f = args
        gu = 2*a*u + b*v + d     # u-component of the gradient
        gv = b*u + 2*c*v + e     # v-component of the gradient
        return np.asarray((gu, gv))

    x0 = (s1.iat[0,0],s2.iat[0,0])
    val = optimize.fmin_cg(f, x0, fprime=gradf, args=args, full_output=True)
    return val[1]


def impl_linear_reg(scale_factor='1'):
    register_udf(myLinearFit,[1,1])    #the UDF's needs to be registered for invocation from DaskDB

    sql_lin = """select
        myLinearFit(l_discount, l_tax)
    from
        lineitem
    where
        l_orderkey < 10
    limit 50;
        """

    q = query(sql_lin, scale_factor)
    print()
    print(q)
    unregister_all_udf()


def impl_kmeans(scale_factor='1'):
    register_udf(myKMeans,[2])
    sql_kmeans = """select myKMeans(l_discount, l_tax)
    from
        lineitem
    where
        l_orderkey < 50
        limit 50;
        """

    q = query(sql_kmeans, scale_factor)
    print()
    print(q)
    unregister_all_udf()


def impl_quantile(scale_factor='1'):
    register_udf(myQuantile,[1])
    sql_quantile = """select myQuantile(l_discount)
    from
        lineitem,orders
    where
        l_orderkey = o_orderkey
        limit 50;
        """

    q = query(sql_quantile, scale_factor)
    print()
    print(q)
    unregister_all_udf()


def impl_conjugate_grad_opt(scale_factor='1'):
    register_udf(myConjugateGradOpt, [1,1])

    sql_cgo = """select
        myConjugateGradOpt(l_discount, l_tax)
    from
        lineitem
    where
        l_orderkey < 10
    limit 1;
        """

    res = query(sql_cgo, scale_factor)
    print()
    print(res)
    unregister_all_udf()


sql_merge = """select * from lineitem,orders where l_orderkey = o_orderkey order by l_extendedprice  limit 10;"""

sql_merge = """select * from lineitem,orders where l_orderkey = o_orderkey order by l_extendedprice  limit 10;"""
sql1="""select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= date '1998-12-01' - interval '117' day
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus
limit 1;
	"""
sql3 = """select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from
    orders,
    customer,
    lineitem
where
    c_mktsegment = 'HOUSEHOLD'
    and c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate < date '1995-03-21'
    and l_shipdate > date '1995-03-21'
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate
limit 10; """

sql5="""select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    customer,
    orders,
    lineitem,
    supplier, 
    nation,
    region
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and l_suppkey = s_suppkey
    and c_nationkey = s_nationkey
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'AMERICA'
    and o_orderdate >= date '1995-01-01'
    and o_orderdate < date '1995-01-01' + interval '1' year
group by
    n_name
order by
    revenue desc
limit 1;"""


sql5a = """select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
    	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'AMERICA'
    	and o_orderdate >= date '1995-01-01'
    	and o_orderdate < date '1995-01-01' + interval '1' year
group by
    n_name
order by
	revenue desc
limit 5;"""

sql6= """select
    l_discount,
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
    l_shipdate >= date '1995-01-01'
    and l_shipdate < date '1995-01-01' + interval '1' year
    and ((l_discount >= (0.08)) and (l_discount <= (1)))
    and l_quantity < 24
limit 1;"""

sql10 = """select
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    customer,
    orders,
    lineitem,
    nation
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate >= date '1995-01-01'
    and o_orderdate < date '1995-01-01' + interval '3' month
    and l_returnflag = 'R'
    and c_nationkey = n_nationkey
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc
limit 20;"""

sql1a = """select
	l_orderkey,
	l_shipdate,
	c_custkey
from
	customer,
	orders,
	lineitem
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
limit 10;"""

sql3a = """select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc
limit 10;"""

sql5a = """select
	l_extendedprice,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	orders,
	lineitem
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
order by
	revenue desc
limit 5;"""

sql_test = """select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	orders,
	lineitem
where
	l_orderkey = o_orderkey
	and o_orderdate >= date '1995-01-01'
group by
	l_orderkey
order by
	revenue
limit 5;"""

sql_test_2 = """select o_orderkey, o_custkey
from
    orders
where
    o_custkey < 50;
    """

sql_rem = """select
    LinearRegression(o_orderkey, o_totalprice)
from
    orders
where
    o_custkey < 50;
    """

sql_benchmark="""select
    c_name
from
    customer,
    orders
where
    c_custkey = o_custkey
    and o_orderdate >= date '1995-01-01'
    and o_orderdate < date '1995-01-01' + interval '1' year
group by
    c_name
limit 1;"""

sql105_iter_1 = """WITH recursive cte_paths (cte_src, cte_target, cte_distance, cte_lvl) AS
(
       SELECT src AS cte_src,
              target AS cte_target,
              distance AS cte_distance,
              1 AS cte_lvl
       FROM   distances
       WHERE  src = 1
       UNION
       SELECT src AS cte_src, 
              target AS cte_target,
              cte_distance + distance AS cte_distance,
              cte_lvl + 1 AS cte_lvl
       FROM   cte_paths,
              distances
       WHERE  cte_target = src
       AND    cte_lvl < 10)
SELECT   cte_src AS through,
         cte_target,
         cte_distance,
         cte_lvl + 1 AS number_of_nodes
FROM     cte_paths
WHERE    cte_target = 5
ORDER BY cte_distance ASC limit 1;
"""

sql106_iter_2 = """WITH recursive cte_customer_tree (cte_custkey, cte_customer_name, cte_segment, cte_revenue, cte_lvl) AS
(
       SELECT c_custkey AS cte_custkey,
              c_name AS cte_customer_name,
              c_mktsegment AS cte_segment,
              o_totalprice AS cte_revenue,
              1 AS cte_lvl
       FROM   customer,
              orders
       WHERE  c_custkey = o_custkey
       AND    c_mktsegment = 'BUILDING'
       AND    c_custkey = 1
       UNION
       SELECT c_custkey AS cte_custkey,
              c_name AS cte_customer_name,
              c_mktsegment AS cte_segment,
              cte_revenue + o_totalprice AS cte_revenue,
              cte_lvl + 1 AS cte_lvl
       FROM   customer,
              orders,
              cte_customer_tree
       WHERE  c_custkey = o_custkey
       AND    c_custkey = cte_custkey)
SELECT   cte_custkey,
         cte_customer_name,
         cte_segment,
         sum(cte_revenue) AS total_revenue
FROM     cte_customer_tree
GROUP BY cte_custkey,
         cte_customer_name,
         cte_segment;"""



# jsonviewer http://jsonviewer.stack.hu/

#impl_linear_reg()
#impl_kmeans()
#impl_quantile()
#impl_conjugate_grad_opt()

query_number =  int(sys.argv[1])
scale_factor = sys.argv[2]
number_or_runs = int(sys.argv[3])
output = 'query_number : '+ str(query_number) + ' scale_factor : ' + scale_factor + ' number_or_runs : ' + str(number_or_runs) + '\n'
result = ""
total_time = 0


start1 = time.time()


first = True
if query_number == 0:
    for i in range(number_or_runs):
        start = timer()
        print(query(sql_rem,scale_factor))
        first = False
        end = timer()
        result += str(end-start) + "\n"
        print ("Run " + str(i+1) + " : " + str(end-start))
        if i > 0:
            total_time += (end-start)
    number_or_runs = number_or_runs - 1 if number_or_runs > 1 else 1
    result += "Avg of warm runs : " + str(total_time / number_or_runs) + "\n\n"
elif query_number == 1:
    for i in range(number_or_runs):
        start = timer()
        print(query(sql_test,scale_factor))
        first = False
        end = timer()
        result += str(end-start) + "\n"
        print ("Run " + str(i+1) + " : " + str(end-start))
        if i > 0:
            total_time += (end-start)
    number_or_runs = number_or_runs - 1 if number_or_runs > 1 else 1
    result += "Avg of warm runs : " + str(total_time/number_or_runs) + "\n\n"
elif query_number == 3:
    for i in range(number_or_runs):
        start = timer()
        #print(query(sql3,scale_factor))
        print(query(sql3,scale_factor))
        first = False
        end = timer()
        result += str(end-start) + "\n"
        print ("Run " + str(i+1) + " : " + str(end-start))
        if i > 0:
            total_time += (end-start)
    number_or_runs = number_or_runs - 1 if number_or_runs > 1 else 1
    result += "Avg of warm runs : " + str(total_time / number_or_runs) + "\n\n"
elif query_number == 5:
    for i in range(number_or_runs):
        start = timer()
        print(output)
        print(query(sql5,scale_factor))
        first = False
        end = timer()
        result += str(end-start) + "\n"
        print ("Run " + str(i+1) + " : " + str(end-start))
        if i > 0:
            total_time += (end-start)
    number_or_runs = number_or_runs - 1 if number_or_runs > 1 else 1
    result += "Avg of warm runs : " + str(total_time / number_or_runs) + "\n\n"
elif query_number == 6:
    for i in range(number_or_runs):
        start = timer()
        print(query(sql6,scale_factor))
        first = False
        end = timer()
        result += str(end-start) + "\n"
        print ("Run " + str(i+1) + " : " + str(end-start))
        if i > 0:
            total_time += (end-start)
    number_or_runs = number_or_runs - 1 if number_or_runs > 1 else 1
    result += "Avg of warm runs : " + str(total_time / number_or_runs) + "\n\n"
elif query_number == 10:
    for i in range(number_or_runs):
        start = timer()
        print(query(sql10,scale_factor))
        first = False
        end = timer()
        result += str(end-start) + "\n"
        print ("Run " + str(i+1) + " : " + str(end-start))
        if i > 0:
            total_time += (end-start)
    number_or_runs = number_or_runs - 1 if number_or_runs > 1 else 1
    result += "Avg of warm runs : " + str(total_time / number_or_runs) + "\n\n"
elif query_number == 11:
    for i in range(number_or_runs):
        start = timer()
        query(sql1a,scale_factor)
        first = False
        end = timer()
        result += str(end-start) + "\n"
        print ("Run " + str(i+1) + " : " + str(end-start))
        if i > 0:
            total_time += (end-start)
    number_or_runs = number_or_runs - 1 if number_or_runs > 1 else 1
    result += "Avg of warm runs : " + str(total_time / number_or_runs) + "\n\n"
elif query_number == 33:
    for i in range(number_or_runs):
        start = timer()
        query(sql3a,scale_factor)
        first = False
        end = timer()
        result += str(end-start) + "\n"
        print ("Run " + str(i+1) + " : " + str(end-start))
        if i > 0:
            total_time += (end-start)
    number_or_runs = number_or_runs - 1 if number_or_runs > 1 else 1
    result += "Avg of warm runs : " + str(total_time / number_or_runs) + "\n\n"
elif query_number == 55:
    for i in range(number_or_runs):
        start = timer()
        print(query(sql5a,scale_factor))
        first = False
        end = timer()
        result += str(end-start) + "\n"
        print ("Run " + str(i+1) + " : " + str(end-start))
        if i > 0:
            total_time += (end-start)
    number_or_runs = number_or_runs - 1 if number_or_runs > 1 else 1
    result += "Avg of warm runs : " + str(total_time / number_or_runs) + "\n\n"
elif query_number == 56:
    for i in range(number_or_runs):
        print('Executing benchmark query')
        start = timer()
        print(query(sql_benchmark,scale_factor))
        first = False
        end = timer()
        result += str(end-start) + "\n"
        print ("Run " + str(i+1) + " : " + str(end-start))
        if i > 0:
            total_time += (end-start)
    number_or_runs = number_or_runs - 1 if number_or_runs > 1 else 1
    result += "Avg of warm runs : " + str(total_time / number_or_runs) + "\n\n"
elif query_number == 101:
    result += "Linear Regression\n"
    for i in range(number_or_runs):
        start = timer()
        print(impl_linear_reg(scale_factor))
        first = False
        end = timer()
        result += str(end-start) + "\n"
        print ("Run " + str(i+1) + " : " + str(end-start))
        if i > 0:
            total_time += (end-start)
    number_or_runs = number_or_runs - 1 if number_or_runs > 1 else 1
    result += "Avg of warm runs : " + str(total_time / number_or_runs) + "\n\n"
elif query_number == 102:
    result += "K-Means\n"
    for i in range(number_or_runs):
        start = timer()
        print(impl_kmeans(scale_factor))
        first = False
        end = timer()
        result += str(end-start) + "\n"
        print ("Run " + str(i+1) + " : " + str(end-start))
        if i > 0:
            total_time += (end-start)
    number_or_runs = number_or_runs - 1 if number_or_runs > 1 else 1
    result += "Avg of warm runs : " + str(total_time / number_or_runs) + "\n\n"
elif query_number == 103:
    result += "Quantiles\n"
    for i in range(number_or_runs):
        start = timer()
        print(impl_quantile(scale_factor))
        first = False
        end = timer()
        result += str(end-start) + "\n"
        print ("Run " + str(i+1) + " : " + str(end-start))
        if i > 0:
            total_time += (end-start)
    number_or_runs = number_or_runs - 1 if number_or_runs > 1 else 1
    result += "Avg of warm runs : " + str(total_time / number_or_runs) + "\n\n"
elif query_number == 104:
    result += "Conjugate Gradient Optimization\n"
    for i in range(number_or_runs):
        start = timer()
        print(impl_conjugate_grad_opt(scale_factor))
        first = False
        end = timer()
        result += str(end-start) + "\n"
        print ("Run " + str(i+1) + " : " + str(end-start))
        if i > 0:
            total_time += (end-start)
    number_or_runs = number_or_runs - 1 if number_or_runs > 1 else 1
    result += "Avg of warm runs : " + str(total_time / number_or_runs) + "\n\n"
elif query_number == 105:
    for i in range(number_or_runs):
        start = timer()
        print(query(sql105_iter_1,scale_factor))
        first = False
        end = timer()
        result += str(end-start) + "\n"
        print ("Run " + str(i+1) + " : " + str(end-start))
        if i > 0:
            total_time += (end-start)
    number_or_runs = number_or_runs - 1 if number_or_runs > 1 else 1
    result += "Avg of warm runs : " + str(total_time/number_or_runs) + "\n\n"
elif query_number == 106:
    for i in range(number_or_runs):
        start = timer()
        print(query(sql106_iter_2, scale_factor))
        first = False
        end = timer()
        result += str(end-start) + "\n"
        print ("Run " + str(i+1) + " : " + str(end-start))
        if i > 0:
            total_time += (end-start)
    number_or_runs = number_or_runs - 1 if number_or_runs > 1 else 1
    result += "Avg of warm runs : " + str(total_time/number_or_runs) + "\n\n"
else:
    print('query number is not a valid option')

f= open("output.txt","a+")
f.write(output)
f.write(result)
f.close()


end1 = time.time()
number_or_runs = number_or_runs - 1 if number_or_runs > 1 else 1
print ("Avg of warm runs : " + str(end1 - start1/number_or_runs))


