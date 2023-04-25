#! /home1/grads/sdas6/anaconda2/bin/python2.7

import raco.myrial.parser as parser
import raco.myrial.interpreter as interpreter
import raco.backends.myria as alg
from DaskDB.dask_query_parser import DaskQueryParser
from raco.expression.expression import UnnamedAttributeRef
from raco.catalog import FromFileCatalog
import dask.dataframe as dd
import json
#from .sqlparser import visitor_tables as vt
import sql_metadata
import re
import DaskDB.dask_physical_plan as dpp
import DaskDB.dask_plan as dp
# from pmlb import fetch_data, classification_dataset_names #sray

col_names_lineitem = ['l_orderkey','l_partkey','l_suppkey','l_linenumber','l_quantity','l_extendedprice','l_discount'
    ,'l_tax','l_returnflag','l_linestatus','l_shipdate','l_commitdate','l_receiptdate','l_shipinstruct'
    ,'l_shipmode', 'l_comment']
col_names_customer = ['c_custkey','c_name','c_address','c_nationkey','c_phone','c_acctbal','c_mktsegment','c_comment']

col_names_orders = ['o_orderkey','o_custkey','o_orderstatus','o_totalprice','o_orderdate','o_orderpriority'
                    ,'o_clerk','o_shippriority','o_comment']

col_names_part = ['p_partkey','p_name','p_mfgr','p_brand','p_type','p_size','p_container','p_retailprice','p_comment']
col_names_supplier = ['s_suppkey','s_name','s_address','s_nationkey','s_phone','s_acctball','s_comment']
col_names_partsupp = ['ps_partkey','ps_suppkey','ps_availqty','ps_supplycost','ps_comment']
col_names_nation = ['n_nationkey','n_name','n_regionkey','n_comment']
col_names_region = ['r_regionkey','r_name','r_comment']
col_names_countries = ['id', 'c_name']
col_names_distances = ['src', 'target', 'distance']
is_fetch_data=False

#skdas
dp.set_col_name_info(col_names_lineitem, col_names_customer, col_names_orders, col_names_part,
                     col_names_supplier, col_names_partsupp, col_names_nation, col_names_region,
                     col_names_countries, col_names_distances)

# primary_key = {
#     'orders' : 'o_orderkey',
#     'customer':'c_custkey',
#     'nation' : 'n_nationkey',
#     'region' : 'r_regionkey',
#     'part' : 'p_partkey',
#     'supplier' : 's_suppkey'
#     }
# 
# foreign_key = {
#     'lineitem' : [['l_partkey','part'], ['l_suppkey','supplier'], ['l_orderkey','orders']],
#     'orders' : [['o_custkey','customer']],
#     'customer' : [['c_nationkey','nation']],
#     'nation' : [['n_regionkey','region']],
#     'supplier' : [['s_nationkey','nation']],
#     'partsupp' : [['ps_partkey','part'], ['ps_suppkey','supplier']]
#     }
# 
# def getPrimaryKeyInfo():
#     return primary_key
# 
# def getForeignKeyInfo():
#     return foreign_key


def getPlan(sql, udf_list):
    plan,used_columns=getNormalPlan(sql, udf_list)
    return plan,used_columns

def get_dataframe(alias):
    if alias == 'lineitem':
        df = dd.read_csv('data/'+alias+".csv",delimiter="|",names=col_names_lineitem, parse_dates=[10,11,12]);
    elif alias == 'customer':
        df = dd.read_csv('data/'+alias+".csv",delimiter="|",names=col_names_customer);
    elif alias == 'orders':
        df = dd.read_csv('data/'+alias+'.csv',delimiter="|",names=col_names_orders, parse_dates=[4]);
    elif alias == 'part':
        df = dd.read_csv('data/'+alias+'.csv',delimiter="|",names=col_names_part);
    elif alias == 'supplier':
        df = dd.read_csv('data/'+alias+'.csv',delimiter="|",names=col_names_supplier);
    elif alias == 'partsupp':
        df = dd.read_csv('data/'+alias+'.csv',delimiter="|",names=col_names_partsupp);
    elif alias == 'nation':
        df = dd.read_csv('data/'+alias+'.csv',delimiter="|",names=col_names_nation);
    elif alias == 'region':
        df = dd.read_csv('data/'+alias+'.csv',delimiter="|",names=col_names_region);
    elif alias == 'countries':
        df = dd.read_csv('data/'+alias+'.csv',delimiter="|",names=col_names_countries);
    elif alias == 'distances':
        df = dd.read_csv('data/'+alias+'.csv',delimiter="|",names=col_names_distances);
    else:
        # TODO: revert
        return dd.read_csv('data/'+"lineitem"+".csv",delimiter="|",names=col_names_lineitem, parse_dates=[10,11,12]);

    return df


def getNormalPlan(sql, udf_list):
    plan = []
    use_cols = dict()
    query_context = DaskQueryParser().parse(sql)
    if query_context.is_iterative():
        base = query_context.base
        recursive = query_context.iterative
        final = query_context.final

        sub_plan, sub_cols = get_query_plan(base, udf_list, query_context)
        plan.append(sub_plan)
        use_cols.update(sub_cols)

        sub_plan, sub_cols = get_query_plan(recursive, udf_list, query_context)
        plan.append(sub_plan)
        use_cols.update(sub_cols)

        sub_plan, sub_cols = get_query_plan(final, udf_list, query_context)
        plan.append(sub_plan)
        use_cols.update(sub_cols)
    else:
        sub_plan, use_cols = get_query_plan(sql, udf_list, query_context)
        plan.append(sub_plan)

    return plan, use_cols

def get_query_plan(sql, udf_list, query_context):
    my_dict={}
    sql_re = sql
    if 'limit' in sql:
        test = re.compile('\slimit\s\d+')
        sql_re = test.sub('',sql_re)
    if 'interval' in sql:
        test = re.compile('(\-|\+)\sinterval\s(\"|\')\d+(\"|\')\s\w+')
        sql_re = test.sub('',sql_re)
    if 'date' in sql:
        test = re.compile('\sdate\s')
        sql_re = test.sub('',sql_re)

    #q,used_columns,find_func=vt.getTableName(sql_re)
    q = sql_metadata.get_query_tables(sql_re)
    used_columns = sql_metadata.get_query_columns(sql_re)
    if len(udf_list) > 0:
        find_func = tuple((i,'LONG_TYPE') for i in udf_list)
    else:
        find_func = (('rand', 'LONG_TYPE'),)
    #find_func = (('unique','LONG_TYPE'),('LinearRegression','LONG_TYPE'))

    use_cols = dict()
    for i in q:
        a=[]
        b=[]
        p=[]
        string_main="public:adhoc:"
        string=i.lower()
        string_main=string_main+string
        #print (string_main)
        alias=string

        p = []
        if query_context.is_iterative() and query_context.cte == alias:
            use_cols[string] = query_context.cte_params
            if query_context.cte == 'cte_paths':
                p.append((query_context.cte_params[0], 'LONG_TYPE'))
                p.append((query_context.cte_params[1], 'LONG_TYPE'))
                p.append((query_context.cte_params[2], 'LONG_TYPE'))
                p.append((query_context.cte_params[3], 'LONG_TYPE'))
            else:
                p.append((query_context.cte_params[0], 'LONG_TYPE'))
                p.append((query_context.cte_params[1], 'STRING_TYPE'))
                p.append((query_context.cte_params[2], 'STRING_TYPE'))
                p.append((query_context.cte_params[3], 'DOUBLE_TYPE'))
                p.append((query_context.cte_params[4], 'LONG_TYPE'))
            #for item in query_context.cte_params:
            #    p.append((item, 'STRING_TYPE'))
        else:
            df = get_dataframe(alias)
            #exec(df_read_string)

            a=df.dtypes.tolist()
            for i in a:
                if i == 'int64':
                    b.append("LONG_TYPE")
                elif i == 'float64':
                    b.append("DOUBLE_TYPE")
                elif i == 'datetime64[ns]':
                    b.append("DATETIME_TYPE")
                else:
                    b.append("STRING_TYPE")

            d=df.columns.values.tolist()
            #print(d)
            _cols = []
            for i in range(len(b)):
                if d[i] in used_columns:
                    _cols.append(d[i])
                    p.append((d[i],b[i]))
            use_cols[string] = _cols

        my_dict.update({string_main:p})

    with open('catalog3.py', 'w') as fp:
            json.dump(my_dict, fp)
    _catalog = FromFileCatalog.load_from_file("catalog3.py")
    _parser = parser.Parser()
    #[_parser.add_python_udf(*uda) for uda in find_func]
    finalstring=""""""
    for i,c in enumerate(q):
        filler= """= scan(public:adhoc:"""
        filler2=""");"""
        finalstring=finalstring+q[i].lower()+ filler +q[i].lower()+filler2
    if find_func:
        print(finalstring +
              """out = """ + sql + """
            store(out, OUTPUT);
            """, find_func)
        statement_list = _parser.parse(
            finalstring +
            """out = """ + sql + """
            store(out, OUTPUT);
            """, find_func)
    else:
           statement_list = _parser.parse(
            finalstring+
            """out = """ + sql + """
            store(out, OUTPUT);
            """)
    processor = interpreter.StatementProcessor(_catalog, True)
    processor.evaluate(statement_list)
    plan = processor.get_json()
    return plan,use_cols

def get_plan_for_all():
    my_dict={}
    q=['customer','orders','lineitem','supplier','nation','region','partsupp','part']
    for i in q:
        a=[]
        b=[]
        p=[]
        string_main="public:adhoc:"
        string=i.lower()
        string_main=string_main+string
        print (string_main)
        alias=string

        df_read_string = get_string_dataframe(alias)
        exec(df_read_string)

        a=df.dtypes.tolist()
        for i in a:
            if i == 'int64':
                b.append("LONG_TYPE")
            if i == 'float64':
                b.append("DOUBLE_TYPE")
            if i == 'object':
                    b.append("STRING_TYPE")
            if i == 'datetime64[ns]':
                    b.append("DATETIME_TYPE")


        d=df.columns.values.tolist()
        for i in range(len(b)):
            p.append((d[i],b[i]))
        my_dict.update({string_main:p})
    with open('catalog3.py', 'w') as fp:
        json.dump(my_dict, fp)
    _catalog = FromFileCatalog.load_from_file("catalog3.py")
    finalstring=""""""
    for i,c in enumerate(q):
        filler= """= scan(public:adhoc:"""
        filler2=""");"""
        finalstring=finalstring+q[i].lower()+ filler +q[i].lower()+filler2
    return finalstring



def getNestedPlan(sql):
    my_dict={}
    sqlnew=sql[sql.find("(")+1:sql.find(")")] + ";"
    sql_re = sqlnew
    if 'limit' in sql:
        test = re.compile('\slimit\s\d+')
        sql_re = test.sub('',sql_re)
    if 'interval' in sql:
        test = re.compile('(\-|\+)\sinterval\s(\"|\')\d+(\"|\')\s\w+')
        sql_re = test.sub('',sql_re)
    if 'date' in sql:
        test = re.compile('\sdate\s')
        sql_re = test.sub('',sql_re)
    q=vt.getTableName(sql_re)
    for i in q:
        a=[]
        b=[]
        p=[]
        string_main="public:adhoc:"
        string=i.lower()
        string_main=string_main+string
        print (string_main)
        alias=string

        df_read_string = get_string_dataframe(alias)
        exec(df_read_string)
        a=df.dtypes.tolist()
        for i in a:
            if i == 'int64':
                b.append("LONG_TYPE")
            if i == 'float64':
                b.append("DOUBLE_TYPE")
            if i == 'object':
                    b.append("STRING_TYPE")
        d=df.columns.values.tolist()
        for i in range(len(b)):
            p.append((d[i],b[i]))
        my_dict.update({string_main:p})
    with open('catalog3.py', 'w') as fp:
        json.dump(my_dict, fp)
    _catalog = FromFileCatalog.load_from_file("catalog3.py")
    _parser = parser.Parser()
    finalstring=""""""
    for i,c in enumerate(q):
        filler= """= scan(public:adhoc:"""
        filler2=""");"""
        finalstring=finalstring+q[i].lower()+ filler +q[i].lower()+filler2
    statement_list = _parser.parse(
        finalstring+
        """out = """ + sqlnew + """
        store(out, OUTPUT);
        """)
    processor = interpreter.StatementProcessor(_catalog, True)
    processor.evaluate(statement_list)
    plan = processor.get_json()
    phys = dpp.PhysicalPlan()
    dask_plan = phys.init(plan)
    #print dask_plan
    da_pl = dp.DaskPlan()
    df = da_pl.convert_to_dask_code(dask_plan)
#df.visualize(filename='test.png')
    print (df.compute())
    df.compute().to_csv("data/temp.csv", encoding='utf-8', index=False)
    sqlnewest=sql[0:sql.find("(")]+"temp ;"
    print (sqlnewest)
    plan,used_columns=getNormalPlan(sqlnewest)
    return plan
