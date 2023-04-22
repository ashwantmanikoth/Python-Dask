hdfs_namenode_IP = dask_scheduler_IP = '10.162.0.2'
hdfs_port = 9000
dask_scheduler_port = 8786
python_dir = '/home/ashwanta75/anaconda3/envs/DaskDB/bin/python'

def get_hdfs_master_node_IP():
    return hdfs_namenode_IP

def get_hdfs_master_node_port():
    return hdfs_port

def get_dask_scheduler_IP():
    return dask_scheduler_IP

def get_dask_scheduler_port():
    return dask_scheduler_port

def get_python_dir():
    return python_dir
