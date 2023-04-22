import dask.dataframe as dd
from dask.distributed import Client
# from dask.multiprocessing import get

#import pyximport
#pyximport.install()
#from test_join import inner_join

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
import dask

dask.config.set(scheduler='single-threaded')
#client = Client('192.168.0.108:8786')
#client.restart()

def test2(row, key):
    if row['subject_id'] == key:
        print('Key : ' + str(key))
        print('Row : ' + str(row))
        return row

def test(pd):
    return len(pd)


def intersect_sets(df, col_1, col_2):
    result = pd.Series(np.intersect1d(df[col_1], col_2))
    result = result.rename("keys")
    return result

raw_data = {
        'subject_id': [11, 13, 14, 16, 17, 18],
        #'student_id' : [20, 25, 21, 23, 22, 20],
        'first_name': ['Alex', 'Amy', 'Allen', 'Alice', 'Ayoung', 'Alex'], 
        'last_name': ['Anderson', 'Ackerman', 'Ali', 'Aoni', 'Atiches', 'Anderson'],
        'game_id': [12, 9, 45, 16, 33, 12]
        }
#df_a = pd.DataFrame(raw_data, columns = ['subject_id', 'student_id', 'first_name', 'last_name', 'game_id'])
df_a = pd.DataFrame(raw_data, columns = ['subject_id', 'first_name', 'last_name', 'game_id'])
#df_a = df_a.set_index('subject_id')
ddf_a = dd.from_pandas(df_a, npartitions=3)
#ddf_a = ddf_a.set_index('subject_id')


raw_data = {
        'sub_id': [11, 14, 10, 17, 18, 19, 14, 14, 11, 11],
        'student_id' : [20, 21, 21, 23, 22, 22, 23, 24, 20, 20],
        'test_id': [11, 15, 15, 61, 16, 14, 25, 56, 17, 51],
        'game_id': [12, 45, 32, 15, 16, 9, 25, 26, 12, 12]
        }
df_n = pd.DataFrame(raw_data, columns = ['sub_id', 'student_id', 'test_id', 'game_id'])
l = [18, 14, 11, 11, 19, 19, 19, 15]
ddf_n = dd.from_pandas(df_n, npartitions=3)
ddf_n = ddf_n.set_index('sub_id', drop=False)


def insert_bit(array, index):
    inner = (index % 64)
    index //= 64
    array[index] |= (1 << inner)
    

def is_set(array, index):
    inner = (index % 64)
    index //= 64
    return array[index] & (1 << inner)


class BloomFilter(object):
    def __init__(self, N, Nbits):
        self.Nbits = Nbits
        self.bloom = np.zeros(Nbits//64 + bool(Nbits % 64), np.uint64)
        self.k = int(np.round(.7 *Nbits/N))

    def hash(self, s, index):
        return (hash(s + (ord('x') * index)) % self.Nbits)

    def insert(self, s):
        for h in range(self.k):
            insert_bit(self.bloom, self.hash(s, h))

    def __contains__(self, s):
        for h in range(self.k):
            if not is_set(self.bloom, self.hash(s, h)):
                return False
            return True

f = BloomFilter(10,100)
f.insert(11)
x = 11 in f
