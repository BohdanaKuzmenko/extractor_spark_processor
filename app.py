import ast
import datetime
import multiprocessing
from os import listdir
from os.path import isfile, join, dirname
from pandas import concat, set_option, read_csv
from numpy import array_split
from extractor import Extractor
from handlers.io_data_handler import *
from handlers.df_handler import *

# set_option('display.max_colwidth', -1)
BASE_DIR = dirname(dirname(__file__))


def get_initial_files(dir_path):
    return [join(dir_path, f) for f in listdir(dir_path) if isfile(join(dir_path, f))]


def get_bios(initial_files):
    data = [read_csv(file, sep="\n", header=None) for file in initial_files]
    result = concat(data)
    result[0] = result[0].apply(lambda x: test(x))
    return result
    # bios = []
    # [bios.extend(open(file).readlines()) for file in initial_files]
    # return bios



def bios_str_to_df(bios):
    val_dict = [ast.literal_eval(i) for i in bios]
    bios = DataFrame(val_dict)
    bios['full_info'] = join_df_cols(bios, ['PA', 'SP', 'Score', 'merged_regex', 'sentence', 'sentence_num', 'url'])
    return bios.groupby(['url'])['full_info'].agg({'result': lambda x: tuple(x, )}).reset_index()

def test(x):
    bio_sent_item = ast.literal_eval(x)
    return (bio_sent_item.get('url'), tuple([bio_sent_item.get(key) for key in sorted(bio_sent_item)]))

if __name__ == "__main__":
    t1 = datetime.datetime.now()
    print("Process started: " + str(t1))
    initial_files= get_initial_files(join(BASE_DIR,'extractor/data/initial'))

    data = get_bios(initial_files)
    data = split_data_frame_col(data, ['url', 'full_info'], data.columns[0])
    bio_df =  data.groupby(['url'])['full_info'].agg({'result': lambda x: tuple(x, )}).reset_index()


    extractor = Extractor()
    p = multiprocessing.Pool(4)
    pool_results = p.map(extractor.group_data, array_split(bio_df, 4))
    p.close()
    p.join()
    p.terminate()

    concatenated = concat(pool_results)
    print(concatenated)
    # DataHandler.chunk_to_csv(result, "data/result/result.csv", header=True)
    t2 = datetime.datetime.now()
    print("Ready result: " + str(t2))
    print("Full process time: " + str(t2 - t1))
