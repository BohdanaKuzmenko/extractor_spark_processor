import ast
import datetime
import multiprocessing
from os import listdir
from os.path import isfile, join, dirname, abspath
from pandas import concat, set_option, read_csv
from numpy import array_split
from extractor import Extractor
from handlers.io_data_handler import *

from handlers.df_handler import *

# set_option('display.max_colwidth', -1)
BASE_DIR = abspath('')


def get_initial_files(dir_path):
    return [join(dir_path, f) for f in listdir(dir_path) if isfile(join(dir_path, f))]

if __name__ == "__main__":

    t1 = datetime.datetime.now()
    print("Process started: " + str(t1))
    initial_files= get_initial_files(join(BASE_DIR,'data/initial'))
    bios = [next(DataHandler.get_csv_values(file_name, sep='\t')) for file_name in initial_files]
    all_bios = concat(bios).reset_index()
    all_bios = all_bios.fillna('')

    all_bios['full_info'] = join_df_cols(all_bios, ['PA', 'SP', 'Score', 'sentence_num', 'url'])
    bio_df = all_bios.groupby(['url'])['full_info'].agg({'result': lambda x: tuple(x, )}).reset_index()

    extractor = Extractor()
    p = multiprocessing.Pool(4)
    pool_results = p.map(extractor.group_data, array_split(bio_df, 4))
    p.close()
    p.join()
    p.terminate()

    concatenated = concat(pool_results)
    DataHandler.chunk_to_csv(concatenated, "data/result/result.csv", header=True)
    t2 = datetime.datetime.now()
    print("Ready result: " + str(t2))
    print("Full process time: " + str(t2 - t1))
