from pandas import DataFrame
from handlers.io_data_handler import DataHandler
import ast
import datetime
from os import listdir
from os.path import isfile, join, dirname, abspath


def get_initial_files(dir_path):
    return [join(dir_path, f) for f in listdir(dir_path) if isfile(join(dir_path, f))]

base_dir = abspath('')
all_files = get_initial_files(join(base_dir,'extractor_data'))
for file_n in all_files:
    t1 = datetime.datetime.now()
    f_name = file_n.split('/')[-1]
    print(f_name)
    f = open(file_n)
    regex_filtered_bios = f.readlines()
    test = regex_filtered_bios
    val_dict = [ast.literal_eval(i) for i in test]
    bios = DataFrame(val_dict)
    DataHandler.chunk_to_csv(bios[['PA', 'SP', 'Score', 'sentence_num', 'url']], "data/initial/{}.csv".format(f_name),
                             header="True")
    t2 = datetime.datetime.now()
    print(str(t2 - t1))
