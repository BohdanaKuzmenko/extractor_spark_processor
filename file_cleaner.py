import ast
from pandas import read_csv
from handlers.df_handler import *
from handlers.io_data_handler import DataHandler
from os import listdir
from os.path import isfile, join, dirname, abspath

def test(x):
    bio_sent_item = ast.literal_eval(x)
    return tuple([bio_sent_item.get(key) for key in sorted(bio_sent_item)])


def get_bios(initial_file):
    data = read_csv(initial_file, sep="\n", header=None)
    data[0] = data[0].apply(lambda x: test(x))
    data = split_data_frame_col(data, ['PA', 'SP', 'Score', 'merged_regex', 'sentence', 'sentence_num', 'url'], data.columns[0])
    return data

def get_initial_files(dir_path):
    return [join(dir_path, f) for f in listdir(dir_path) if isfile(join(dir_path, f))]

base_dir = abspath('')
all_files = get_initial_files(join(base_dir,'extractor_data'))
print(all_files)
for file in all_files:
    file_name = file.split('/')[-1]
    print(file_name)
    result = get_bios(file)
    DataHandler.chunk_to_csv(result[['PA', 'SP', 'Score', 'sentence_num', 'url']], "data/initial/{}.csv".format(file_name), header=True)

