#!/usr/bin/python
# -*- coding: utf-8 -*

from handlers.df_handler import *
from pandas import to_numeric, notnull
import datetime
from handlers.io_data_handler import DataHandler
from numpy import nan

PROFILE_URL_COL = "url"
SENTENCE_INDEX_COL = "sentence_num"
SPECIALTIES_COL = 'SP'
PRACTICE_AREAS_COL = 'PA'
REG_SCORE_COL = 'Score'
PRACTICE_AREAS_SCORE_COL = 'practice_area_score'


class Extractor(object):
    def group_data(self, filtered_bios_df):
        """
        :param filtered_bios_df: DataFrame
        Method filters rows with the same practice areas and specialties according to max score value.
        """
        print("Grouping and filtering data started: " + str(datetime.datetime.now()))
        filtered_bios_df = split_data_frame_rows(filtered_bios_df, 'result')
        filtered_bios_df = split_data_frame_col(filtered_bios_df,
                                                [PRACTICE_AREAS_COL, SPECIALTIES_COL, REG_SCORE_COL,
                                                 SENTENCE_INDEX_COL, PROFILE_URL_COL], 'result')

        filtered_bios_df[[REG_SCORE_COL, SENTENCE_INDEX_COL]] = filtered_bios_df[
            [REG_SCORE_COL, SENTENCE_INDEX_COL]].apply(to_numeric)

        filtered_bios_df[REG_SCORE_COL] = filtered_bios_df[REG_SCORE_COL] + (
            filtered_bios_df[REG_SCORE_COL] / filtered_bios_df[SENTENCE_INDEX_COL])

        cols_to_join = [SPECIALTIES_COL, REG_SCORE_COL]
        filtered_bios_df['sentence_info'] = join_df_cols(filtered_bios_df, cols_to_join)
        filtered_bios_df.drop(cols_to_join, inplace=True, axis=1)

        group_by_cols = [PROFILE_URL_COL, SENTENCE_INDEX_COL, PRACTICE_AREAS_COL]
        grouped_bios = filtered_bios_df.groupby(group_by_cols)['sentence_info'] \
            .agg({'result': lambda specialities_info: self.get_max_scored_sp(specialities_info)}).reset_index()

        split_cols = [SPECIALTIES_COL, REG_SCORE_COL]
        grouped_bios = split_data_frame_col(grouped_bios, split_cols, 'result')

        grouped_bios[PRACTICE_AREAS_SCORE_COL] = grouped_bios.groupby([PROFILE_URL_COL, PRACTICE_AREAS_COL])[
            REG_SCORE_COL].transform('sum')
        print("Grouping and filtering data finished: " + str(datetime.datetime.now()))
        result = self.count_result(grouped_bios)
        DataHandler.chunk_to_csv(result, "data/result/result.csv")


    def count_result(self, grouped_df):
        """
        :param grouped_df: DataFrame
        Method removes unappropriate practice areas and returns data according to limit
        """
        print("Count result started: " + str(datetime.datetime.now()))
        cols_to_join = [SPECIALTIES_COL, PRACTICE_AREAS_COL, PRACTICE_AREAS_SCORE_COL]
        grouped_df['practice_area_info'] = join_df_cols(grouped_df, cols_to_join)
        grouped_df.drop(cols_to_join, inplace=True, axis=1)

        grouped_df = grouped_df.fillna('')
        grouped_bios = grouped_df.groupby([PROFILE_URL_COL, SENTENCE_INDEX_COL])['practice_area_info'] \
            .agg({'result': lambda x: tuple(self.remove_conflicts(x, ))})

        grouped_bios = grouped_bios.fillna('')

        grouped_bios = split_data_frame_rows(grouped_bios, 'result')

        split_cols = [SPECIALTIES_COL, PRACTICE_AREAS_COL, REG_SCORE_COL]
        grouped_bios = split_data_frame_col(grouped_bios, split_cols, 'result').reset_index()

        cols_to_join = [SPECIALTIES_COL, PRACTICE_AREAS_COL, REG_SCORE_COL]
        grouped_bios['bio_full_info'] = join_df_cols(grouped_bios, cols_to_join)

        grouped_bios = grouped_bios.groupby([PROFILE_URL_COL])['bio_full_info'] \
            .agg({'test': lambda x: tuple(self.filter_by_practice_area_score(x, ))})

        grouped_bios = split_data_frame_rows(grouped_bios, 'test')

        split_cols = [SPECIALTIES_COL, PRACTICE_AREAS_COL, REG_SCORE_COL]
        grouped_bios = split_data_frame_col(grouped_bios, split_cols, 'test').reset_index()

        grouped_bios = grouped_bios.groupby([PROFILE_URL_COL])[SPECIALTIES_COL, PRACTICE_AREAS_COL].agg(
            {"Predictions": lambda x: ', '.join([i for i in set(x, ) if i])}).reset_index()
        print("Count result finished: " + str(datetime.datetime.now()))
        return grouped_bios

    def get_max_scored_sp(self, specialities_info):
        """
        :param specialities_info: tuple
        :return: tuple of max scored specialty information
        """
        max_score = max([sent_info[1] for sent_info in specialities_info])
        return tuple([spec_info for spec_info in specialities_info if spec_info[1] == max_score][0])

    def remove_conflicts(self, sentence_info):
        """
        :param sentence_info: tuple, that contains information gotten for certain sentence: sets of speciality, practice
         area and practice area score
        :return: filtered tuple of tuples
        method removes potential practice areas that are incompatible with other selections.
        """
        specialties = [value[0] for value in sentence_info]

        conflict_groups = {key: [] for key in set(specialties) if key}
        unconflict_groups = []
        for data in sentence_info:
            specialty, pract_area, score = data
            if specialty and specialties.count(specialty) > 1:
                conflict_groups[specialty].append(data)
            elif pract_area in conflict_groups.keys() and not specialty:
                conflict_groups[pract_area].append(data)
            else:
                unconflict_groups.append(data)
        for key in conflict_groups.keys():
            if conflict_groups.get(key):
                group_max_score = max([i[2] for i in conflict_groups.get(key)])
                max_scored = [i for i in conflict_groups.get(key) if i[2] == group_max_score]
                practice_only = [i for i in max_scored if not i[0]]
                unconflict_groups.extend(practice_only) if practice_only else unconflict_groups.extend(max_scored)
        return unconflict_groups

    def filter_by_practice_area_score(self, data_frame):
        """
        :param data_frame:pd.DataFrame
        :return: limited list of practice areas and specialties
        """
        data_frame = data_frame.fillna('')

        max_scored_limit = 2
        sorted_data = sorted(set(data_frame), key=lambda x: x[2], reverse=True)
        appropriate_data = []
        for bio_info in sorted_data:
            if len(appropriate_data) < max_scored_limit or bio_info[2] == appropriate_data[-1][2]:
                appropriate_data.append(bio_info)
        return appropriate_data


if __name__ == "__main__":
    pass
