#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May  9 12:46:27 2019

@author: harsh97mah
"""

import re
import string
import sys
import unicodedata
import functools
from threading import Thread

from dateutil.parser import parse


from .parsingException import ParsingException
from .query import *


class SelectParser(Thread):
    def __init__(self, columns_of_select, phrase, count_keywords, sum_keywords, average_keywords,
                 max_keywords, min_keywords, distinct_keywords,non_max_keywords, non_min_keywords, empty_keywords,
                 non_empty_keywords,explore_keywords, database_dico, database_object):
        Thread.__init__(self)
        self.select_objects = []
        self.columns_of_select = columns_of_select
        self.phrase = phrase
        self.count_keywords = count_keywords
        self.sum_keywords = sum_keywords
        self.average_keywords = average_keywords
        self.max_keywords = max_keywords
        self.min_keywords = min_keywords
        self.distinct_keywords = distinct_keywords
        self.non_max_keywords = non_max_keywords
        self.non_min_keywords = non_min_keywords
        self.empty_keywords = empty_keywords
        self.non_empty_keywords = non_empty_keywords
        self.explore_keywords = explore_keywords
        self.database_dico = database_dico
        self.database_object = database_object

    def get_tables_of_column(self, column):
        tmp_table = []
        for table in self.database_dico:
            if column in self.database_dico[table]:
                tmp_table.append(table)
        return tmp_table

    def get_column_name_with_alias_table(self, column):
        #one_table_of_column = self.get_tables_of_column(column)[0]
        return str(column)

    def uniquify(self, list):
        already = []
        for element in list:
            if element not in already:
                already.append(element)
        return already

    def run(self):
                self.select_object = Select(self.phrase)
                is_count = False
                self.columns_of_select = self.uniquify(self.columns_of_select)
                number_of_select_column = len(self.columns_of_select)
                select_phrases = []
                previous_index = 0

                for i in range(0, len(self.phrase)):
                        select_phrases.append(self.phrase[previous_index:i + 1])
                        previous_index = i + 1
                select_phrases = list(filter(None, select_phrases))
                #print(select_phrases)
                for i in range(0, len(select_phrases)):  # for each select phrase (i.e. column processing)
                    select_type = []
                    phrase = [word.lower() for word in select_phrases[i]]
                    #print(phrase)
#                    for keyword in self.average_keywords:
#                        if keyword in phrase:
#                            select_type.append('AVG')
#                    for keyword in self.count_keywords:
#                        if keyword in phrase:
#                            select_type.append('COUNT')
#                    for keyword in self.max_keywords:
#                        if keyword in phrase:
#                            select_type.append('MAX')
#                    for keyword in self.min_keywords:
#                        if keyword in phrase:
#                            select_type.append('MIN')
#                    for keyword in self.sum_keywords:
#                        if keyword in phrase:
#                            select_type.append('SUM')
#                    for keyword in self.distinct_keywords:
#                        if keyword in phrase:
#                            select_type.append('DISTINCT')
#                    for keyword in self.non_max_keywords:
#                        if keyword in phrase:
#                            select_type.append('NON_MAX')
#                    for keyword in self.non_min_keywords:
#                        if keyword in phrase:
#                            select_type.append('NON_MIN')
#                    for keyword in self.empty_keywords:
#                        if keyword in phrase:
#                            select_type.append('EMPTY')
#                    for keyword in self.non_empty_keywords:
#                        if keyword in phrase:
#                            select_type.append('NON_EMPTY')
                    for keyword in self.explore_keywords:
                        if keyword in phrase:
                            select_type.append(['exp','COUNT','PERCENTAGE'])
#                    if len(select_type) == 0:
#                        select_type.append('ALL')
                    if (i != len(select_phrases) - 1) or i == 0:
                        if len(select_type) >=1:
                            if len(self.columns_of_select) == 0:
                                self.select_object.add_column(None, self.uniquify(select_type))
                            else:
                                column = self.get_column_name_with_alias_table(self.columns_of_select[0])
                                self.select_object.add_column(column, self.uniquify(select_type))
                        else:
                            column = self.get_column_name_with_alias_table(self.columns_of_select[0])
                            self.select_object.add_column(column, ['ALL'])
                self.select_objects.append(self.select_object)

    def join(self):
        Thread.join(self)
        return self.select_objects

class FromParser(Thread):
    def __init__(self, columns_of_select, columns_of_where, database_object):
        Thread.__init__(self)
        self.queries = []
        self.columns_of_select = columns_of_select
        self.columns_of_where = columns_of_where

        self.database_object = database_object
        self.database_dico = self.database_object.get_tables_into_dictionary()

    def get_tables_of_column(self, column):
        tmp_table = []
        for table in self.database_dico:
            if column in self.database_dico[table]:
                tmp_table.append(table)
        return tmp_table

    def intersect(self, a, b):
        return list(set(a) & set(b))

    def difference(self, a, b):
        differences = []
        for _list in a:
            if _list not in b:
                differences.append(_list)
        return differences

    def is_direct_join_is_possible(self, table_src, table_trg):
        fk_column_of_src_table = self.database_object.get_foreign_keys_of_table(table_src)
        fk_column_of_trg_table = self.database_object.get_foreign_keys_of_table(table_trg)

        for column in fk_column_of_src_table:
            if column.is_foreign()['foreign_table'] == table_trg:
                return [(table_src, column.name), (table_trg, column.is_foreign()['foreign_column'])]

        for column in fk_column_of_trg_table:
            if column.is_foreign()['foreign_table'] == table_src:
                return [(table_src, column.is_foreign()['foreign_column']), (table_trg, column.name)]

                # pk_table_src = self.database_object.get_primary_key_names_of_table(table_src)
                # pk_table_trg = self.database_object.get_primary_key_names_of_table(table_trg)
                # match_pk_table_src_with_table_trg = self.intersect(pk_table_src, self.database_dico[table_trg])
                # match_pk_table_trg_with_table_src = self.intersect(pk_table_trg, self.database_dico[table_src])

                # if len(match_pk_table_src_with_table_trg) >= 1:
                #     return [(table_trg, match_pk_table_src_with_table_trg[0]), (table_src, match_pk_table_src_with_table_trg[0])]
                # elif len(match_pk_table_trg_with_table_src) >= 1:
                # return [(table_trg, match_pk_table_trg_with_table_src[0]),
                # (table_src, match_pk_table_trg_with_table_src[0])]

    def get_all_direct_linked_tables_of_a_table(self, table_src):
        links = []
        for table_trg in self.database_dico:
            if table_trg != table_src:
                link = self.is_direct_join_is_possible(table_src, table_trg)
                if link is not None:
                    links.append(link)
        return links

    def is_join(self, historic, table_src, table_trg):
        historic = historic
        links = self.get_all_direct_linked_tables_of_a_table(table_src)

        differences = []
        for join in links:
            if join[0][0] not in historic:
                differences.append(join)
        links = differences

        for join in links:
            if join[1][0] == table_trg:
                return [0, join]

        path = []
        historic.append(table_src)

        for join in links:
            result = [1, self.is_join(historic, join[1][0], table_trg)]
            if result[1] != []:
                if result[0] == 0:
                    path.append(result[1])
                    path.append(join)
                else:
                    path = result[1]
                    path.append(join)
        return path

    def get_link(self, table_src, table_trg):
        path = self.is_join([], table_src, table_trg)
        if len(path) > 0:
            path.pop(0)
            path.reverse()
        return path

    def unique(self, _list):
        return [list(x) for x in set(tuple(x) for x in _list)]

    def unique_ordered(self, _list):
        frequency = []
        for element in _list:
            if element not in frequency:
                frequency.append(element)
        return frequency

    def run(self):
            self.queries = []

            query = Query()
            join_object = Join()
            links = []
            join_object.set_links(self.unique_ordered(links))
            query.set_join(join_object)
            self.queries.append(query)

    def join(self):
        Thread.join(self)
        return self.queries


class WhereParser(Thread):
    def __init__(self, phrases, columns_of_values_of_where, count_keywords, sum_keywords,
                 average_keywords, max_keywords, min_keywords, greater_keywords, less_keywords, between_keywords,
                 negation_keywords, junction_keywords, disjunction_keywords, like_keywords, distinct_keywords,equal_keywords,
                 non_max_keywords, non_min_keywords, empty_keywords,gte_keywords,lte_keywords,non_empty_keywords,
                 explore_keywords,database_dico, database_object):
        Thread.__init__(self)
        self.where_objects = []
        self.phrases = phrases
        self.columns_of_values_of_where = columns_of_values_of_where
        self.count_keywords = count_keywords
        self.sum_keywords = sum_keywords
        self.average_keywords = average_keywords
        self.max_keywords = max_keywords
        self.min_keywords = min_keywords
        self.greater_keywords = greater_keywords
        self.less_keywords = less_keywords
        self.between_keywords = between_keywords
        self.negation_keywords = negation_keywords
        self.junction_keywords = junction_keywords
        self.disjunction_keywords = disjunction_keywords
        self.like_keywords = like_keywords
        self.distinct_keywords = distinct_keywords
        self.database_dico = database_dico
        self.database_object = database_object
        self.equal_keywords = equal_keywords
        self.non_max_keywords = non_max_keywords
        self.non_min_keywords = non_min_keywords
        self.empty_keywords = empty_keywords
        self.gte_keywords = gte_keywords
        self.lte_keywords = lte_keywords
        self.non_empty_keywords = non_empty_keywords
        self.explore_keywords = explore_keywords

    def get_tables_of_column(self, column):
        tmp_table = []
        for table in self.database_dico:
            if column in self.database_dico[table]:
                tmp_table.append(table)
        return tmp_table

    def get_column_name_with_alias_table(self, column):
        one_table_of_column = self.get_tables_of_column(column)[0]
        return str(column)

    def intersect(self, a, b):
        return list(set(a) & set(b))

    def predict_operation_type(self, previous_column_offset, current_column_offset, column_datatype):
        interval_offset = list(range(previous_column_offset, current_column_offset))
        if column_datatype == 'int':
            if (len(self.intersect(interval_offset, self.count_keyword_offset)) >= 1):
                return 'COUNT'
            elif (len(self.intersect(interval_offset, self.sum_keyword_offset)) >= 1):
                return 'SUM'
            elif (len(self.intersect(interval_offset, self.average_keyword_offset)) >= 1):
                return 'AVG'
            elif (len(self.intersect(interval_offset, self.non_max_keyword_offset)) >= 1):
                return 'IS_NOT_MAXVAL'
            elif (len(self.intersect(interval_offset, self.non_min_keyword_offset)) >= 1):
                return 'IS_NOT_MINVAL'
            elif (len(self.intersect(interval_offset, self.max_keyword_offset)) >= 1):
                return 'IS_MAXVAL'
            elif (len(self.intersect(interval_offset, self.min_keyword_offset)) >= 1):
                return 'IS_MINVAL'
            elif (len(self.intersect(interval_offset, self.non_empty_keyword_offset)) >= 1):
                return 'NOT_EMPTY'
            elif (len(self.intersect(interval_offset, self.empty_keyword_offset)) >= 1):
                return 'IS_EMPTY'
            elif (len(self.intersect(interval_offset, self.empty_keyword_offset)) >= 1):
                return 'EXPLORE'
            else:
                return None
        else:
            if (len(self.intersect(interval_offset, self.empty_keyword_offset)) >= 1):
                return 'EXPLORE'
            else:
                return None

    def predict_operator(self, current_column_offset, next_column_offset, column_datatype):
        interval_offset = list(range(current_column_offset, next_column_offset))
        if column_datatype == 'int':
            if (len(self.intersect(interval_offset, self.negation_keyword_offset)) >= 1) and (
                        len(self.intersect(interval_offset, self.greater_keyword_offset)) >= 1):
                    return 'LT'
            elif (len(self.intersect(interval_offset, self.negation_keyword_offset)) >= 1) and (
                        len(self.intersect(interval_offset, self.less_keyword_offset)) >= 1):
                    return 'GT'
            if (len(self.intersect(interval_offset, self.gte_keyword_offset)) >= 1):
                return 'GTE'
            elif (len(self.intersect(interval_offset, self.lte_keyword_offset)) >= 1):
                return 'LTE'
            elif (len(self.intersect(interval_offset, self.less_keyword_offset)) >= 1):
                return 'LT'
            elif (len(self.intersect(interval_offset, self.greater_keyword_offset)) >= 1):
                return 'GT'
            elif (len(self.intersect(interval_offset, self.between_keyword_offset)) >= 1):
                return 'IN_RANGE'
            elif (len(self.intersect(interval_offset, self.negation_keyword_offset)) >= 1):
                return 'NE'
            elif (len(self.intersect(interval_offset, self.like_keyword_offset)) >= 1):
                return 'LIKE'
            elif (len(self.intersect(interval_offset, self.equal_keyword_offset)) >=1):
                return 'EQ'
            else:
                return None
        else:
            if (len(self.intersect(interval_offset, self.equal_keyword_offset)) >=1):
                return 'EQ'
            else:
                return None

    def predict_junction(self, previous_column_offset, current_column_offset):
        interval_offset = list(range(previous_column_offset, current_column_offset))
        junction = 'AND'
        if (len(self.intersect(interval_offset, self.disjunction_keyword_offset)) >= 1):
            return 'OR'
        elif (len(self.intersect(interval_offset, self.junction_keyword_offset)) >= 1):
            return 'AND'

        first_encountered_junction_offset = -1
        first_encountered_disjunction_offset = -1

        for offset in self.junction_keyword_offset:
            if offset >= current_column_offset:
                first_encountered_junction_offset = offset
                break

        for offset in self.disjunction_keyword_offset:
            if offset >= current_column_offset:
                first_encountered_disjunction_offset = offset
                break

        if first_encountered_junction_offset >= first_encountered_disjunction_offset:
            return 'AND'
        else:
            return 'OR'

    def uniquify(self, list):
        already = []
        for element in list:
            if element not in already:
                already.append(element)
        return already

    def run(self):
        number_of_where_columns = 0
        columns_of_where = []
        offset_of = {}
        column_list = []
        value_list = []

        column_offset = []
        self.count_keyword_offset = []
        self.sum_keyword_offset = []
        self.average_keyword_offset = []
        self.max_keyword_offset = []
        self.min_keyword_offset = []
        self.greater_keyword_offset = []
        self.less_keyword_offset = []
        self.between_keyword_offset = []
        self.junction_keyword_offset = []
        self.disjunction_keyword_offset = []
        self.negation_keyword_offset = []
        self.like_keyword_offset = []
        self.equal_keyword_offset = []
        self.non_max_keyword_offset = []
        self.non_min_keyword_offset = []
        self.empty_keyword_offset = []
        self.gte_keyword_offset = []
        self.lte_keyword_offset = []
        self.non_empty_keyword_offset = []
        self.explore_keyword_offset = []
        if len(self.phrases) != 0:
            for phrase in self.phrases:
                phrase_offset_string = ''
                for i in range(0, len(phrase)):
                    for table_name in self.database_dico:
                        columns = self.database_object.get_table_by_name(table_name).get_columns()
                        for column in columns:
                            if (phrase[i] == column.name) or (phrase[i] in column.equivalences):
                                number_of_where_columns += 1
                                column_datatype = column.type
                                column_list.append(column_datatype)
                                print(column_list)
                                columns_of_where.append(column.name)
                                offset_of[phrase[i]] = i
                                column_offset.append(i)
                                break
                        else:
                            continue
                        break
                    columns_of_where = list(sorted(set(columns_of_where), key = columns_of_where.index))
                    column_offset = list(set(column_offset))
                    phrase_keyword = str(phrase[i]).lower()  # for robust keyword matching
                    phrase_offset_string += phrase_keyword + " "
                    #print(len(phrase_offset_string))
                    for keyword in self.count_keywords:
                        if keyword in phrase_offset_string :    # before the column
                            if (phrase_offset_string.find(keyword) + len(keyword) + 1 == len(phrase_offset_string) ) :
                                self.count_keyword_offset.append(i)

                    for keyword in self.sum_keywords:
                        if keyword in phrase_offset_string.split() :    # before the column
                            if (phrase_offset_string.find(keyword) + len(keyword) + 1 == len(phrase_offset_string) ) :
                                self.sum_keyword_offset.append(i)

                    for keyword in self.average_keywords:
                        if keyword in phrase_offset_string.split() :    # before the column
                            if (phrase_offset_string.find(keyword) + len(keyword) + 1 == len(phrase_offset_string) ) :
                                self.average_keyword_offset.append(i)

                    for keyword in self.max_keywords:
                        if keyword in phrase_offset_string.split() :    # before the column
                            if (phrase_offset_string.find(keyword) + len(keyword) + 1 == len(phrase_offset_string) ) :
                                self.max_keyword_offset.append(i)

                    for keyword in self.min_keywords:
                        if keyword in phrase_offset_string.split() :    # before the column
                            if (phrase_offset_string.find(keyword) + len(keyword) + 1 == len(phrase_offset_string) ) :
                                self.min_keyword_offset.append(i)

                    for keyword in self.greater_keywords:
                        if keyword in phrase_offset_string :    # after the column
                            if (phrase_offset_string.find(keyword) + len(keyword) + 1 == len(phrase_offset_string) ) :
                                self.greater_keyword_offset.append(i)

                    for keyword in self.less_keywords:
                        if keyword in phrase_offset_string :    # after the column
                            if (phrase_offset_string.find(keyword) + len(keyword) + 1 == len(phrase_offset_string) ) :
                                self.less_keyword_offset.append(i)

                    for keyword in self.between_keywords:
                        if keyword in phrase_offset_string.split() :    # after the column
                            if (phrase_offset_string.find(keyword) + len(keyword) + 1 == len(phrase_offset_string) ) :
                                self.between_keyword_offset.append(i)

                    for keyword in self.junction_keywords:
                        if keyword in phrase_offset_string.split() :    # after the column
                            if (phrase_offset_string.find(keyword) + len(keyword) + 1 == len(phrase_offset_string) ) :
                                self.junction_keyword_offset.append(i)

                    for keyword in self.disjunction_keywords:
                        if keyword in phrase_offset_string.split() :
                            if (phrase_offset_string.find(keyword) + len(keyword) + 1 == len(phrase_offset_string) ) :
                                self.disjunction_keyword_offset.append(i)

                    for keyword in self.negation_keywords:
                        if keyword in phrase_offset_string :
                            if (phrase_offset_string.find(keyword) + len(keyword) + 1 == len(phrase_offset_string) ) :
                                self.negation_keyword_offset.append(i)

                    for keyword in self.like_keywords:
                        if keyword in phrase_offset_string :    # after the column
                            if (phrase_offset_string.find(keyword) + len(keyword) + 1 == len(phrase_offset_string) ) :
                                self.like_keyword_offset.append(i)

                    for keyword in self.equal_keywords:
                        if keyword in phrase_offset_string :    # after the column
                            if (phrase_offset_string.find(keyword) + len(keyword) + 1 == len(phrase_offset_string) ) :
                                self.equal_keyword_offset.append(i)

                    for keyword in self.non_max_keywords:
                        if keyword in phrase_offset_string :    # after the column
                            if (phrase_offset_string.find(keyword) + len(keyword) + 1 == len(phrase_offset_string) ) :
                                self.non_max_keyword_offset.append(i)

                    for keyword in self.non_min_keywords:
                        if keyword in phrase_offset_string :    # after the column
                            if (phrase_offset_string.find(keyword) + len(keyword) + 1 == len(phrase_offset_string) ) :
                                self.non_min_keyword_offset.append(i)

                    for keyword in self.empty_keywords:
                        if keyword in phrase_offset_string :    # after the column
                            if (phrase_offset_string.find(keyword) + len(keyword) + 1 == len(phrase_offset_string) ) :
                                self.empty_keyword_offset.append(i)

                    for keyword in self.gte_keywords:
                        if keyword in phrase_offset_string :    # after the column
                            if (phrase_offset_string.find(keyword) + len(keyword) + 1 == len(phrase_offset_string) ) :
                                self.gte_keyword_offset.append(i)

                    for keyword in self.lte_keywords:
                        if keyword in phrase_offset_string :    # after the column
                            if (phrase_offset_string.find(keyword) + len(keyword) + 1 == len(phrase_offset_string) ) :
                                self.lte_keyword_offset.append(i)

                    for keyword in self.non_empty_keywords:
                        if keyword in phrase_offset_string :    # after the column
                            if (phrase_offset_string.find(keyword) + len(keyword) + 1 == len(phrase_offset_string) ) :
                                self.non_empty_keyword_offset.append(i)

                    for keyword in self.explore_keywords:
                        if keyword in phrase_offset_string.split() :    # after the column
                            if (phrase_offset_string.find(keyword) + len(keyword) + 1 == len(phrase_offset_string) ) :
                                self.explore_keyword_offset.append(i)
            where_object = Where()

            for i in range(0, len(column_offset)):
                current = column_offset[i]
                if i == 0:
                    previous = 0
                else:
                    previous = column_offset[i - 1]

                if i == (len(column_offset) - 1):
                    _next = 999
                else:
                    _next = column_offset[i + 1]
                junction = self.predict_junction(previous, current)
                column = self.get_column_name_with_alias_table(columns_of_where[i])
                operation_type = self.predict_operation_type(previous, current,column_datatype)
                if operation_type is not None:
                    value = 'true'
                else:
                    if len(self.columns_of_values_of_where) >= i and self.columns_of_values_of_where is not None:
                        value = self.columns_of_values_of_where[
                            len(self.columns_of_values_of_where) - len(columns_of_where) + i]
                    else:
                        value = 'OOV'  # Out Of Vocabulary: default value
                print(value)
                if value != 'true':
                    value_type = type(value)
                    value_list.append(value_type.__name__)
                else:
                    value_type = column_datatype
                    value_list.append(value_type)
                value_list = ['int' if x=='tuple' else x for x in value_list]
                print(value_list)
                operator = self.predict_operator(current, _next, column_datatype)
                if len(column) is not None:
                    where_object.add_condition(junction, Condition(column, operation_type, operator, value))
        if value_list != column_list:
            raise SystemExit(1)
        self.where_objects.append(where_object)


    def join(self):
        Thread.join(self)
        return self.where_objects


class GroupByParser(Thread):
    def __init__(self, phrases, database_dico, database_object):
        Thread.__init__(self)
        self.group_by_objects = []
        self.select_objects_exp = []
        self.phrases = phrases
        self.database_dico = database_dico
        self.database_object = database_object

    def get_tables_of_column(self, column):
        tmp_table = []
        for table in self.database_dico:
            if column in self.database_dico[table]:
                tmp_table.append(table)
        return tmp_table

    def get_column_name_with_alias_table(self, column):
        one_table_of_column = self.get_tables_of_column(column)[0]
        return str(column)

    def run(self):
        group_by_object = GroupBy()
        #print(self.phrases)
        for phrase in self.phrases:
            for i in range(0, len(phrase)):
                for table_name in self.database_dico:
                    columns = self.database_object.get_table_by_name(table_name).get_columns()
                    for column in columns:
                        if (phrase[i] == column.name) or (phrase[i] in column.equivalences):
                            column_with_alias = self.get_column_name_with_alias_table(column.name)
                            #print(column_with_alias)
                            group_by_object.set_column(column_with_alias)
                            #print(group_by_object)
        self.group_by_objects.append(group_by_object)
        #print(self.group_by_objects[0])

    def join(self):
        Thread.join(self)
        return self.group_by_objects


class OrderByParser(Thread):
    def __init__(self, phrases, asc_keywords, desc_keywords, database_dico, database_object):
        Thread.__init__(self)
        self.order_by_objects = []
        self.phrases = phrases
        self.asc_keywords = asc_keywords
        self.desc_keywords = desc_keywords
        self.database_dico = database_dico
        self.database_object = database_object

    def get_tables_of_column(self, column):
        tmp_table = []
        for table in self.database_dico:
            if column in self.database_dico[table]:
                tmp_table.append(table)
        return tmp_table

    def get_column_name_with_alias_table(self, column):
        one_table_of_column = self.get_tables_of_column(column)[0]
        return str(table_of_from) + '.' + str(column)

    def intersect(self, a, b):
        return list(set(a) & set(b))

    def predict_order(self, phrase):
        if (len(self.intersect(phrase, self.desc_keywords)) >= 1):
            return 'DESC'
        else:
            return 'ASC'

    def run(self):
        order_by_object = OrderBy()
        if len(self.phrases) != 0:
            for phrase in self.phrases:
                for i in range(0, len(phrase)):
                    for table_name in self.database_dico:
                        columns = self.database_object.get_table_by_name(table_name).get_columns()
                        for column in columns:
                            if (phrase[i] == column.name) or (phrase[i] in column.equivalences):
                                column_with_alias = self.get_column_name_with_alias_table(column.name)
                                order_by_object.add_column(column_with_alias, self.predict_order(phrase))
        self.order_by_objects.append(order_by_object)

    def join(self):
        Thread.join(self)
        return self.order_by_objects


class Parser:
    database_object = None
    database_dico = None

    count_keywords = []
    sum_keywords = []
    average_keywords = []
    max_keywords = []
    min_keywords = []
    junction_keywords = []
    disjunction_keywords = []
    greater_keywords = []
    less_keywords = []
    between_keywords = []
    order_by_keywords = []
    asc_keywords = []
    desc_keywords = []
    group_by_keywords = []
    negation_keywords = []
    equal_keywords = []
    like_keywords = []
    non_max_keywords = []
    non_min_keywords =[]
    empty_keywords =[]
    gte_keywords = []
    lte_keywords = []
    non_empty_keywords =[]
    explore_keywords = []
    all_keywords = []

    def __init__(self, database, config):
        self.database_object = database
        self.database_dico = self.database_object.get_tables_into_dictionary()

        self.count_keywords = config.get_count_keywords()
        self.sum_keywords = config.get_sum_keywords()
        self.average_keywords = config.get_avg_keywords()
        self.max_keywords = config.get_max_keywords()
        self.min_keywords = config.get_min_keywords()
        self.junction_keywords = config.get_junction_keywords()
        self.disjunction_keywords = config.get_disjunction_keywords()
        self.greater_keywords = config.get_greater_keywords()
        self.less_keywords = config.get_less_keywords()
        self.between_keywords = config.get_between_keywords()
        self.order_by_keywords = config.get_order_by_keywords()
        self.asc_keywords = config.get_asc_keywords()
        self.desc_keywords = config.get_desc_keywords()
        self.group_by_keywords = config.get_group_by_keywords()
        self.negation_keywords = config.get_negation_keywords()
        self.equal_keywords = config.get_equal_keywords()
        self.like_keywords = config.get_like_keywords()
        self.distinct_keywords = config.get_distinct_keywords()
        self.non_max_keywords = config.get_non_max_keywords()
        self.non_min_keywords = config.get_non_min_keywords()
        self.empty_keywords = config.get_empty_keywords()
        self.gte_keywords = config.get_gte_keywords()
        self.lte_keywords = config.get_lte_keywords()
        self.non_empty_keywords = config.get_non_empty_keywords()
        self.explore_keywords = config.get_explore_keywords()
        self.all_keywords = config.get_all_keywords()


    @staticmethod
    def _myCmp(s1,s2):
        if len(s1.split()) == len(s2.split()) :
            if len(s1) >= len(s2) :
                return 1
            else:
                return -1
        else:
            if len(s1.split()) >= len(s2.split()):
                return 1
            else:
                return -1

    def is_date(string, fuzzy=False):
        """
        Return whether the string can be interpreted as a date.

        :param string: str, string to check for date
        :param fuzzy: bool, ignore unknown tokens in string if True
        """
        try:
            parse(string, fuzzy=fuzzy)
            return True

        except ValueError:
            return False

    @classmethod
    def transformation_sort(cls,transition_list):
        # Sort on basis of two keys split length and then token lengths in the respective priority.
        return sorted(transition_list, key=functools.cmp_to_key(cls._myCmp),reverse=True)


    def remove_accents(self, string):
        nkfd_form = unicodedata.normalize('NFKD', str(string))
        return "".join([c for c in nkfd_form if not unicodedata.combining(c)])

    def parse_sentence(self, sentence, stopwordsFilter=None):
        sys.tracebacklimit = 0  # Remove traceback from Exception
        number_of_select_column = 0
        number_of_where_column = 0
        last_table_position = 0
        columns_of_select = []
        columns_of_where = []
        #print(self.database_dico)
        if stopwordsFilter is not None:
            sentence = stopwordsFilter.filter(sentence)

        input_for_finding_value = sentence.rstrip(string.punctuation.replace('"', '').replace("'", ""))
        columns_of_values_of_where = []

        filter_list = [",", "!"]

        for filter_element in filter_list:
            input_for_finding_value = input_for_finding_value.replace(filter_element, " ")

        input_word_list = input_for_finding_value.split()

        number_of_where_column_temp = 0
        last_table_position_temp = 0
        med_phrase = ''
        all_key = 0
        for word in input_word_list:
            if word in self.all_keywords:
                all_key += 1

        if all_key == 0:
            print('Invalid Sentence')
            raise SystemExit(1)
        # TODO: merge this part of the algorithm (detection of values of where)
        #  in the rest of the parsing algorithm (about line 725) '''

        for i in range(0, len(input_word_list)):
            for table_name in self.database_dico:
                columns = self.database_object.get_table_by_name(table_name).get_columns()
                for column in columns:
                    if (input_word_list[i] == column.name) or (input_word_list[i] in column.equivalences):
                        if number_of_where_column_temp == 0:
                            last_table_position_temp = 1
                            med_phrase = input_word_list[:last_table_position_temp + 1]
                        number_of_where_column_temp += 1
                        break
                    else:
                        if (number_of_where_column_temp == 0) and (
                                    i == (len(input_word_list) - 1)):
                            med_phrase = input_word_list[:]
                else:
                    continue
                break
        if 'explore' in input_word_list:
            med_phrase = input_word_list[:]
        end_phrase = input_word_list[len(med_phrase):]
        irext = ' '.join(end_phrase)

        ''' @todo set this part of the algorithm (detection of values of where) in the WhereParser thread '''

        if irext:
            irext = self.remove_accents(irext.lower())

            filter_list = [",", "!"]

            for filter_element in filter_list:
                irext = irext.replace(filter_element, " ")

            assignment_list = self.equal_keywords + self.like_keywords + self.greater_keywords + self.less_keywords + self.negation_keywords + self.gte_keywords + self.lte_keywords
            # As these words can also be part of assigners

            # custom operators added as they can be possibilities
            assignment_list.append(':')
            assignment_list.append('to')
            assignment_list.append('=')
            for key in self.between_keywords:
                if key in irext:
                    assignment_list.append('and')
            # Algorithmic logic for best substitution for extraction of values with the help of assigners.
            assignment_list = self.transformation_sort(assignment_list)

            maverickjoy_general_assigner = "*res*@3#>>*"
            maverickjoy_like_assigner = "*like*@3#>>*"

            for idx, assigner in enumerate(assignment_list):
                if assigner in self.like_keywords:
                    assigner = str(" " + assigner + " ")
                    irext = irext.replace(assigner, str(" " + maverickjoy_like_assigner + " "))
                else:
                    assigner = str(" " + assigner + " ")
                    # Reason for adding " " these is according to the LOGIC implemented assigner operators help us extract the value,
                    # hence they should be independent entities not part of some other big entity else logic will fail.
                    # for eg -> "show data for city where cityName where I like to risk my life  is Pune" will end up extacting ,
                    # 'k' and '1' both. I know its a lame sentence but something like this could be a problem.

                    irext = irext.replace(assigner, str(" " + maverickjoy_general_assigner + " "))

            # replace all spaces from values to <_> for proper value assignment in SQL
            # eg. (where name is 'abc def') -> (where name is abc<_>def)
            for i in re.findall("(['\"].*?['\"])", irext):
                irext = irext.replace(i, i.replace(' ', '<_>').replace("'", '').replace('"', ''))

            irext_list = irext.split()
            for idx, x in enumerate(irext_list):
                index = idx + 1
                idx_prev = idx - 2
                if x == maverickjoy_like_assigner:
                    if index < len(irext_list) and irext_list[index] != maverickjoy_like_assigner and irext_list[index] !=\
                            maverickjoy_general_assigner:
                        # replace back <_> to spaces from the values assigned
                        if irext_list[idx_prev] in self.between_keywords:
                            if irext_list[index].isdigit() == True:
                                columns_of_values_of_where.append((int(irext_list[idx]), int(irext_list[index])))
                        else:
                            if irext_list[index].isdigit() == True:
                                columns_of_values_of_where.append(int(irext_list[index].replace('<_>', ' ')))
                        #elif self.is_date(irext_list[index]) == True:
                        #    columns_of_values_of_where.append(int(irext_list[index].replace('<_>', ' ')))
                            else:
                                columns_of_values_of_where.append(str(irext_list[index].replace('<_>', ' ')))
                if x == maverickjoy_general_assigner:
                    if index < len(irext_list) and irext_list[index] != maverickjoy_like_assigner and irext_list[index] != \
                            maverickjoy_general_assigner:
                        # replace back <_> to spaces from the values assigned
                        if irext_list[idx_prev] in self.between_keywords:
                            if irext_list[index].isdigit() == True:
                                columns_of_values_of_where.append((int(irext_list[idx-1]), int(irext_list[index])))
                        else:
                            if irext_list[index].isdigit() == True:
                                columns_of_values_of_where.append(int(irext_list[index].replace('<_>', ' ')))
                        #elif self.is_date(irext_list[index]) == True:
                        #    columns_of_values_of_where.append(int(irext_list[index].replace('<_>', ' ')))
                            else:
                                columns_of_values_of_where.append(str(irext_list[index].replace('<_>', ' ')))
        ''' ----------------------------------------------------------------------------------------------------------- '''
        #print(columns_of_values_of_where)
        select_phrase = ''
        from_phrase = ''
        where_phrase = ''
        where_group_phrase = ''

        words = re.findall(r"[\w]+", self.remove_accents(sentence))
        select_phrase = words[:len(med_phrase)]
        last_table_position = len(med_phrase)

        if 'explore' not in words:
            for i in range(0, len(words)):
                for table_name in self.database_dico:
                        columns = self.database_object.get_table_by_name(table_name).get_columns()
                        for column in columns:
                            if (words[i] == column.name) or (words[i] in column.equivalences):
                                    columns_of_select.append(column.name)
                                    number_of_select_column += 1
                                    if number_of_where_column == 0:
                                        from_phrase = words[len(select_phrase):last_table_position + 1]
                                    columns_of_where.append(column.name)
                                    number_of_where_column += 1
                                    break
                            else:
                                if (number_of_where_column == 0) and (i == (len(words) - 1)):
                                    from_phrase = words[len(select_phrase):]
        else:
            for i in range(0,len(words)):
                for table_name in self.database_dico:
                    columns = self.database_object.get_table_by_name(table_name).get_columns()
                    for column in columns:
                        if (words[i] == column.name) or (words[i] in column.equivalences):
                            columns_of_select.append(column.name)
                            number_of_select_column += 1
                            break
        if 'explore' in words:
            where_phrase = []
        else:
            where_phrase = words[len(select_phrase):]
        where_group_phrase = words[:len(select_phrase)]
        #print(columns_of_select)
        #if (number_of_select_column  + number_of_where_column) == 0:
        #    raise ParsingException("No keyword found in sentence!")
        #tables_of_from = self.database_object.get_table_with_this_column(columns_of_select[0])[0].split(' ')
        if len(columns_of_select) > 0:
            columns_of_select = list(set(columns_of_select))
            column_of_select = columns_of_select[0].split(' ')
        if len(columns_of_where) > 0:
            column_of_where = columns_of_where[0].split(' ')

        from_phrases = []
        previous_index = 0
        for i in range(0, len(from_phrase)):
            from_phrases.append(from_phrase[previous_index:i + 1])
            previous_index = i + 1
        last_junction_word_index = -1


        for i in range(0, len(from_phrases)):
            number_of_junction_words = 0
            number_of_disjunction_words = 0

            for word in from_phrases[i]:
                if word in self.junction_keywords:
                    number_of_junction_words += 1
                if word in self.disjunction_keywords:
                    number_of_disjunction_words += 1

            if (number_of_junction_words + number_of_disjunction_words) > 0:
                last_junction_word_index = i

        if last_junction_word_index == -1:
            from_phrase = sum(from_phrases[:1], [])
            where_phrase = sum(from_phrases[1:], []) + where_phrase
        else:
            from_phrase = sum(from_phrases[:last_junction_word_index + 1], [])
            where_phrase = sum(from_phrases[last_junction_word_index + 1:], []) + where_phrase
        group_by_phrase = []
        order_by_phrase = []
        new_where_phrase = []
        previous_index = 0
        previous_phrase_type = 0
        yet_where = 0
        if 'explore' in where_group_phrase:
            if len(where_phrase) == 0:
                for i in range(0, len(where_group_phrase)):
                    if where_group_phrase[i] in self.order_by_keywords:
                        if yet_where > 0:
                            if previous_phrase_type == 1:
                                order_by_phrase.append(where_group_phrase[previous_index:i])
                            elif previous_phrase_type == 2:
                                group_by_phrase.append(where_group_phrase[previous_index:i])
                        else:
                            new_where_phrase.append(where_phrase[previous_index:i])
                        previous_index = i
                        yet_where += 1
                    if where_group_phrase[i] in self.group_by_keywords or where_group_phrase[i] in self.explore_keywords:
                        if yet_where > 0:
                            if previous_phrase_type == 1:
                                order_by_phrase.append(where_group_phrase[previous_index:i])
                            elif previous_phrase_type == 2:
                                group_by_phrase.append(where_group_phrase[previous_index:i])
                        else:
                            new_where_phrase.append(where_phrase[previous_index:i])
                            previous_phrase_type = 1
                        previous_index = i
                        previous_phrase_type = 2
                        yet_where += 1

                if previous_phrase_type == 1:
                    order_by_phrase.append(where_group_phrase[previous_index:])
                elif previous_phrase_type == 2:
                    group_by_phrase.append(where_group_phrase[previous_index:])
                else:
                    new_where_phrase.append(where_phrase)
            else:
                for i in range(0, len(where_group_phrase)):
                    for j in range(0, len(where_phrase)):
                        if where_group_phrase[i] in self.order_by_keywords:
                            if yet_where > 0:
                                if previous_phrase_type == 1:
                                    order_by_phrase.append(where_group_phrase[previous_index:i])
                                elif previous_phrase_type == 2:
                                    group_by_phrase.append(where_group_phrase[previous_index:i])
                            else:
                                new_where_phrase.append(where_phrase[previous_index:i])
                            previous_index = i
                            yet_where += 1
                        if where_phrase[j] in self.group_by_keywords:
                            if yet_where > 0:
                                if previous_phrase_type == 1:
                                    order_by_phrase.append(where_phrase[previous_index:j])
                                elif previous_phrase_type == 2:
                                    group_by_phrase.append(where_phrase[previous_index:j])
                                    previous_phrase_type = 1
                            else:
                                new_where_phrase.append(where_phrase[previous_index:j])
                            previous_index = i
                            previous_phrase_type = 2
                            yet_where += 1

                    if previous_phrase_type == 1:
                        order_by_phrase.append(where_phrase[previous_index:])
                    elif previous_phrase_type == 2:
                        group_by_phrase.append(where_phrase[previous_index:])
                    else:
                        new_where_phrase.append(where_phrase)
                temp_phrase = select_phrase
                select_phrase = list(set().union(*group_by_phrase))
                select_phrase.insert(0,'explore')
                group_by_phrase = []
                group_by_phrase.append(input_word_list)
                columns_of_select = []
                for table_name in self.database_dico:
                    columns = self.database_object.get_table_by_name(table_name).get_columns()
                    for word in select_phrase:
                        for column in columns:
                            if word == column.name:
                                columns_of_select.append(column.name)
                columns_of_where = []
                new_where_phrase = []

        else:
            for i in range(0, len(where_phrase)):
                if where_phrase[i] in self.order_by_keywords:
                    if yet_where > 0:
                        if previous_phrase_type == 1:
                            order_by_phrase.append(where_phrase[previous_index:i])
                        elif previous_phrase_type == 2:
                            group_by_phrase.append(where_phrase[previous_index:i])
                    else:
                        new_where_phrase.append(where_phrase[previous_index:i])
                    previous_index = i
                    yet_where += 1
                if where_phrase[i] in self.group_by_keywords or where_phrase[i] in self.explore_keywords:
                    if yet_where > 0:
                        if previous_phrase_type == 1:
                            order_by_phrase.append(where_phrase[previous_index:i])
                        elif previous_phrase_type == 2:
                            group_by_phrase.append(where_phrase[previous_index:i])
                            previous_phrase_type = 1
                    else:
                        new_where_phrase.append(where_phrase[previous_index:i])
                    previous_index = i
                    previous_phrase_type = 2
                    yet_where += 1

            if previous_phrase_type == 1:
                order_by_phrase.append(where_phrase[previous_index:])
            elif previous_phrase_type == 2:
                group_by_phrase.append(where_phrase[previous_index:])
            else:
                new_where_phrase.append(where_phrase)
#        print(where_phrase)

        if len(from_phrase) + len(where_phrase) + len(group_by_phrase) + len(order_by_phrase) == 0:
            print('Invalid Sentence')
            raise SystemExit(1)

        try:

            where_parser = WhereParser(new_where_phrase, columns_of_values_of_where,
                                       self.count_keywords, self.sum_keywords, self.average_keywords, self.max_keywords,
                                       self.min_keywords, self.greater_keywords, self.less_keywords,
                                       self.between_keywords, self.negation_keywords, self.junction_keywords,
                                       self.disjunction_keywords, self.like_keywords, self.distinct_keywords,self.equal_keywords,
                                       self.non_max_keywords, self.non_min_keywords, self.empty_keywords,
                                       self.gte_keywords, self.lte_keywords, self.non_empty_keywords,
                                       self.explore_keywords,self.database_dico, self.database_object)
            select_parser = SelectParser(columns_of_select, select_phrase, self.count_keywords,
                                         self.sum_keywords, self.average_keywords, self.max_keywords, self.min_keywords,
                                         self.distinct_keywords,self.non_max_keywords, self.non_min_keywords, self.empty_keywords,self.non_empty_keywords,
                                         self.explore_keywords, self.database_dico, self.database_object)
            from_parser = FromParser(columns_of_select, columns_of_where, self.database_object)
            group_by_parser = GroupByParser(group_by_phrase, self.database_dico, self.database_object)
            order_by_parser = OrderByParser(order_by_phrase, self.asc_keywords, self.desc_keywords,
                                            self.database_dico, self.database_object)

            where_parser.start()
            select_parser.start()
            from_parser.start()
            group_by_parser.start()
            order_by_parser.start()

            queries = from_parser.join()
            #print(queries[0])
        except:
            raise ParsingException("Parsing error occured in thread!")

        if isinstance(queries, ParsingException):
            raise queries

        try:
            where_objects = where_parser.join()
            select_objects = select_parser.join()
            group_by_objects = group_by_parser.join()
            order_by_objects = order_by_parser.join()
            #print(where_objects[0])
            #print(select_objects[0])
            #print(group_by_objects[0])
            #print(order_by_objects[0])
        except:
            raise ParsingException("Parsing error occured in thread!")

        for i in range(0, len(queries)):
            query = queries[i]
            if 'explore' in input_word_list:
                if select_objects[i] is not None:
                    query.set_select(select_objects[i])
                if group_by_objects[i] is not None:
                    query.set_group_by(group_by_objects[i])
            else:
                if where_objects[i] is not None:
                    query.set_where(where_objects[i])
                if select_objects[i] is not None:
                    query.set_select(select_objects[i])
                if group_by_objects[i] is not None:
                    query.set_group_by(group_by_objects[i])
                if order_by_objects[i] is not None:
                    query.set_order_by(order_by_objects[i])
        return queries
