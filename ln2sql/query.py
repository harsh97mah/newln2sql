#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May  9 12:47:17 2019

@author: harsh97mah
"""

import json
import logging

log = logging.getLogger(__name__)

class Join():
    tables = []
    links = []

    def __init__(self):
        self.tables = []
        self.links = []

    def set_links(self, links):
        self.links = links

    # def __str__(self):
    #     if len(self.links) >= 1:
    #         string = ''
    #         for i in range(0, len(self.links)):
    #             string += '\n' + str('INNER JOIN ')  + str(
    #                 self.links[i][1][0]) + '\n' + str('ON ') + str(self.links[i][0][0]) + str('.') + str(
    #                 self.links[i][0][1]) + str(' = ') + str(self.links[i][1][0]) + str('.') + str(self.links[i][1][1])
    #         return string
    #     elif len(self.tables) >= 1:
    #         if len(self.tables) == 1:
    #             return '\n' + str('NATURAL JOIN ') + self.tables[0]
    #         else:
    #             string = '\n' + str('NATURAL JOIN ')
    #             for i in range(0, len(self.tables)):
    #                 if i == (len(self.tables) - 1):
    #                     string += str(self.tables[i])
    #                 else:
    #                     string += str(self.tables[i]) + ', '
    #             return string
    #     else:
    #         return ''

class Condition():
    internal_name = ''
    column_type = ''
    operator = ''
    value = ''
    datatype = ''

    def __init__(self, internal_name, column_type, operator, value, datatype):
        self.internal_name = internal_name
        self.column_type = column_type
        self.operator = operator
        self.value = value
        self.datatype = datatype

    def get_pretty_operator(self, operator, column_type):
        if column_type is None:
            if operator == 'BETWEEN':
                return str('BETWEEN') + str(' OOV ')  + str('AND')
            else:
                return operator
        else:
            return column_type

    def __str__(self):
        if self.datatype == 'TEXT':
            return '"' + str(self.internal_name) + '": {"' + str(
                self.get_pretty_operator(self.operator, self.column_type)) + '": ' + str(self.value) + '}'
        elif self.datatype == 'NUMERIC':
            return '"' + str(self.internal_name) + '": {"' + str(
                self.get_pretty_operator(self.operator, self.column_type)) + '": ' + str(self.value) + '}'

class Where():
    conditions = []

    def __init__(self, clause=None):
        if clause is not None:
            self.conditions.append([None, clause])
        else:
            self.conditions = []

    def add_condition(self, junction, clause):
        self.conditions.append([junction, clause])

    def __str__(self):
        string = ''
        if len(self.conditions) == 1:
            string += str('"CONDITION"') + str(': {')+ str(self.conditions[0][1]) + '},'
            return string

        elif len(self.conditions) > 1:
            for i in range(0, len(self.conditions)):
                if i == 0:
                    string += str('"CONDITION": {"' + str(self.conditions[i][0]) + '": [{') + str(self.conditions[i][1]) + '}'
                else:
                    string += ',{' + str(self.conditions[i][1]) + '}'

            return string + ']},'
        else:
            return ''

class GroupBy():
    column = None
    internal_name = None
    def __init__(self, internal_name=None):
        if internal_name is not None:
            self.internal_name = internal_name
        else:
            self.internal_name = None

    def set_column(self, internal_name):
        self.internal_name = internal_name

    def __str__(self):
        if self.internal_name is not None:
            return str('"PIVOT": {"GROUP_BY": [{"COLUMN": "') + str(self.internal_name) + '", "INTERNAL_NAME": "value"}],'
        else:
            return ''

class Select():
    group = GroupBy()

    def __init__(self, phrase):
        self.columns = []
        self.phrase = phrase

    def add_column(self, column, column_type):
        if [column, column_type] not in self.columns:
            self.columns.append([column, column_type])

    def __str__(self):
        select_string = ''
        if 'explore' in self.phrase:
            for i in range(1,len(self.columns[1][1][0])):
                if i == 1:
                    select_string += ' ['
                select_string = select_string + str('{"FUNCTION": "') + str(self.columns[1][1][0][i][0]) + str('", "AS": "') + str(self.columns[1][1][0][i][1]) + str('", "INTERNAL_NAME": "') + str(self.columns[1][1][0][i][2]) + str('"}')
                if i != len(self.columns[1][1][0])-1:
                    select_string += ','
            select_string +=  ']},'
        else:
            select_string += ' "ALL",'
        return str('"SELECT":') + select_string

class OrderBy():
    columns = []

    def __init__(self):
        self.columns = []

    def add_column(self, column, order):
        if [column, order] not in self.columns:
            self.columns.append([column, order])

    def __str__(self):
        if self.columns != []:
            string =  'ORDER BY '
            for i in range(0, len(self.columns)):
                if i == (len(self.columns) - 1):
                    string += self.columns[i][0] + ' '  + self.columns[i][1]
                else:
                    string += self.columns[i][0] + ' ' + self.columns[i][1]  + ', '
            return '\n' + string
        else:
            return ''

class Query():
    select = None
    join = None
    where = None
    group_by = None
    order_by = None


    def __init__(self, select=None, join=None, where=None, group_by=None, order_by=None):
        if select is not None:
            self.select = select
        else:
            self.select = None
        if join is not None:
            self.join = join
        else:
            self.join = None
        if where is not None:
            self.where = where
        else:
            self.where = None
        if group_by is not None:
            self.group_by = group_by
        else:
            self.group_by = None
        if order_by is not None:
            self.order_by = order_by
        else:
            self.order_by = None

    def set_select(self, select):
        self.select = select

    def get_select(self):
        return self.select

    def set_join(self, join):
        self.join = join

    def get_join(self):
        return self.join

    def set_where(self, where):
        self.where = where

    def get_where(self):
        return self.where

    def set_group_by(self, group_by):
        self.group_by = group_by

    def get_group_by(self):
        return self.group_by

    def set_order_by(self, order_by):
        self.order_by = order_by

    def get_order_by(self):
        return self.order_by

    def __str__(self):
        string = ""
        if self.group_by is not None:
            string += str(self.group_by)
        if self.where is not None:
            string += str(self.where)
        if self.select is not None:
            string += str(self.select)
        if self.order_by is not None:
            string += str(self.order_by)
        return string
