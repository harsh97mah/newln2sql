#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May  9 12:47:17 2019

@author: harsh97mah
"""

from .constants import Color
import json


class From():
    table = ''

    def __init__(self, table=None):
        if table is not None:
            self.table = table
        else:
            self.table = ''

    def set_table(self, table):
        self.table = table

    def get_table(self):
        return self.table

    def __str__(self):
        return ''

    def print_json(self, output):
        if self.table != '':
            output.write('\t"from": {\n')
            output.write('\t\t"table": "' + str(self.table) + '"\n')
            output.write('\t},\n')
        else:
            output.write('\t')

class Join():
    tables = []
    links = []

    def __init__(self):
        self.tables = []
        self.links = []

    def add_table(self, table):
        if table not in self.tables:
            self.tables.append(table)

    def set_links(self, links):
        self.links = links

    def get_tables(self):
        return self.tables

    def get_links(self):
        return self.links

    def __str__(self):
        if len(self.links) >= 1:
            string = ''
            for i in range(0, len(self.links)):
                string += '\n' + str('INNER JOIN ')  + str(
                    self.links[i][1][0]) + '\n' + str('ON ') + str(self.links[i][0][0]) + str('.') + str(
                    self.links[i][0][1]) + str(' = ') + str(self.links[i][1][0]) + str('.') + str(self.links[i][1][1])
            return string
        elif len(self.tables) >= 1:
            if len(self.tables) == 1:
                return '\n' + str('NATURAL JOIN ') + self.tables[0]
            else:
                string = '\n' + str('NATURAL JOIN ')
                for i in range(0, len(self.tables)):
                    if i == (len(self.tables) - 1):
                        string += str(self.tables[i])
                    else:
                        string += str(self.tables[i]) + ', '
                return string
        else:
            return ''

    def print_json(self, output):
        if len(self.tables) >= 1:
            if len(self.tables) == 1:
                output.write('\t"join": {\n')
                output.write('\t\t"table": "' + str(self.tables[0]) + '"\n')
                output.write('\t},\n')
            else:
                output.write('\t"join": {\n')
                output.write('\t\t"tables": [')
                for i in range(0, len(self.tables)):
                    if i == (len(self.tables) - 1):
                        output.write('"' + str(self.tables[i]) + '"')
                    else:
                        output.write('"' + str(self.tables[i]) + '", ')
                output.write(']\n')
                output.write('\t},\n')
        else:
            output.write('')

class Condition():
    column = ''
    column_type = ''
    operator = ''
    value = ''

    def __init__(self, column, column_type, operator, value):
        self.column = column
        self.column_type = column_type
        self.operator = operator
        self.value = value

    def get_column(self):
        return self.column

    def get_column_type(self):
        return self.column_type

    def get_operator(self):
        return self.operator

    def get_value(self):
        return self.value

    def get_in_list(self):
        return [self.column, self.column_type, self.operator, self.value]

    def get_just_column_name(self, column):
        #if column != str(None):
        #    return column.rsplit('.', 1)[1]
        #else:
            return column

    def get_column_with_type_operation(self, column):
        #if column_type is None:
        #    return self.column
        #else:
            return self.column

    def get_pretty_operator(self, operator, column_type):
        if column_type is None:
            if operator == 'BETWEEN':
                return str('BETWEEN') + str(' OOV ')  + str('AND')
            else:
                return operator
        else:
            return column_type
    def __str__(self):
        return str(self.get_column_with_type_operation(self.column)) + ': {' + str(
            self.get_pretty_operator(self.operator, self.column_type)) + ': ' + str(self.value) + '}'

    def print_json(self, output):
        if self.operator:
            output.write('\t{' + str(self.column) + ': {' + str(self.operator) + ': ' + str(self.value) + '}' + '}')
        else:
            output.write('\t{' + str(self.column) + ': {' + str(self.column_type)+ ': true' +'}'+'}')

class Where():
    conditions = []

    def __init__(self, clause=None):
        if clause is not None:
            self.conditions.append([None, clause])
        else:
            self.conditions = []

    def add_condition(self, junction, clause):
        self.conditions.append([junction, clause])


    def get_conditions(self):
        return self.conditions

    def __str__(self):
        string = ''
        #print(self.conditions[0][1])
        #print(self.conditions)
        if len(self.conditions) == 1:
            string += '\n'  + str('CONDITION') + str(': {')+ str(self.conditions[0][1]) + '},'
            return string
        elif len(self.conditions) > 1:
            for i in range(0, len(self.conditions)):
                if i == 0:
                    string += '\n' + str('CONDITION: {' + str(self.conditions[i][0]) + ':[{') + str(self.conditions[i][1]) + '}'
                else:
                    string += ',{' + str(self.conditions[i][1]) + '}'

            return string + '],'
        else:
            return ''

    def print_json(self, output):
        if len(self.conditions) >= 1:
            if len(self.conditions) == 1:
                output.write('\t\t\t\tCONDITION : ')
                self.conditions[0][1].print_json(output)
                output.write('\n')
            else:
                output.write('\t\t\t\tCONDITION: {')
                for i in range(0, len(self.conditions)):
                    if i == 0:
                        output.write(str(self.conditions[i][0])+' :[\n\t\t\t\t\t\t\t')
                    self.conditions[i][1].print_json(output)
                    if i != len(self.conditions)-1:
                        output.write(',')
                output.write('\n\t\t\t\t\t\t\t\t\t\t\t\t\t]\n')
                output.write('\t\t\t\t\t\t\t\t\t\t}\n')
        else:
            output.write('')

class GroupBy():
    column = None

    def __init__(self, column=None):
        if column is not None:
            self.column = column
        else:
            self.column = None

    def set_column(self, column):
        self.column = column

    def get_column(self):
        return self.column

    def get_just_column_name(self, column):
            return column

    def __str__(self):
        #print(self.column)
        if self.column is not None:
            return '\n' + str('GROUP BY: [{COLUMN: "') + self.get_just_column_name(str(self.column)) + '", INTERNAL_NAME: "' + self.get_just_column_name(str(self.column)) + '"}],'
        else:
            return ''

    def print_json(self, output):
        if self.column is not None:
            output.write('\tGROUP_BY: [{')
            output.write('COLUMN: "' + self.get_just_column_name(str(self.column)) + '", INTERNAL_NAME: ' + self.get_just_column_name(str(self.column)) + '"')
            output.write('}],\n')
        else:
            output.write('')

class Select():
    group = GroupBy()

    def __init__(self, phrase):
        self.columns = []
        self.phrase = phrase

    def add_column(self, column, column_type):
        if [column, column_type] not in self.columns:
            self.columns.append([column, column_type])

    def get_columns(self):
        return self.columns

    def get_just_column_name(self, column):
            return column

    #def print_column(self, selection):
    #    column = selection[0]
    #    column_type = selection[1]
    #    if column is None:
    #        if column_type is not None:
    #            if 'COUNT' in column_type:
    #                return str('COUNT(') +str('ALL')+ str(')')
    #            else:
    #                return 'ALL'
    #        else:
    #            return 'ALL'
    #    else:
    #        if len(column_type) == 1:
    #            if 'DISTINCT' in column_type:
    #                if 'COUNT' in column_type:
    #                    return str('COUNT(DISTINCT ')+ str(column)+ str(')')
    #                else:
    #                    return str('DISTINCT ') +str(column)
    #            if 'COUNT' in column_type:
    #                return str('COUNT(')+str(column)+str(')')
    #            elif 'AVG' in column_type:
    #                return str('AVG(')+ str(column)+ str(')')
    #            elif 'SUM' in column_type:
    #                return str('SUM(') + str(column)+ str(')')
    #            elif 'MAX' in column_type:
    #                return str('MAX(') + str(column)+ str(')')
    #            elif 'MIN' in column_type:
    #                return str('MIN(') + str(column)+ str(')')
    #            else:
    #                return str(column)
    #        else:
    #            return '*'

    def __str__(self):
        select_string = ''
        if 'explore' in self.phrase:
            for i in range(1,len(self.columns[1][1][0])):
                if i == 1:
                    select_string += ' ['
                select_string = select_string + str('{FUNCTION: "') + str(self.columns[1][1][0][i]) + str('", AS: "') + str(self.columns[1][1][0][i]) + str('", INTERNAL_NAME: "') + str(self.columns[1][1][0][i]).lower() + str('", COLUMN: "') + str(self.columns[1][0]) + str('"}')
                if i != len(self.columns[1][1][0])-1:
                    select_string += ',\n'
            select_string +=  '],\n'
        else:
            select_string += ' "ALL",\n'
        return '\n' + str('SELECT:') + select_string

    def print_json(self, output):
        self.column_exp  = ' '.join([str(item) for sublist in self.columns for item in sublist])
        #print(self.columns)
        if len(self.columns) >= 1:
            if ('exp' in self.column_exp) != True:
                if len(self.columns) == 1:
                        output.write('\t\t\t\tSELECT: "ALL"\n')
                else:
                    output.write('\t\t\t\tSELECT: [')
                    for i in range(len(self.columns)):
                        output.write('{COLUMN : "' + str(self.columns[0][0]) + '"}\n')
                        if i != (len(self.columns) -1):
                            output.write(',')
                        else:
                            output.write(']')
            else:
                if self.columns[1][0] == self.group.column :
                    output.write('\t\t\t\tSELECT: [\n')
                    for i in range(1,len(self.columns[1][1][0])):
                        output.write('{FUNCTION : "'+ str(self.columns[1][1][0][i]) + '", ' + 'AS : "' +
                        str(self.columns[1][1][0][i]).lower() + '", INTERNAL_NAME : "'
                        + str(self.columns[1][1][0][i]).lower() + '"}')
                        if i != (len(self.columns[1][1][0]) -1):
                            output.write(',\n')
                        else:
                            output.write(']\n')
                else:
                    output.write('\t\t\t\tSELECT: [\n')
                    for i in range(1,len(self.columns[1][1][0])):
                        if self.columns[1][1][0][i] == 'PERCENTAGE':
                            output.write('{FUNCTION : "'+ str(self.columns[1][1][0][i]) + '", ' + 'AS : "' +
                             str(self.columns[1][1][0][i]).lower() + '", INTERNAL_NAME : "'
                            + str(self.columns[1][1][0][i]).lower() + '"}')
                        else:
                            output.write('{FUNCTION : "'+ str(self.columns[1][1][0][i]) + '", ' + 'AS : "' +
                            str(self.columns[1][1][0][i]) + '", INTERNAL_NAME : "'
                            + str(self.columns[1][1][0][i]).lower() + '", COLUMN : "' + str(self.columns[1][0]) + '"}')
                        if i != (len(self.columns[1][0]) -1):
                            output.write(',\n')
                        else:
                            output.write(']\n')
        else:
            output.write('')

class OrderBy():
    columns = []

    def __init__(self):
        self.columns = []

    def add_column(self, column, order):
        if [column, order] not in self.columns:
            self.columns.append([column, order])

    def get_columns(self):
        return self.columns

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

    def print_json(self, output):
        if len(self.columns) >= 1:
            if len(self.columns) == 1:
                output.write('\t"select": {\n')
                output.write('\t\t"column": "' + str(self.columns[0][0]) + '",\n')
                output.write('\t\t"order": "' + str(self.columns[0][1]) + '"\n')
                output.write('\t},\n')
            else:
                output.write('\t"select": {\n')
                output.write('\t\t"columns": [\n')
                for i in range(0, len(self.columns)):
                    if i == (len(self.columns) - 1):
                        output.write('\t\t\t{ "column": "' + str(self.columns[i][0]) + '",\n')
                        output.write('\t\t\t  "order": "' + str(self.columns[i][1]) + '"\n')
                        output.write('\t\t\t}\n')
                    else:
                        output.write('\t\t\t{ "column": "' + str(self.columns[i][0]) + '",\n')
                        output.write('\t\t\t  "order": "' + str(self.columns[i][1]) + '"\n')
                        output.write('\t\t\t},\n')
                output.write('\t\t]\n')
                output.write('\t},\n')
        else:
            output.write('')


class Query():
    select = None
    _from = None
    join = None
    where = None
    group_by = None
    order_by = None


    def __init__(self, select=None, _from=None, join=None, where=None, group_by=None, order_by=None):
        if select is not None:
            self.select = select
        else:
            self.select = None
        if _from is not None:
            self._from = _from
        else:
            self._from = None
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

    def set_from(self, _from):
        self._from = _from

    def get_from(self):
        return self._from

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
        string = '\nparam: {'
        if self.group_by is not None:
            string += str(self.group_by)
        if self.where is not None:
            string += str(self.where)
        if self.select is not None:
            string += str(self.select)
        if self.join is not None:
            string += str(self.join)
        if self.order_by is not None:
            string += str(self.order_by)
        string += 'WORKSPACE_ID: 3013\n}'
        return string

    def print_json(self, filename="output.json"):
        output = open(filename, 'a')
        output.write('param: ')
        output.write('{\n')
        if self.where is not None:
            self.where.print_json(output)
        if self.group_by is not None:
            self.group_by.print_json(output)
        if self.select is not None:
            self.select.print_json(output)
        if self.join is not None:
            self.join.print_json(output)
        if self.order_by is not None:
            self.order_by.print_json(output)
        output.write('WORKSPACE_ID: 3013\n')
        output.write('}\n')
        output.close()
