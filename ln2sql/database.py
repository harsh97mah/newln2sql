import os
import re

from .constants import Color
from .table import Table
from .column import Column


class Database:

    def __init__(self):
        self.tables = []
        self.thesaurus_object = None

    def set_thesaurus(self, thesaurus):
        self.thesaurus_object = thesaurus

    def get_number_of_tables(self):
        return len(self.tables)

    def get_tables(self):
        return self.tables

    def get_table_names(self):
        table_names = []
        for table in self.tables:
            table_names.append(table.name)

    def get_table_with_this_column(self, name):
        tables = []
        for table in self.tables:
            for column in table.get_columns():
                if column.name == name:
                    tables.append(table.name)
        return tables


    def get_column_with_this_name(self, name):
        for table in self.tables:
            for column in table.get_columns():
                if column.name == name:
                    return column

    def get_table_by_name(self, table_name):
        for table in self.tables:
            if table.name == table_name:
                return table

    def get_tables_into_dictionary(self):
        data = {}
        for table in self.tables:
            data[table.name] = []
            for column in table.get_columns():
                data[table.name].append(column.name)
        return data

    def get_primary_keys_by_table(self):
        data = {}
        for table in self.tables:
            data[table.name] = table.get_primary_keys()
        return data

    def get_foreign_keys_by_table(self):
        data = {}
        for table in self.tables:
            data[table.name] = table.get_foreign_keys()
        return data

    def get_primary_keys_of_table(self, ):
        for table in self.tables:
            if table.name == table_name:
                return table.get_primary_keys()

    def get_primary_key_names_of_table(self, table_name):
        for table in self.tables:
            if table.name == table_name:
                return table.get_primary_key_names()

    def get_foreign_keys_of_table(self, table_name):
        for table in self.tables:
            if table.name == table_name:
                return table.get_foreign_keys()

    def get_foreign_key_names_of_table(self, table_name):
        for table in self.tables:
            if table.name == table_name:
                return table.get_foreign_key_names()

    def add_table(self, table):
        self.tables.append(table)

    # @staticmethod
    # def _generate_path(path):
    #     cwd = os.path.dirname(__file__)
    #     filename = os.path.join(cwd, path)
    #     return filename

    def load(self, data):
        table = self.create_table(data)
        self.add_table(table)

    def predict_type(self, string):
        if 'int' in string.lower():
            return 'int'
        elif 'char' in string.lower() or 'text' in string.lower():
            return 'str'
        elif 'date' in string.lower():
            return 'date'
        else:
            return 'unknow'


    def create_table(self, data_list):
        # lines = table_string.split("\n")
        table = Table()
#        for line in lines:
#            if 'TABLE' in line:
        table_name = 'dataset'
        table.name = table_name
        #if self.thesaurus_object is not None:
            #table.equivalences = self.thesaurus_object.get_synonyms_of_a_word(table.name)
            #elif 'PRIMARY KEY' in line:

        # primary_key_columns = re.findall("`(\w+)`", line)
                # for primary_key_column in primary_key_columns:
                    # table.add_primary_key(primary_key_column)
            # else:
        # column_name = re.search("`(\w+)`", line)
        for i in range(len(data_list)):
            column_name = data_list[i]['col_name']
            column_internal_name = data_list[i]['internal_name']
            column_type = data_list[i]['col_type']
            if column_name is not None:
                    # column_type = self.predict_type(line)
                if self.thesaurus_object is not None:
                    equivalences = self.thesaurus_object.get_synonyms_of_a_word(column_name.group(1))
                else:
                    equivalences = []
                table.add_column(column_name, column_internal_name, column_type, equivalences)
        return table

    def print_me(self):
        for table in self.tables:
            print('+-------------------------------------+')
            print("| %25s           |" % (table.name.upper()))
            print('+-------------------------------------+')
            for column in table.columns:
                if column.is_primary():
                    print("| 🔑 %31s           |" % (Color.BOLD + column.name + ' (' + column.get_type() + ')' + Color.END))
                elif column.is_foreign():
                    print("| #️⃣ %31s           |" % (Color.ITALIC + column.name + ' (' + column.get_type() + ')' + Color.END))
                else:
                    print("|   %23s           |" % (column.name + ' (' + column.get_type() + ')'))
            print('+-------------------------------------+\n')
