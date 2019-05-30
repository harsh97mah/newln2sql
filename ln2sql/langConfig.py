#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 15 10:46:24 2019

@author: harsh97mah
"""

import os
import unicodedata
import string

def onlyUpper(word):
    for c in word:
        if not c.isupper():
            return False
    return True

class LangConfig:
    def __init__(self):
        self.avg_keywords = []
        self.sum_keywords = []
        self.max_keywords = []
        self.min_keywords = []
        self.count_keywords = []
        self.junction_keywords = []
        self.disjunction_keywords = []
        self.greater_keywords = []
        self.less_keywords = []
        self.between_keywords = []
        self.order_by_keywords = []
        self.asc_keywords = []
        self.desc_keywords = []
        self.group_by_keywords = []
        self.negation_keywords = []
        self.equal_keywords = []
        self.like_keywords = []
        self.distinct_keywords = []
        self.non_max_keywords = []
        self.non_min_keywords = []
        self.empty_keywords = []
        self.gte_keywords = []
        self.lte_keywords = []
        self.explore_keywords = []
        self.all_keywords = []

    def get_avg_keywords(self):
        return self.avg_keywords

    def get_sum_keywords(self):
        return self.sum_keywords

    def get_max_keywords(self):
        return self.max_keywords

    def get_min_keywords(self):
        return self.min_keywords

    def get_count_keywords(self):
        return self.count_keywords

    def get_junction_keywords(self):
        return self.junction_keywords

    def get_disjunction_keywords(self):
        return self.disjunction_keywords

    def get_greater_keywords(self):
        return self.greater_keywords

    def get_less_keywords(self):
        return self.less_keywords

    def get_between_keywords(self):
        return self.between_keywords

    def get_order_by_keywords(self):
        return self.order_by_keywords

    def get_asc_keywords(self):
        return self.asc_keywords

    def get_desc_keywords(self):
        return self.desc_keywords

    def get_group_by_keywords(self):
        return self.group_by_keywords

    def get_negation_keywords(self):
        return self.negation_keywords

    def get_equal_keywords(self):
        return self.equal_keywords

    def get_like_keywords(self):
        return self.like_keywords

    def get_distinct_keywords(self):
        return self.distinct_keywords

    def get_non_max_keywords(self):
        return self.non_max_keywords

    def get_non_min_keywords(self):
        return self.non_min_keywords

    def get_empty_keywords(self):
        return self.empty_keywords

    def get_gte_keywords(self):
        return self.gte_keywords

    def get_lte_keywords(self):
        return self.lte_keywords

    def get_non_empty_keywords(self):
        return self.non_empty_keywords

    def get_explore_keywords(self):
        return self.explore_keywords

    def get_all_keywords(self):
        return self.all_keywords

    def remove_accents(self, string):
        nkfd_form = unicodedata.normalize('NFKD', str(string))
        return "".join([c for c in nkfd_form if not unicodedata.combining(c)])

    @staticmethod
    def _generate_path(path):
        cwd = os.path.dirname(__file__)
        filename = os.path.join(cwd, path)
        return filename

    def onlyUpper(word):
        for c in word:
            if not c.isupper():
                return False
        return True

    def load(self, path):
        all = []
        allin = []
        with open(self._generate_path(path)) as f:
            content = f.readlines()
            self.all_keywords = list(map(self.remove_accents, list(map(str.strip, content[:]))))
            for i in range(len(self.all_keywords)):
                self.all_keywords[i] = list(self.all_keywords[i].replace(':',',').split(','))
            [all.extend(e) for e in self.all_keywords]
            while '' in all:
                all.remove('')
            for word in all:
                word = word.replace(' ','')
                allin.append(word)
            self.all_keywords = allin
            self.avg_keywords = list(
                map(self.remove_accents, list(map(str.strip, content[0].replace(':', ',').split(",")))))
            self.avg_keywords = self.avg_keywords[1:len(self.avg_keywords)]
            self.avg_keywords = [keyword.lower() for keyword in self.avg_keywords]
            while("" in self.avg_keywords):
                self.avg_keywords.remove("")
            self.sum_keywords = list(
                map(self.remove_accents, list(map(str.strip, content[1].replace(':', ',').split(",")))))
            self.sum_keywords = self.sum_keywords[1:len(self.sum_keywords)]
            self.sum_keywords = [keyword.lower() for keyword in self.sum_keywords]
            while("" in self.sum_keywords):
                self.sum_keywords.remove("")

            self.max_keywords = list(
                map(self.remove_accents, list(map(str.strip, content[2].replace(':', ',').split(",")))))
            self.max_keywords = self.max_keywords[1:len(self.max_keywords)]
            self.max_keywords = [keyword.lower() for keyword in self.max_keywords]
            while("" in self.max_keywords):
                self.max_keywords.remove("")

            self.min_keywords = list(
                map(self.remove_accents, list(map(str.strip, content[3].replace(':', ',').split(",")))))
            self.min_keywords = self.min_keywords[1:len(self.min_keywords)]
            self.min_keywords = [keyword.lower() for keyword in self.min_keywords]
            while("" in self.min_keywords):
                self.min_keywords.remove("")

            self.count_keywords = list(
                map(self.remove_accents, list(map(str.strip, content[4].replace(':', ',').split(",")))))
            self.count_keywords = self.count_keywords[1:len(self.count_keywords)]
            self.count_keywords = [keyword.lower() for keyword in self.count_keywords]
            while("" in self.count_keywords):
                self.count_keywords.remove("")

            self.junction_keywords = list(
                map(self.remove_accents, list(map(str.strip, content[5].replace(':', ',').split(",")))))
            self.junction_keywords = self.junction_keywords[1:len(self.junction_keywords)]
            self.junction_keywords = [keyword.lower() for keyword in self.junction_keywords]
            while("" in self.junction_keywords):
                self.junction_keywords.remove("")

            self.disjunction_keywords = list(
                map(self.remove_accents, list(map(str.strip, content[6].replace(':', ',').split(",")))))
            self.disjunction_keywords = self.disjunction_keywords[1:len(self.disjunction_keywords)]
            self.disjunction_keywords = [keyword.lower() for keyword in self.disjunction_keywords]
            while("" in self.disjunction_keywords):
                self.disjunction_keywords.remove("")

            self.greater_keywords = list(
                map(self.remove_accents, list(map(str.strip, content[7].replace(':', ',').split(",")))))
            self.greater_keywords = self.greater_keywords[1:len(self.greater_keywords)]
            self.greater_keywords = [keyword.lower() for keyword in self.greater_keywords]
            while("" in self.greater_keywords):
                self.greater_keywords.remove("")

            self.less_keywords = list(
                map(self.remove_accents, list(map(str.strip, content[8].replace(':', ',').split(",")))))
            self.less_keywords = self.less_keywords[1:len(self.less_keywords)]
            self.less_keywords = [keyword.lower() for keyword in self.less_keywords]
            while("" in self.less_keywords):
                self.less_keywords.remove("")

            self.between_keywords = list(
                map(self.remove_accents, list(map(str.strip, content[9].replace(':', ',').split(",")))))
            self.between_keywords = self.between_keywords[1:len(self.between_keywords)]
            self.between_keywords = [keyword.lower() for keyword in self.between_keywords]
            while("" in self.between_keywords):
                self.between_keywords.remove("")

            self.order_by_keywords = list(
                map(self.remove_accents, list(map(str.strip, content[10].replace(':', ',').split(",")))))
            self.order_by_keywords = self.order_by_keywords[1:len(self.order_by_keywords)]
            self.order_by_keywords = [keyword.lower() for keyword in self.order_by_keywords]
            while("" in self.avg_keywords):
                self.order_by_keywords.remove("")

            self.asc_keywords = list(
                map(self.remove_accents, list(map(str.strip, content[11].replace(':', ',').split(",")))))
            self.asc_keywords = self.asc_keywords[1:len(self.asc_keywords)]
            self.asc_keywords = [keyword.lower() for keyword in self.asc_keywords]
            while("" in self.asc_keywords):
                self.asc_keywords.remove("")

            self.desc_keywords = list(
                map(self.remove_accents, list(map(str.strip, content[12].replace(':', ',').split(",")))))
            self.desc_keywords = self.desc_keywords[1:len(self.desc_keywords)]
            self.desc_keywords = [keyword.lower() for keyword in self.desc_keywords]
            while("" in self.desc_keywords):
                self.desc_keywords.remove("")

            self.group_by_keywords = list(
                map(self.remove_accents, list(map(str.strip, content[13].replace(':', ',').split(",")))))
            self.group_by_keywords = self.group_by_keywords[1:len(self.group_by_keywords)]
            self.group_by_keywords = [keyword.lower() for keyword in self.group_by_keywords]
            while("" in self.group_by_keywords):
                self.group_by_keywords.remove("")

            self.negation_keywords = list(
                map(self.remove_accents, list(map(str.strip, content[14].replace(':', ',').split(",")))))
            self.negation_keywords = self.negation_keywords[1:len(self.negation_keywords)]
            self.negation_keywords = [keyword.lower() for keyword in self.negation_keywords]
            while("" in self.negation_keywords):
                self.negation_keywords.remove("")

            self.equal_keywords = list(
                map(self.remove_accents, list(map(str.strip, content[15].replace(':', ',').split(",")))))
            self.equal_keywords = self.equal_keywords[1:len(self.equal_keywords)]
            self.equal_keywords = [keyword.lower() for keyword in self.equal_keywords]
            while("" in self.equal_keywords):
                self.equal_keywords.remove("")

            self.like_keywords = list(
                map(self.remove_accents, list(map(str.strip, content[16].replace(':', ',').split(",")))))
            self.like_keywords = self.like_keywords[1:len(self.like_keywords)]
            self.like_keywords = [keyword.lower() for keyword in self.like_keywords]
            while("" in self.like_keywords):
                self.like_keywords.remove("")

            self.distinct_keywords = list(
                map(self.remove_accents, list(map(str.strip, content[17].replace(':', ',').split(",")))))
            self.distinct_keywords = self.distinct_keywords[1:len(self.distinct_keywords)]
            self.distinct_keywords = [keyword.lower() for keyword in self.distinct_keywords]
            while("" in self.distinct_keywords):
                self.distinct_keywords.remove("")

            self.non_max_keywords = list(
                map(self.remove_accents, list(map(str.strip, content[18].replace(':', ',').split(",")))))
            self.non_max_keywords = self.non_max_keywords[1:len(self.non_max_keywords)]
            self.non_max_keywords = [keyword.lower() for keyword in self.non_max_keywords]
            while("" in self.non_max_keywords):
                self.non_max_keywords.remove("")

            self.non_min_keywords = list(
                map(self.remove_accents, list(map(str.strip, content[19].replace(':', ',').split(",")))))
            self.non_min_keywords = self.non_min_keywords[1:len(self.non_min_keywords)]
            self.non_min_keywords = [keyword.lower() for keyword in self.non_min_keywords]
            while("" in self.non_min_keywords):
                self.non_min_keywords.remove("")

            self.empty_keywords = list(
                map(self.remove_accents, list(map(str.strip, content[20].replace(':', ',').split(",")))))
            self.empty_keywords = self.empty_keywords[1:len(self.empty_keywords)]
            self.empty_keywords = [keyword.lower() for keyword in self.empty_keywords]
            while("" in self.empty_keywords):
                self.empty_keywords.remove("")

            self.gte_keywords = list(
                map(self.remove_accents, list(map(str.strip, content[21].replace(':', ',').split(",")))))
            self.gte_keywords = self.gte_keywords[1:len(self.gte_keywords)]
            self.gte_keywords = [keyword.lower() for keyword in self.gte_keywords]
            while("" in self.gte_keywords):
                self.gte_keywords.remove("")

            self.lte_keywords = list(
                map(self.remove_accents, list(map(str.strip, content[22].replace(':', ',').split(",")))))
            self.lte_keywords = self.lte_keywords[1:len(self.lte_keywords)]
            self.lte_keywords = [keyword.lower() for keyword in self.lte_keywords]
            while("" in self.lte_keywords):
                self.lte_keywords.remove("")

            self.non_empty_keywords = list(
                map(self.remove_accents, list(map(str.strip, content[23].replace(':', ',').split(",")))))
            self.non_empty_keywords = self.non_empty_keywords[1:len(self.non_empty_keywords)]
            self.non_empty_keywords = [keyword.lower() for keyword in self.non_empty_keywords]
            while("" in self.non_empty_keywords):
                self.non_empty_keywords.remove("")

            self.explore_keywords = list(
                map(self.remove_accents, list(map(str.strip, content[24].replace(':', ',').split(",")))))
            self.explore_keywords = self.explore_keywords[1:len(self.explore_keywords)]
            self.explore_keywords = [keyword.lower() for keyword in self.explore_keywords]
            while("" in self.explore_keywords):
                self.explore_keywords.remove("")

    def print_me(self):
        print(self.avg_keywords)
        print(self.sum_keywords)
        print(self.max_keywords)
        print(self.min_keywords)
        print(self.count_keywords)
        print(self.junction_keywords)
        print(self.disjunction_keywords)
        print(self.greater_keywords)
        print(self.less_keywords)
        print(self.between_keywords)
        print(self.order_by_keywords)
        print(self.asc_keywords)
        print(self.desc_keywords)
        print(self.group_by_keywords)
        print(self.negation_keywords)
        print(self.equal_keywords)
        print(self.like_keywords)
        print(self.distinct_keywords)
        print(self.non_max_keywords)
        print(self.non_min_keywords)
        print(self.empty_keywords)
        print(self.gte_keywords)
        print(self.lte_keywords)
        print(self.non_empty_keywords)
        print(self.explore_keywords)
