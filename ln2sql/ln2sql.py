#!/usr/bin/python3
import argparse
import os

from .database import Database
from .langConfig import LangConfig
from .parser import Parser
from .stopwordFilter import StopwordFilter
from .thesaurus import Thesaurus
import logging
log = logging.getLogger(__name__)

class Ln2sql:
    def __init__(
            self,
            data,
            thesaurus_path=None,
            stopwords_path=None,
    ):
        language_path='lang_store/english.csv'

        database = Database()
        self.stopwordsFilter = None

        if thesaurus_path:
            thesaurus = Thesaurus()
            thesaurus.load(thesaurus_path)
            database.set_thesaurus(thesaurus)

        if stopwords_path:
            self.stopwordsFilter = StopwordFilter()
            self.stopwordsFilter.load(stopwords_path)

        database.load(data)

        config = LangConfig()
        config.load(language_path)

        self.parser = Parser(database, config)

    def validate(self, input_sentence):
        validation = self.parser.validate_input_sentence(input_sentence)
        return validation

    def query_type(self, input_sentence):
        task_type = self.parser.determine_task_type(input_sentence)
        return task_type

    def get_query(self, input_sentence, workspace_id):
        queries = self.parser.parse_sentence(input_sentence, self.stopwordsFilter)
        full_query = ''
        for query in queries:
            log.debug("%s", query)
            if query is not None:
                log.debug("%s", str(query))
                full_query += "{'param': {"
                full_query += str(query)
                full_query += " 'WORKSPACE_ID': " + '"' + str(workspace_id) + '"}' +'}'
        log.debug("%s", full_query)
        return full_query
