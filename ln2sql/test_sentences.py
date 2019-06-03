import os
import pytest

from ln2sql.parser import Parser
from ln2sql.stopwordFilter import StopwordFilter
from ln2sql.database import Database
from ln2sql import Ln2sql

BASE_PATH = os.path.dirname(os.path.dirname(__file__))  # Project directory.
STOPWORDS_PATH = os.path.join(BASE_PATH, 'ln2sql/stopwords/')
DATABASE_PATH = os.path.join(BASE_PATH, 'ln2sql/database_store')
LANGUAGE_PATH  =os.path.join(BASE_PATH, 'ln2sql/lang_store')
language_path = LANGUAGE_PATH + '/english.csv'
database_1 = DATABASE_PATH + '/school.sql'
database_2 = DATABASE_PATH + '/city.sql'

list_apply_filter = [('apply filter with age equal to 11',"\nparam: {" + "\nCONDITION: {age: {EQ: 11}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
('apply filter with age not equal to 11',"\nparam: {" + "\nCONDITION: {age: {NE: 11}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
('apply filter with age less than 11',"\nparam: {" + "\nCONDITION: {age: {LT: 11}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
('apply filter with age less than or equal to 11',"\nparam: {" + "\nCONDITION: {age: {LTE: 11}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
('apply filter with age greater than 11',"\nparam: {" + "\nCONDITION: {age: {GT: 11}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
('apply filter with age greater than or equal to 11',"\nparam: {" + "\nCONDITION: {age: {GTE: 11}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
('apply filter with maximum age',"\nparam: {" + "\nCONDITION: {age: {IS_MAXVAL: true}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
('apply filter with other than maximum age',"\nparam: {" + "\nCONDITION: {age: {IS_NOT_MAXVAL: true}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
('apply filter with minimum age',"\nparam: {" + "\nCONDITION: {age: {IS_MINVAL: true}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
('apply filter with other than minimum age',"\nparam: {" + "\nCONDITION: {age: {IS_NOT_MINVAL: true}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
('apply filter with empty value in age',"\nparam: {" + "\nCONDITION: {age: {IS_EMPTY: true}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
('apply filter with not empty value in age',"\nparam: {" + "\nCONDITION: {age: {NOT_EMPTY: true}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
('apply filter for age in range 21 to 23',"\nparam: {" + "\nCONDITION: {age: {IN_RANGE: (21, 23)}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
('apply filter for age more than 10 and idClass less than 2',"\nparam: {" + "\nCONDITION: {AND:[{age: {GT: 10}},{idClass: {LT: 2}}]," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
('apply filter where firstname is alex',"\nparam: {" + "\nCONDITION: {firstname: {EQ: alex}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
('apply filter where age is between 21 and 23',"\nparam: {" + "\nCONDITION: {age: {IN_RANGE: (21, 23)}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
('show all the data having firstname as alex',"\nparam: {" + "\nCONDITION: {firstname: {EQ: alex}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
('give all the datapoints with field as engineering',"\nparam: {" + "\nCONDITION: {field: {EQ: engineering}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
("filter for age not more than 11","\nparam: {" + "\nCONDITION: {age: {LT: 11}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
("apply filter to data where firstname contains ama","\nparam: {" + "\nCONDITION: {firstname: {CONTAINS: ama}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
("filter where firstname starts with ara","\nparam: {" + "\nCONDITION: {firstname: {STARTS_WITH: ara}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
("filter where firstname ends with ana","\nparam: {" + "\nCONDITION: {firstname: {ENDS_WITH: ana}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
("filter where firstname does not contain era","\nparam: {" + "\nCONDITION: {firstname: {NOT_CONTAINS: era}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
("apply filter where firstname is not equal to aman","\nparam: {" + "\nCONDITION: {firstname: {NE: aman}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
("apply filter where firstname does not start with to a","\nparam: {" + "\nCONDITION: {firstname: {NOT_STARTS_WITH: a}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
("apply filter where firstname does not end with to a","\nparam: {" + "\nCONDITION: {firstname: {NOT_ENDS_WITH: a}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
("apply filter where firstname has empty values","\nparam: {" + "\nCONDITION: {firstname: {IS_EMPTY: true}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
("apply filter where firstname has non empty values","\nparam: {" + "\nCONDITION: {firstname: {NOT_EMPTY: true}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"),
("apply filter where age is maximum","\nparam: {" + "\nCONDITION: {age: {IS_MAXVAL: true}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}")
]

list_explore_cards = [('show explore for age',"\nparam: {" + '\nGROUP BY: [{COLUMN: "age", INTERNAL_NAME: "age"}],' + '\nSELECT: [{FUNCTION: "COUNT", AS: "COUNT", INTERNAL_NAME: "count", COLUMN: "age"},'+ '\n{FUNCTION: "PERCENTAGE", AS: "PERCENTAGE", INTERNAL_NAME: "percentage", COLUMN: "age"}],' + '\nWORKSPACE_ID: 3013\n}'),
("print explore card for the column firstname","\nparam: {" + '\nGROUP BY: [{COLUMN: "firstname", INTERNAL_NAME: "firstname"}],' + '\nSELECT: [{FUNCTION: "COUNT", AS: "COUNT", INTERNAL_NAME: "count", COLUMN: "firstname"},'+ '\n{FUNCTION: "PERCENTAGE", AS: "PERCENTAGE", INTERNAL_NAME: "percentage", COLUMN: "firstname"}],' + '\nWORKSPACE_ID: 3013\n}')]

list_invalid_sentence = ['this is wrong sentence',"age wont matter anyway","i cannot find anything in this sentence"]
list_wrong_datatypes = ["apply filter for age equals alex","apply filter for age greater than 21 and firstname is 2","apply filter where age is not equal to a",
"apply filter where age contains 21"]

@pytest.mark.parametrize("input_sentence,expected", list_apply_filter)
def test_apply_filter(input_sentence,expected):
    assert Ln2sql(database_1,language_path).get_query(input_sentence)==expected

@pytest.mark.parametrize("input_sentence,expected", list_explore_cards)
def test_explore_cards(input_sentence,expected):
    assert Ln2sql(database_1,language_path).get_query(input_sentence)==expected

@pytest.mark.parametrize("input_sentence", list_invalid_sentence)
def test_invalid_sentence(input_sentence):
    with pytest.raises(SystemExit):
        Ln2sql(database_1, language_path).get_query(input_sentence)

@pytest.mark.parametrize("input_sentence", list_wrong_datatypes)
def test_wrong_datatypes(input_sentence):
    with pytest.raises(Exception):
        Ln2sql(database_1, language_path).get_query(input_sentence)
