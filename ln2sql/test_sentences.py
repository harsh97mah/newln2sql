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

def test_output_gen1():
    database_path = DATABASE_PATH + '/school.sql'
    input_sentence = 'apply filter with age equal to 11'
    expected = "\nparam: {" + "\nCONDITION: {age: {EQ: 11}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"
    assert Ln2sql(database_path, language_path).get_query(input_sentence) == expected

def test_output_gen2():
    database_path = DATABASE_PATH + '/school.sql'
    input_sentence = 'apply filter with age not equal to 11'
    expected = "\nparam: {" + "\nCONDITION: {age: {NE: 11}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"
    assert Ln2sql(database_path, language_path).get_query(input_sentence) == expected

def test_output_gen3():
    database_path = DATABASE_PATH + '/school.sql'
    input_sentence = 'apply filter with age less than 11'
    expected = "\nparam: {" + "\nCONDITION: {age: {LT: 11}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"
    assert Ln2sql(database_path, language_path).get_query(input_sentence) == expected

def test_output_gen4():
    database_path = DATABASE_PATH + '/school.sql'
    input_sentence = 'apply filter with age less than or equal to 11'
    expected = "\nparam: {" + "\nCONDITION: {age: {LTE: 11}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"
    assert Ln2sql(database_path, language_path).get_query(input_sentence) == expected

def test_output_gen5():
    database_path = DATABASE_PATH + '/school.sql'
    input_sentence = 'apply filter with age greater than 11'
    expected = "\nparam: {" + "\nCONDITION: {age: {GT: 11}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"
    assert Ln2sql(database_path, language_path).get_query(input_sentence) == expected

def test_output_gen6():
    database_path = DATABASE_PATH + '/school.sql'
    input_sentence = 'apply filter with age greater than or equal to 11'
    expected = "\nparam: {" + "\nCONDITION: {age: {GTE: 11}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"
    assert Ln2sql(database_path, language_path).get_query(input_sentence) == expected

def test_output_gen7():
    database_path = DATABASE_PATH + '/school.sql'
    input_sentence = 'apply filter with maximum age'
    expected = "\nparam: {" + "\nCONDITION: {age: {IS_MAXVAL: true}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"
    assert Ln2sql(database_path, language_path).get_query(input_sentence) == expected

def test_output_gen8():
    database_path = DATABASE_PATH + '/school.sql'
    input_sentence = 'apply filter with other than maximum age'
    expected = "\nparam: {" + "\nCONDITION: {age: {IS_NOT_MAXVAL: true}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"
    assert Ln2sql(database_path, language_path).get_query(input_sentence) == expected

def test_output_gen9():
    database_path = DATABASE_PATH + '/school.sql'
    input_sentence = 'apply filter with minimum age'
    expected = "\nparam: {" + "\nCONDITION: {age: {IS_MINVAL: true}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"
    assert Ln2sql(database_path, language_path).get_query(input_sentence) == expected

def test_output_gen10():
    database_path = DATABASE_PATH + '/school.sql'
    input_sentence = 'apply filter with other than minimum age'
    expected = "\nparam: {" + "\nCONDITION: {age: {IS_NOT_MINVAL: true}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"
    assert Ln2sql(database_path, language_path).get_query(input_sentence) == expected

def test_output_gen11():
    database_path = DATABASE_PATH + '/school.sql'
    input_sentence = 'apply filter with empty value in age'
    expected = "\nparam: {" + "\nCONDITION: {age: {IS_EMPTY: true}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"
    assert Ln2sql(database_path, language_path).get_query(input_sentence) == expected

def test_output_gen12():
    database_path = DATABASE_PATH + '/school.sql'
    input_sentence = 'apply filter with not empty value in age'
    expected = "\nparam: {" + "\nCONDITION: {age: {NOT_EMPTY: true}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"
    assert Ln2sql(database_path, language_path).get_query(input_sentence) == expected

def test_output_gen13():
    database_path = DATABASE_PATH + '/school.sql'
    input_sentence = 'apply filter for age in range 21 to 23'
    expected = "\nparam: {" + "\nCONDITION: {age: {IN_RANGE: (21, 23)}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"
    assert Ln2sql(database_path, language_path).get_query(input_sentence) == expected

def test_output_gen14():
    database_path = DATABASE_PATH + '/school.sql'
    input_sentence = 'apply filter for age more than 10 and idClass less than 2'
    expected = "\nparam: {" + "\nCONDITION: {AND:[{age: {GT: 10}},{idClass: {LT: 2}}]," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"
    assert Ln2sql(database_path, language_path).get_query(input_sentence) == expected

def test_output_gen15():
    database_path = DATABASE_PATH + '/school.sql'
    input_sentence = 'apply filter where firstname is alex'
    expected = "\nparam: {" + "\nCONDITION: {firstname: {EQ: alex}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"
    assert Ln2sql(database_path, language_path).get_query(input_sentence) == expected

def test_output_gen16():
    database_path = DATABASE_PATH + '/school.sql'
    input_sentence = 'apply filter where firstname is alex'
    expected = "\nparam: {" + "\nCONDITION: {firstname: {EQ: alex}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"
    assert Ln2sql(database_path, language_path).get_query(input_sentence) == expected

def test_output_gen17():
    database_path = DATABASE_PATH + '/school.sql'
    input_sentence = 'apply filter where age is between 21 and 23'
    expected = "\nparam: {" + "\nCONDITION: {age: {IN_RANGE: (21, 23)}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"
    assert Ln2sql(database_path, language_path).get_query(input_sentence) == expected

def test_output_gen18():
    database_path = DATABASE_PATH + '/city.sql'
    input_sentence = 'apply filter for id with value equal to 1'
    expected = "\nparam: {" + "\nCONDITION: {id: {EQ: 1}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"
    assert Ln2sql(database_path, language_path).get_query(input_sentence) == expected

def test_output_gen19():
    database_path = DATABASE_PATH + '/school.sql'
    input_sentence = 'show all the data having firstname as alex'
    expected = "\nparam: {" + "\nCONDITION: {firstname: {EQ: alex}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"
    assert Ln2sql(database_path, language_path).get_query(input_sentence) == expected

def test_output_gen20():
    database_path = DATABASE_PATH + '/city.sql'
    input_sentence = 'show all data with Name as amas'
    expected = "\nparam: {" + "\nCONDITION: {Name: {EQ: amas}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"
    assert Ln2sql(database_path, language_path).get_query(input_sentence) == expected

def test_output_gen21():
    database_path = DATABASE_PATH + '/school.sql'
    input_sentence = 'give all the datapoints with field as engineering'
    expected = "\nparam: {" + "\nCONDITION: {field: {EQ: engineering}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"
    assert Ln2sql(database_path, language_path).get_query(input_sentence) == expected

def test_output_gen22():
    database_path = DATABASE_PATH + '/school.sql'
    input_sentence = 'show explore card for age'
    expected = "\nparam: {" + '\nGROUP BY: [{COLUMN: "age", INTERNAL_NAME: "age"}],' + '\nSELECT: [{FUNCTION: "COUNT", AS: "COUNT", INTERNAL_NAME: "count", COLUMN: "age"},'+ '\n{FUNCTION: "PERCENTAGE", AS: "PERCENTAGE", INTERNAL_NAME: "percentage", COLUMN: "age"}],' + '\nWORKSPACE_ID: 3013\n}'
    assert Ln2sql(database_path, language_path).get_query(input_sentence) == expected

def test_output_gen23():
    database_path = DATABASE_PATH + '/school.sql'
    input_sentence = 'this is wrong sentence'
    expected = 'Invalid Sentence'
    with pytest.raises(SystemExit):
        Ln2sql(database_path, language_path).get_query(input_sentence)

def test_output_gen24():
    database_path = DATABASE_PATH + '/school.sql'
    input_sentence = "age wont matter anyway"
    expected = 'Invalid Sentence'
    with pytest.raises(SystemExit):
        Ln2sql(database_path, language_path).get_query(input_sentence)

def test_output_gen25():
    database_path = DATABASE_PATH + '/city.sql'
    input_sentence = "i cannot find anything in this sentence"
    expected = 'Invalid Sentence'
    with pytest.raises(SystemExit):
        Ln2sql(database_path, language_path).get_query(input_sentence)

def test_output_gen26():
    database_path = DATABASE_PATH + '/school.sql'
    input_sentence = "print explore card for the column firstname"
    expected = "\nparam: {" + '\nGROUP BY: [{COLUMN: "firstname", INTERNAL_NAME: "firstname"}],' + '\nSELECT: [{FUNCTION: "COUNT", AS: "COUNT", INTERNAL_NAME: "count", COLUMN: "firstname"},'+ '\n{FUNCTION: "PERCENTAGE", AS: "PERCENTAGE", INTERNAL_NAME: "percentage", COLUMN: "firstname"}],' + '\nWORKSPACE_ID: 3013\n}'
    assert Ln2sql(database_path, language_path).get_query(input_sentence) == expected

def test_output_gen27():
    database_path = DATABASE_PATH + '/school.sql'
    input_sentence = "filter for age not more than 11"
    expected = "\nparam: {" + "\nCONDITION: {age: {LT: 11}}," + '\nSELECT: "ALL",' + "\nWORKSPACE_ID: 3013\n}"
    assert Ln2sql(database_path, language_path).get_query(input_sentence) == expected

def test_output_gen28():
    database_path = DATABASE_PATH + '/school.sql'
    input_sentence = "apply filter for age equals alex"
    expected = 'Invalid Sentence'
    with pytest.raises(Exception):
        Ln2sql(database_path, language_path).get_query(input_sentence)

def test_output_gen29():
    database_path = DATABASE_PATH + '/school.sql'
    input_sentence = "apply filter for age greater than 21 and firstname is 2"
    expected = 'Invalid Sentence'
    with pytest.raises(Exception):
        Ln2sql(database_path, language_path).get_query(input_sentence)
