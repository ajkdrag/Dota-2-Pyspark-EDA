from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, IntegerType


def get_array_type_ohe_udf(mapping, col):
    def one_hot_encode_array_type(col):
        one_hot_list = [0]*len(mapping)
        for item in col:
            one_hot_list[mapping[item]] = 1
        return one_hot_list
    return udf(one_hot_encode_array_type, ArrayType(IntegerType()))(col)

