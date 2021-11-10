from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, IntegerType, StringType


def get_array_type_ohe_udf(mapping, col):
    def one_hot_encode_array_type(col):
        one_hot_list = [0]*len(mapping)
        for item in col:
            one_hot_list[mapping[item]] = 1
        return one_hot_list
    return udf(one_hot_encode_array_type, ArrayType(IntegerType()))(col)

def get_array_type_inv_ohe_udf(idx_mapping, val_mapping, col):
    def invert_one_hot_encoded_array_type(col):
        return [[f"{idx_mapping.get(idx, 'None')}", f"{val_mapping.get(val, 'None')}"] for idx, val in enumerate(col)]
    return udf(invert_one_hot_encoded_array_type, ArrayType(ArrayType(StringType())))(col)
        
            
