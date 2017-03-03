import pyspark as ps
from pyspark.ml import Pipeline
from pyspark.mllib.recommendation import ALS
from pyspark.mllib.evaluation import RegressionMetrics
# from pyspark.sql.functions import *
import math
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.functions import udf, col

from scipy import sparse
import pandas as pd

from Baselines import UniformRandomBaseline, GlobalMeanBaseline, MeanOfMeansBaseline
from collections import OrderedDict
import numpy as np



def print_unique(ratings_contents):
    print "Number of readers: {}".format(ratings_contents.reader_id.nunique())
    print "Number of books: {}".format(ratings_contents.book_id.nunique())

def create_matrix(df):
    '''
    Creates a matrix ('reader_id' as rows, 'book_id' as columns, 'claimed'(rating) as element) from a long format to wide format - pivots
    '''
    highest_reader_id = df.reader_id.max()
    highest_book_id = df.book_id.max()
    print_unique(df)
    ratings_as_mat = sparse.lil_matrix((highest_reader_id, highest_book_id))
    for _, row in df.iterrows():
        # subtract 1 from id's due to match 0 indexing
        ratings_as_mat[row.reader_id - 1, row.book_id - 1] = row.claimed
    return ratings_as_mat



def load_data():
    '''
    Load data and return RDD
    '''
    return sc.textFile('data/subset.csv')

def check_type_and_take_3(data):
    '''
    Print type and top 3 of data
    '''
    print type(data)
    # print data.take(3)

def remove_header_reduce_columns(data):
    """
    Remove header from RDD, also reduce columns to only 'reader_id' (row[3]), 'book_id' (row[5]) and 'claimed' (row[9])
    """
    data_header = data.take(1)[0]
    data_no_header = data.filter(lambda line: line!=data_header).map(lambda line: line.split(",")).map(lambda row: (row[3],row[5],row[9])).cache()
    return data_no_header

def train_validate_test_split(data):
    '''
    Split data into three RDD's, also remove 'claimed' column from validate and test RDD's to make 'for_predict_RDD'
    '''
    train_RDD, validate_RDD, test_RDD = data.randomSplit([6, 2, 2], seed=0L)
    validate_for_predict_RDD = validate_RDD.map(lambda x: (x[0], x[1]))
    test_for_predict_RDD = test_RDD.map(lambda x: (x[0], x[1]))
    return train_RDD, validate_RDD, validate_for_predict_RDD, test_RDD, test_for_predict_RDD

def clicked_not_claimed_to_neg1(claimed, clicked):
    if (clicked == 1) and (claimed == 0):
        return -1.0
    # elif (clicked == 1) and (claimed == 1):
    #     return 0.9
    else:
        return claimed

def adding_neg1(data_rdd):
    data_header = data_rdd.take(1)[0]
    data_with_schema = data_rdd.filter(lambda line: line!=data_header).map(lambda line: line.split(",")).map(lambda p: Row(reader_id=p[3], book_id=p[5], clicked=int(p[8]), claimed=float(p[9])))
    data_df = spark.createDataFrame(data_with_schema)
    my_udf = udf(lambda claimed, clicked: clicked_not_claimed_to_neg1(claimed, clicked), FloatType())
    df_out = data_df.withColumn('claimed', my_udf(col('claimed'), col('clicked')))
    rdd_with_neg1 = df_out.rdd
    return_rdd = rdd_with_neg1.map(lambda tokens: (tokens[3],tokens[0],tokens[1])).cache()

    return return_rdd


def test_model(train_RDD, validate_RDD, validate_for_predict_RDD):
    seed = 5L
    iterations = 20
    regularization_parameter = 0.1
    ranks = [10, 20, 14]
    errors = [0, 0, 0]
    reg_met = [0, 0, 0]
    err = 0

    min_error = float('inf')
    best_rank = -1
    best_iteration = -1
    for rank in ranks:
        model = ALS.train(train_RDD, rank=rank, seed=seed, iterations=iterations, lambda_=regularization_parameter)
        # model = ALS.trainImplicit(train_RDD, rank=rank, seed=seed, iterations=iterations, lambda_=regularization_parameter)
        predictions = model.predictAll(validate_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))

        rates_and_preds = validate_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
        valuesAndPreds = rates_and_preds.map(lambda p: (p[1]))
        metrics = RegressionMetrics(valuesAndPreds)
        error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
        errors[err] = error
        reg_met[err] = metrics.rootMeanSquaredError
        err += 1
        print 'For rank %s the RMSE is %s the reg_met is %s' % (rank, error,\
                                                                metrics.rootMeanSquaredError)
        if error < min_error:
            min_error = error
            best_rank = rank

    print 'The best model was trained with rank %s' % best_rank

if __name__ == '__main__':
    spark = ps.sql.SparkSession.builder \
                .master("local[4]") \
                .appName("building recommender") \
                .getOrCreate()

    sc = spark.sparkContext

    subset_raw_data = load_data()

    subset_data = remove_header_reduce_columns(subset_raw_data)
    # subset_data_neg1 = adding_neg1(subset_raw_data)

    train_RDD, validate_RDD, validate_for_predict_RDD, test_RDD, test_for_predict_RDD = train_validate_test_split(subset_data)
    # train_RDD_neg1, validate_RDD_neg1, validate_for_predict_RDD_neg1, test_RDD_neg1, test_for_predict_RDD_neg1 = train_validate_test_split(subset_data_neg1)

    test_model(train_RDD, validate_RDD, validate_for_predict_RDD)
    # test_model(train_RDD_neg1, validate_RDD_neg1, validate_for_predict_RDD_neg1)

    # check_type_and_take_3(train_RDD)

    # baseline_methods = OrderedDict()
    # baseline_methods['ur'] = UniformRandomBaseline
    # baseline_methods['gm'] = GlobalMeanBaseline
    # baseline_methods['mom'] = MeanOfMeansBaseline
    #
    # R = np.random.randint(-10,10,1000).astype('float')
    # test = R
    #
    # train = R.copy()
    # train[np.random.randint(0,1000,50)] = np.nan
    #
    # train = train.reshape(100,10)
    # test = test.reshape(100,10)
    #
    # pd_df = pd.read_table("data/subset.csv", sep=',')
    # # pd_df_matrix = create_matrix(pd_df)
    # # np.asarray(pd_df_matrix.todense())
    #
    # baselines = {}
    # for name in baseline_methods:
    #     Method = baseline_methods[name]
    #     method = Method(train)
    #     baselines[name] = method.rmse(test)
    #     print('%s RMSE:\t%.5f' % (method, baselines[name]))
