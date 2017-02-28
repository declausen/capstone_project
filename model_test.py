import pyspark as ps
from pyspark.ml import Pipeline
from pyspark.mllib.recommendation import ALS
from pyspark.mllib.evaluation import RegressionMetrics
import math


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

def remove_header(data):
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
    train_RDD, validate_RDD, test_RDD = subset_data.randomSplit([6, 2, 2], seed=0L)
    validate_for_predict_RDD = validate_RDD.map(lambda x: (x[0], x[1]))
    test_for_predict_RDD = test_RDD.map(lambda x: (x[0], x[1]))
    return train_RDD, validate_RDD, validate_for_predict_RDD, test_RDD, test_for_predict_RDD

def test_model(train_RDD, validate_RDD, validate_for_predict_RDD):
    seed = 5L
    iterations = 20
    regularization_parameter = 0.05
    ranks = [10, 15, 20, 25, 30]
    errors = [0, 0, 0, 0, 0]
    reg_met = [0, 0, 0, 0, 0]
    err = 0
    tolerance = 0.02

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
    subset_data = remove_header(subset_raw_data)

    train_RDD, validate_RDD, validate_for_predict_RDD, test_RDD, test_for_predict_RDD = train_validate_test_split(subset_data)

    test_model(train_RDD, validate_RDD, validate_for_predict_RDD)

    # check_type_and_take_3(train_RDD)
