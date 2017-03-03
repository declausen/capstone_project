import pyspark as ps
# from pyspark.ml import Pipeline
from pyspark.mllib.recommendation import ALS
# from pyspark.mllib.evaluation import RegressionMetrics
# import math
# from pyspark.sql import Row
# from pyspark.sql.types import IntegerType, FloatType
# from pyspark.sql.functions import udf, col
import numpy as np
import pandas as pd



class BookRecommender(object):
    ''' Makes a BookRecommender object '''
    def __init__(self, model):
        '''
        Initializes the BookRecommender object with a algorithm model (I used pyspark.mllib.recommendation.als)
        '''
        self.model = model()
        self.trained_model = None

    def train_model_with_parameters(self, data_rdd, rank, iterations, regularization_parameter):
        '''
        Adds parameters to the algorithm model, trains it and saves it as trained_model
        '''
        print "Model Training..."
        self.trained_model = self.model.train(data_rdd, rank, iterations, regularization_parameter)
        print "Model Training Complete"

    def recommend_books(self, reader_id, num_books):
        ''' Recommends num_books number of books for reader_id '''
        print "Finding Recommendations"
        print self.trained_model.recommendProducts(reader_id, num_books)



if __name__ == '__main__':
    spark = ps.sql.SparkSession.builder \
                .master("local[4]") \
                .appName("building recommender") \
                .getOrCreate()

    readers = np.arange(11,23)
    books = np.array(list(np.arange(1,4))*4)
    ratings = np.random.randint(0,2,12)
    data = np.hstack((np.hstack((readers.reshape(-1,1),books.reshape(-1,1))), ratings.reshape(-1,1)))

    pd_df = pd.DataFrame(data, columns=("reader_id", "book_id", "claimed"))
    spark_rdd = spark.createDataFrame(pd_df).rdd


    book_rec = BookRecommender(ALS)
    book_rec.train_model_with_parameters(data_rdd=spark_rdd, rank=3, iterations=5, regularization_parameter=0.1)
    book_rec.recommend_books(reader_id=15, num_books=3)
