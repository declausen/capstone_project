import pyspark as ps
import pandas as pd
from pyspark.mllib.recommendation import ALS

import sys

sys.path.insert(0, '/Users/davidclausen/Galvanize/DSI/capstone_project')
from recommender import BookRecommender, remove_header_reduce_columns



if __name__ == '__main__':
    spark = ps.sql.SparkSession.builder \
        .master("local[4]") \
        .appName("building recommender") \
        .getOrCreate()

    sc = spark.sparkContext

    subset_raw_data = sc.textFile('../data/subset.csv')
    subset_data = remove_header_reduce_columns(subset_raw_data)

    book_rec = BookRecommender(ALS)
    book_rec.add_library('../data/books_information.csv')
    # book_rec.add_books_read('../data/subset_claimed_books_by_reader.csv')
    book_rec.train_model_with_parameters(data_rdd=subset_data, rank=14, iterations=20, regularization_parameter=0.1)
