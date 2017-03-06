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
# from model_test import remove_header_reduce_columns
# import cPickle as pickle
# import graphlab as gl


def remove_header_reduce_columns(data):
    """
    Remove header from RDD, also reduce columns to only 'reader_id' (row[3]), 'book_id' (row[5]) and 'claimed' (row[9])
    """
    data_header = data.take(1)[0]
    data_no_header = data.filter(lambda line: line!=data_header).map(lambda line: line.split(",")).map(lambda row: (row[3],row[5],row[9])).cache()
    return data_no_header


class BookRecommender(object):
    ''' Makes a BookRecommender object '''
    def __init__(self, model):
        '''
        Initializes the BookRecommender object with a algorithm model (I used pyspark.mllib.recommendation.als)
        '''
        self.model = model() # model to use for recommending books
        self.trained_model = None # trianed model, for some reason I needed to do this in order to call recommendProducts in recommend_books()
        self.library = None # pandas dataframe of all books
        # self.books_read_df = None # pandas df with reader_id and list of books claimed
        self.current_reader_id = None
        self.current_recommended_books = None

    def _get_book_ids_from_recs(self, recommendations):
        '''
        Takes a list of Rating objects, pulls out the product numbers (book_id's) and stores them in a list
        '''
        book_ids_list = []
        for rec in recommendations:
            book_ids_list.append(rec.product)
        # book_ids_list = self._remove_previously_downloaded_books(book_ids_list)
        # try using OrderedDict and pickling it instead of csv's
        self._get_book_info_from_book_ids(book_ids_list)

    def _get_book_info_from_book_ids(self, book_ids_list):
        '''
        Takes a list of book_id's and gets the book_id, title, genre_id, genre_id_2 and genre_id_3 for the books and stores them in book_titles_list
        '''
        book_titles_list = []
        for book_id in book_ids_list:
            book_titles_list.append(self.library.loc[self.library['id'] == book_id].values[0])
        self.current_recommended_books = book_titles_list

    # def _remove_previously_downloaded_books(self, book_ids_list):
    #     books_not_claimed = []
    #     books_claimed_by_current_reader_id = self.books_read_df[self.books_read_df.reader_id == self.current_reader_id].values[0]
    #     for book_id in book_ids_list:
    #         if book_id not in books_claimed_by_current_reader_id:
    #             books_not_claimed.append(book_id)
    #     return books_not_claimed

    def _print_top_3_recommendations(self):
        for idx in range(3):
            print self.current_recommended_books[idx][1]

    def _recommendations_to_web_app(self):
        top_3_book_titles = []
        for idx in range(3):
            top_3_book_titles.append(self.current_recommended_books[idx][1])
        return top_3_book_titles

    def train_model_with_parameters(self, data_rdd, rank, iterations, regularization_parameter):
        '''
        Adds parameters to the algorithm model, trains it and saves it as trained_model
        '''
        print "Model Training..."
        self.trained_model = self.model.train(data_rdd, rank=rank, iterations=iterations, lambda_=regularization_parameter, seed=17, nonnegative=True)
        print "Model Training Complete"

    def recommend_books(self, reader_id, num_recs=10):
        ''' Recommends num_books number of books for reader_id '''
        # print "Finding Recommendations for: {}\n".format(reader_id)
        self.current_reader_id = reader_id
        recommendations = self.trained_model.recommendProducts(reader_id, num_recs)
        self._get_book_ids_from_recs(recommendations)
        # self._print_top_3_recommendations()
        return self._recommendations_to_web_app()

    def add_library(self, file_path):
        '''
        Reads in file_path as pandas df and stores it in self.library
        Columns: id, title, genre_id, genre_id_2 and genre_id_3
        Rows: individual books
        '''
        library = pd.read_csv(file_path, index_col=u'Unnamed: 0')
        self.library = library

    # def add_books_read(self, file_path):
    #     books_read_df = pd.read_csv(file_path, names=['reader_id', 'books_read'], dtype={'reader_id': int, 'books_read': list})
    #     self.books_read_df = books_read_df



if __name__ == '__main__':
    spark = ps.sql.SparkSession.builder \
                .master("local[4]") \
                .appName("building recommender") \
                .getOrCreate()

    sc = spark.sparkContext

    ''' Toy Dataset'''
    # readers = np.arange(11,23)
    # books = np.array(list(np.arange(1,4))*4)
    # ratings = np.random.randint(0,2,12)
    # data = np.hstack((np.hstack((readers.reshape(-1,1),books.reshape(-1,1))), ratings.reshape(-1,1)))
    #
    # pd_df = pd.DataFrame(data, columns=("reader_id", "book_id", "claimed"))
    # spark_rdd = spark.createDataFrame(pd_df).rdd

    '''Subset'''
    subset_raw_data = sc.textFile('data/subset.csv')
    subset_data = remove_header_reduce_columns(subset_raw_data)

    book_rec = BookRecommender(ALS)
    book_rec.add_library('data/books_information.csv')
    # book_rec.add_books_read('data/subset_claimed_books_by_reader.csv')
    book_rec.train_model_with_parameters(data_rdd=subset_data, rank=14, iterations=20, regularization_parameter=0.1)
    # book_rec.recommend_books(reader_id=523754)
