from flask import Flask, request, render_template
import pyspark as ps
import pandas as pd
import random
from pyspark.mllib.recommendation import ALS
import sys

sys.path.insert(0, '/Users/davidclausen/Galvanize/DSI/capstone_project')
from recommender import BookRecommender, remove_header_reduce_columns

app = Flask(__name__)

def load_and_train_recommender(reader_id):
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
    # return book_rec
    books = book_rec.recommend_books(reader_id=reader_id)
    return books


# def getBooks(reader_id):
#     # book_rec = load_and_train_recommender(reader_id)
#     # books = book_rec.recommend_books(reader_id)
#     books = load_and_train_recommender(reader_id)
#     print '\n*******{}*******\n'.format(books)
#     return books
	# formattedBooks = formatBooks(books)

# def formatBooks(books):
# 	return []


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/reader_id', methods=['GET', 'POST'])
def reader_id():
	print '\n*******{}*******\n'.format(reader_id)

@app.route('/display_recommendations', methods=['POST', 'GET'])
	if reader_id is None:
		return  render_template('reader_id.html')
	else:
		booklist = load_and_train_recommender(reader_id)#getBooks(reader_id)
        # print '\n*******{}*******\n'.format(bookList)

		# booklist = ["The Linebacker's Secret Baby",
        #              'Me and Fat Marge: An Erotic Short Story',
        #              'Touched']

		return render_template('bookList.html', booklist=booklist)



if __name__ == '__main__':

    app.run(host='0.0.0.0', port=8081, debug=True, threaded=True)


# @app.route('/')
# def index():
#
# 	return render_template('index.html')
#
#
# @app.route('/enter_reader_id', methods=['GET', 'POST'])
# def enter_reader_id():
#
# 	return render_template('some.html', hikes=hikes)
#
# @app.route('/display_recommendations', methods=['POST', 'GET'])
# def display_recommendations():
#     # book_recommender.recommend_books(reader_id=)
# 	return render_template('some.html', best_hikes=best_hikes)
#
#
# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port=8081, debug=True, threaded=True)
