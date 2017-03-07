from flask import Flask, request, render_template
import pyspark as ps
import pandas as pd
import random
import graphlab as gl

app = Flask(__name__)

def remove_columns_from_recommend_log(sf):
    sf.remove_columns(['X1',
                         'id',
                         'source',
                         'from_book_id',
                         'ad_id',
                         'boost_id',
                         'clicked',
                         'optin',
                         'created_at',
                         'updated_at'])
    return sf

def get_titles_from_book_ids(book_ids_list, library):
    titles_list = []
    for book in book_ids_list:
        titles_list.append([library[library['id'] == book][0]['title'], library[library['id'] == book][0]['genre_id'], library[library['id'] == book][0]['genre_id_2']])
    return titles_list

def get_genre_from_id(titles_list, genres):
    titles_with_genres_list = []
    for title in titles_list:
        if title[2] == None:
            titles_with_genres_list.append([title[0], genres[genres['id'] == title[1]][0]['genre'], None])
        else:
            titles_with_genres_list.append([title[0], genres[genres['id'] == title[1]][0]['genre'], genres[genres['id'] == title[2]][0]['genre']])
    return titles_with_genres_list


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/reader_id', methods=['GET', 'POST'])
def reader_id():
	return  render_template('reader_id.html')

@app.route('/display_recommendations', methods=['POST', 'GET'])
def display_recommendations():
    reader_id = request.form.get('reader_id')
    if reader_id is None:
        return  render_template('bookList.html')
    else:
        book_ids_list = model.recommend([reader_id],5)['book_id']
        titles_list = get_titles_from_book_ids(book_ids_list, library)
        booklist = get_genre_from_id(titles_list, genres)
        read_ids_list = list(recommend_log[(recommend_log['reader_id'] == int(reader_id)) & (recommend_log['claimed'] == 1)]['book_id'])
        read_titles_list = get_titles_from_book_ids(read_ids_list, library)
        readlist = get_genre_from_id(read_titles_list, genres)
        return render_template('bookList.html', booklist=booklist, readlist=readlist)



if __name__ == '__main__':
    model = gl.load_model('graphlab_recommender_subset')
    library = gl.SFrame.read_csv('../data/books_information.csv').remove_column('X1')
    genres = gl.SFrame.read_csv('../data/instafreebie2-21-17_genres.csv')
    rec_log = gl.SFrame.read_csv('../data/subset.csv')
    recommend_log = remove_columns_from_recommend_log(rec_log)
    app.run(host='0.0.0.0', port=8082, debug=True, threaded=True)
