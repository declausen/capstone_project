import graphlab as gl
import pandas as pd
import cPickle as pickle



def create_train_save_model(sf):
    model = gl.recommender.factorization_recommender.create(sf, user_id='reader_id', item_id='book_id', target='claimed', num_factors=36, solver='als', regularization=0.01, random_seed=17)
    model.save('web_app/graphlab_recommender')


if __name__ == '__main__':
    # Load Data
    sf = gl.SFrame.read_csv('data/instafreebie2-21-17_recommend_log.csv')
    # sf = gl.SFrame.read_csv('data/subset.csv')
    sf.remove_columns(['id',
                         'source',
                         'from_book_id',
                         'ad_id',
                         'boost_id',
                         'clicked',
                         'optin',
                         'created_at',
                         'updated_at'])

    create_train_save_model(sf)
