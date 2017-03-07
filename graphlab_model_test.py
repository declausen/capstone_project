import graphlab as gl

def remove_columns(sf):
    columns = ['id',
                 'source',
                 'from_book_id',
                 'ad_id',
                 'boost_id',
                 'clicked',
                 'optin',
                 'created_at',
                 'updated_at']
    for column in columns:
        sf.remove_column(column)
    return sf

if __name__ == '__main__':
    sf = gl.SFrame.read_csv('data/instafreebie2-21-17_recommend_log.csv')
    sf = remove_columns(sf)

    # Train Test Split
    train_sf, test_sf = gl.recommender.util.random_split_by_user(sf, user_id='reader_id', item_id='book_id', random_seed=17)

    # model = gl.recommender.factorization_recommender.create(train_sf, user_id='reader_id', item_id='book_id', target='claimed', random_seed=17)#, num_factors=14, solver='als')

    # Tune Parameters
    num_factors = [36]#, 30, 32, 34, 14, 18, 22, 26]
    regularization = [1e-8]#, 1e-10, 1e-9, 0.00000001, 0.000001, 0.0001, 0.01, 1.0]
    linear_regularization = [1e-10]#, 1e-12, 1e-11, 1e-8, 0.00000001, 0.000001, 0.0001, 0.01, 1.0]
    nmf = [True]#, False]
    solver = ['als']#'sgd', 'adagrad']

    model = gl.recommender.factorization_recommender.create(train_sf, user_id='reader_id', item_id='book_id', target='claimed', num_factors=36, solver='als', regularization=0.01, random_seed=17)


    # params = dict([('num_factors', num_factors),
    #                     ('regularization', regularization),
    #                     ('linear_regularization', linear_regularization),
    #                     ('nmf', nmf),
    #                     ('random_seed', 17),
    #                     ('user_id', 'reader_id'),
    #                     ('item_id', 'book_id'),
    #                     ('target', 'claimed'),
    #                     ('solver', solver)])
    # job = gl.grid_search.create(sf,
    #                             gl.recommender.factorization_recommender.create,
    #                             params)
    # job.get_results()

    # Evaluate Model
    rmse = model.evaluate_rmse(test_sf, target='claimed')
