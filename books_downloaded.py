import pandas as pd


def total_downloaded(df):
    '''
    I am not sure if this works correctly
    It is supposed to make a pandas df of all books downloaded, by adding from_book_id's to claimed books
    This was taking a lot of time and I decided it was not critical to finishing my project, so I just went with the total_claimed()
    '''
    claimed_df = df.loc[df['claimed'] == 1].groupby('reader_id')['book_id'].apply(list)
    from_book_df = df.groupby('reader_id')['from_book_id'].unique().apply(list)

    for reader in claimed_df.index:
        for value in claimed_df[claimed_df.index == reader].values[0]:
            from_book_df[from_book_df.index == reader].values[0].append(value)
    return from_book_df

def total_claimed(df):
    '''
    Makes pandas dataframe with reader_id and list of all books claimed
    '''
    claimed_df = df.loc[df['claimed'] == 1].groupby('reader_id')['book_id'].apply()
    return claimed_df


if __name__ == '__main__':
    # rec_log_df = pd.read_csv('data/instafreebie2-21-17_recommend_log.csv')
    # subset_df = pd.read_csv('data/subset.csv')
    #
    # total_claimed_books = total_claimed(rec_log_df)
    # subset_claimed_books = total_claimed(subset_df)

    # total_claimed_books.to_csv('data/claimed_books_by_reader.csv')
    # subset_claimed_books.to_csv('data/subset_claimed_books_by_reader.csv')
