import pandas as pd
import matplotlib.pyplot as plt

def plot_books_genre(df, labels):
    x = range(1,33)
    plt.style.use('fivethirtyeight')
    plt.figure(figsize=(12,5))
    df.genre_id.hist(bins='auto', normed=True)
    plt.xticks(x, labels, rotation=90)
    plt.show()


if __name__ == '__main__':
    books_df = pd.read_csv('data/instafreebie2-21-17_books.csv')

    labels = ['', 'Action/Adventure', 'Arts/Photo',
       'Bios/Memoirs', 'Business', 'Children',
       'Comics/Graph. Novels', 'Contemp.',
       'Cooking/Food', 'Crime', 'Educ/Teaching',
       'Erotica', 'Fantasy', 'Health', 'Hist. Romance', 'History',
       'Horror', 'Humor/Entertain.', 'LGBT', 'Mystery', 'Paranormal',
       'Romance', 'SciFi', 'Self-Help', 'Spiritual/Relig.',
       'Teen/Young Adult', 'Thriller', 'Travel', "Women's Fiction",
       'Other(Fiction)', 'Other(Nonfiction)', 'Hist. Fiction']

    plot_books_genre(books_df, labels)
