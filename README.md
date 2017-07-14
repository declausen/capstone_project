# InstaBookRec  

<!-- <img src="images/break_line.png"> -->

### A Book Recommendation Algorithm for the Company Instafreebie
Galvanize Data Science Immersive - Capstone Project - March 2017

### David E Clausen
Data Scientist | Mathematician

<img src="images/break_line.png">

## Table of Contents
- [Overview](#overview)
- [Recommendation Systems](#recommendation-systems)
- [Data](#data)
- [Models](#models)
- [Deployment](#deployment)
- [About Me](#about-me)
- [Resources](#resources)

<img src="images/break_line.png">

## Overview
I became a data scienctist because I wanted to use my talents to help companies succeed.  This is why I decided to work with a company for my capstone project at [Galvanize](http://www.galvanize.com).  

For my project I collaborated with the company [Instafreebie](https://www.instafreebie.com) to improve their book recommendation algorithm.  Instafreebie seeks to connect authors with the right readers by offering a platform for authors to distribute their work electronically and expand their readership through sharing portions of their work in exchange for email sign-ups.  

When a book is downloaded from Instafreebie, three new books are recommended to the reader.  Good book recommendations are important for the company. Better recommendations result in more downloads, which connect authors to new readers and create more business for Instafreebie.  

Their algorithm was self-described as "naive" because it did little more than make recommendations based on genre.  This algorithm resulted in a 9.5% download rate of recommendations it made.

My task was to create a recommendation algorithm that would result in more downloads.

<img src="images/break_line.png">

## Recommendation Systems
Recommendation systems have become increasingly more popular in recent years as a way to increase business.  Netflix, Pandora and Amazon are just a few examples of companies that use a well defined process to make suggestions about new movies/television shows, songs/artists and products to consumers.  In a broad sense, recommendation systems predict the level of interest a user has in a new item.

There are two distinct techniques that are used to make these predictions, collaborative filtering and content-based filtering.  **Collaborative filtering** uses information collected from a user's past decisions, combined with information gathered from other users with similar behavior, to make recommendations.  You can think of it as a "collaboration" of many users to help out one user.  **Content-based filtering** systems make suggestions to a user based off of comparing descriptions of items to the user's preferences.  This method utilizes some "content" of the items to match them with a user.

Below is a general break down of the two types of recommendation systems:

1) Collaborative Filtering
- Memory-based
     + item-item similarity
     + user-user similarity

- Model-based
    + clustering
    + matrix factorization

2) Content-based Filtering

**Memory-based** collaborative filtering calculates similarity scores between users and items based upon the rating data of the users.  These scores are then employed to suggest items to users.  These scores must be saved and in a sense committed to "memory" in order to utilize them.

**Model-based** approaches to recommendation systems use machine learning algorithms to uncover correlations between users and items within a set of data.  These correlations in the data are commonly referred to as latent features and are used to make predictions for users.  This technique relies heavily on a mathematical "model" in order to make recommendations.

It is also possible to combine the collaborative and content-based approaches into one model and create a hybrid recommendation system.

<img src="images/break_line.png">

## Data
Instafreebie gave me access to tables on their MySQL database.  The table with the most relevant information was a log of previously made recommendations.  There were 29,591,800 recommendations to 508,163 readers spanning 10,078 books.

###### Figure 1: The Distribution of Books between Genres
<div style="text-align:center">
<img src="images/genre_graph.png" width=75% height=75%/>
</div>

It was important that the recommender I built did more than just suggest books from the same genre as the book that was downloaded, otherwise it would likely perform similarly to the company's current algorithm.  There were 32 genres represented in the ten thousand books, and nearly one third of the books were from the genre of Romance.  With Romance dominating the genre distribution, it could propose a challenge to suggest books from other genres.

This log also kept track of whether or not the recommendations were downloaded by the users.  This type of data is implicit rather than explicit.  Implicit data is an observed behavior of a user in regard to an item and is typically binary (i.e. downloaded or not downloaded, clicked on or not clicked on), as opposed to explicit data, which is a conscious rating of an item by a user and is usually on some kind of a scale (i.e. 0 to 5 stars, or 1 to 10).


<img src="images/break_line.png">

## Models
I wanted to develop a model that would suggest books outside of the genre of the downloaded book, but also make certain that it did not solely suggest Romance novels.

I utilized the alternating least squares (ALS) algorithm in GraphLab Create and Spark to construct two different model-based collaborative filtering recommender systems.  The ALS method is an example of the broader strategy of matrix factorization.  **Matrix factorization**, also called matrix decomposition, is a technique within linear algebra where a matrix is factored into a product of simpler component matrices.

###### Figure 2: Matrix Factorization
<div style="text-align:center">
<img src="images/matrix_factorization1.png">
</div>
<!-- Matrix R <sub><sup>combining the two tags</sup></sub>
<sub>combining the two tags</sub> -->
<!-- <br> -->

The image above illustrates matrix factorization for my case.  R represents the rating matrix, which is a readers by books matrix that is populated with the ratings of the books by the readers.  For clarity, reader i's rating of book j is the intersection of the two brown lines. Matrix factorization is when R decomposes into the matrices U and V, whose product is an approximation of R.  U is a readers by latent features matrix, and V is a latent features by books matrix.  You can think of the latent features as unlabeled hidden genres of the books that are deduced by the matrix facotrization.  In a sense, V describes how much each book scores according to these latent features, and U describes how much each reader likes each latent feature.  The number of latent features is called the **rank** of the factorization.  The rank will typically be less than the number of items and the number of users.

(The first step of the alternating least squares algorithm is to initialize the V matrix.)  Matrix decomposition through the alternating least squares algorithm is accomplished by first initializing the V matrix.  This is done by making the first row be the average ratings of each book, and then filling the rest of the matrix with random small numbers.  Once V is initialized, it is held constant while U is adjusted to minimize a cost function.  When the cost function has been sufficiently minimized, ALS alternates U and V, so U is held constant and V is adjusted to minimize the cost function.  This process is continued until the product of U and V is a close enough approximation of R, or for a specified amount of iterations.

After using a portion of the dataset to train the models, they were ready to have their performance tested.  I did this by comparing the root mean squared errors (RMSEs) of my models to that of a baseline model.  In this case, the RMSE is basically how closely the algorithms predicted the readers' interest levels in the books. The Spark and GraphLab recommender systems performed similarly at 0.289 and 0.287 respectively, and both achieved a better score than the baseline model, which had an RMSE of 0.578.  The baseline I used was a uniform random model.  This means that my algorithms were twice as good as randomly guessing a reader's level of interest in a book.  But the best way to tell if the recommendation algorithm is working, is to see it in action.

<img src="images/break_line.png">

## Deployment
I created a web app in order to interface with my GraphLab recommender system.  Below are screen shots of my web app.

<img src="images/homepage.png" width=90% height=90%>

This is the homepage.

<img src="images/example1.png" width=90% height=90%>

Enter a reader ID in order to receive recommendations for a specific reader.

<img src="images/example1_results.png" width=90% height=90%>

This page displays five recommended books, as well as up to ten previously read books.  The books are posted along with their primary and secondary genres.  This example illustrates how the algorithm was able to recommend all the genres in the previously read books and in similar proportions.

<img src="images/example2_results.png" width=90% height=90%>

These are recommendations for another reader.  This example demonstrates how  with only one previously read book, my recommender was able to correctly recommend another thriller as the top suggestion, and also branch out to other genres.  When there is limited information about the user, it can be difficult to make good recommendations.

Thank you for your interest in my project.  Please feel free to contact me with any questions, concerns or comments.

<img src="images/break_line.png">

## About Me
<div style="text-align:center">
<img src="images/headshot_square.PNG" width=30% height=30%/>
</div><br>

I am a Data Scientist living in Denver, Colorado. I believe in harnessing the power of computing to derive actionable insights from data in order to make more informed business decisions.

In 2013, I graduated from the University of Colorado, Boulder with a degree in Mathematics.  I spent 3 years working as the Head of Human Resources and Accounts Receivable/Payable Manager for Smiley Inc., a small construction company in Boulder that specializes in historic residential remodels and renovations.

At Galvanize, I advanced my skills in machine learning, statistical analysis, and computer programming.  I seek to fully utilize my abilities as a data scientist and mathematician in order to be a useful and effective member of a company.

#### Contact Information

Email: davideclausen@gmail.com

Linkedin: [/in/declausen](https://www.linkedin.com/in/declausen/)


<img src="images/break_line.png">

## Resources

#### Similar Past Galvanize Capstone Projects

* Sal Khan - [Electronic Music Recommender](https://github.com/salmank09/musicrecommender)
* Olivia Schow - [Take-A-Hike: A Colorado Trail Recommender](https://github.com/oschow/take-a-hike)


#### Online

* Analytics Vidhya (uses GraphLab Create) - [Quick Guide to Build a Recommendation Engine in Python](https://www.analyticsvidhya.com/blog/2016/06/quick-guide-build-recommendation-engine-python/)
* Turi-code (uses GraphLab Create) - [Movie Recommender Sample Project](https://github.com/turi-code/sample-movie-recommender)
* Apache Spark - [Collaborative Filtering](https://spark.apache.org/docs/latest/ml-collaborative-filtering.html)
