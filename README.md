# InstaBookRec  

<img src="images/break_line.png">

### A Book Recommendation Algorithm for the Company Instafreebie
Galvanize Data Science Immersive - Capstone Project - March 2017

### David E Clausen
Data Scientist | Mathematician

<img src="images/break_line.png">

## Table of Contents
<!-- - [Purpose](#purpose) -->
- [Overview](#overview)
- [Recommendation Systems](#recommendation-systems)
- [Data](#data)
- [Models](#models)
- [Deployment](#deployment)
- [About Me](#about-me)
- [Resources](#resources)

<img src="images/break_line.png">

<!-- ## Purpose

I became a data scienctist because I wanted to use my talents to help companies succeed.  This is why I decided to work with a company for my capstone project at [Galvanize](http://www.galvanize.com).  

<img src="images/break_line.png">
 -->
## Overview
I became a data scienctist because I wanted to use my talents to help companies succeed.  This is why I decided to work with a company for my capstone project at [Galvanize](http://www.galvanize.com).  

For my project I collaborated with the company [Instafreebie](https://www.instafreebie.com) to improve their book recommendation algorithm.  Instafreebie seeks to connect authors with the right readers by offering a platform for authors to distribute their work electronically and expand their readership through sharing portions of their work in exchange for email sign-ups.  

When a book is downloaded from Instafreebie, three new books are recommended to the reader.  Good book recommendations are important for the company. Better recommendations result in more downloads, which connect authors to new readers and create more business for Instafreebie.  

Their algorithm was self-described as "naive" because it did little more than make recommendations based on genre.  This algorithm resulted in a 9.5% download rate of recommendations it made.

My task was to create a recommendation algorithm that would result in more downloads.

<img src="images/break_line.png">

## Recommendation Systems
Intro

Collaborative Filtering
- Memory-based
     + item-item similarity
     + user-user similarity

- Model-based
    + clustering
    + matrix factorization

Content-based Filtering

<img src="images/break_line.png">

## Data
Instafreebie gave me access to tables on their MySQL database.  The table with the most relevant information was a log of previously made recommendations.  There were 29,591,800 recommendations to 508,163 readers spanning 10,078 books.

There were 32 genres represented in the ten thousand books, but nearly one third of the books were from the genre of Romance.  This is significant because it was important that the recommender I built did more than just suggest books from the same genre as the book that was downloaded, otherwise it would likely perform similarly to the company's current algorithm. Also, with Romance dominating the genre distribution, it could propose a challenge to suggest books from other genres.

This log also kept track of whether or not the recommendations were downloaded by the users.  This type of data is implicit rather than explicit.  Implicit data is an observed behavior of a user towards an item and is typically binary (i.e. downloaded or not downloaded, clicked on or not clicked on), as opposed to explicit data, which is a conscious rating of an item by a user and is usually on some kind of a scale (i.e. 0 to 5 stars, or 1 to 10).


<img src="images/break_line.png">

## Models
Spark and GraphLab

I utilized the alternating least squares (ALS) algorithm in GraphLab Create and Spark to construct two different model-based collaborative filtering recommender systems.  

Performance

<img src="images/break_line.png">

## Deployment
Web App

Add screenshots

<img src="images/break_line.png">

## About Me
<div style="text-align:center">
<img src="images/IMG_1968_square.PNG" width=30% height=30%/>
</div><br>

I am a Data Scientist living in Denver, Colorado. I believe in harnessing the power of computers to derive actionable insights from data in order to make more informed business decisions.

In 2013, I graduated from the University of Colorado, Boulder with a degree in Mathematics.  I spent 3 years working as the Head of Human Resources and Accounts Receivable/Payable Manager for Smiley Inc., a small construction company in Boulder that specializes in historic remodels and renovations.

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
