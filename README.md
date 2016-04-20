# Yelp-Review-Analysis

This is the Final Project of Course CSYE7200 Big Data System engineering Using Scala
The main goal is to predict the star level of a certain review context through data parser, word count, identical words clean, machine learning. Also, it analysi the stars variation in different years. 

Zeppelin is used to visualize the results. 

# Data Source

Yelp Dataset Challenge: https://www.yelp.com/dataset_challenge

# Data Preparation for ML
1. Add this Dependency to the build.sbt to Parse the JSON file
    libraryDependencies ++= Seq("com.typesafe.play" % "play-json_2.11" % "2.5.1")
2. Remove the reviews of No English speak county, like Germany(Karlsruhe)
   Remove the special signs and unaccent letters, like #, ^, &, and èéêë.
   Then get the clear review context in five star level, and save them into five file according to star level
3. Use Spark MapReduce to word count
4. Refine the word count result, clean the identical words which are in top position.
    - extract the identical words in the top 1000  of the five wordcount files.
    - remove the identical words from each wordcount files.
    - do step 1 and 2 for several times.
    Then, we get the feature words sets of different star levels.
5. Covert these five feature words sets into a certain format for data training

# Machine Learning
1.	Problem: 
This is a Multiple Classification Problem which we will classify the reviews to 5 classes (from 1 star to 5 star). So, the algorithms that we will use should solve this kind of problem
2.	Algorithms:
We chose 4 algorithms to training model, there are Boosted Decision Tree, Random Forest Tree, Naïve Bayes and Neural Network.
3.	Objects:
a.	mlNB is training Naïve Bayes model
b.	mlNerualNetwork.scala is object to train NerualNetwork Model
c.	mlRF is object to train Random Forest Model
d.	mlDT is object to train Decision Tree Model
4.	Apply model:
In the predict folder, object reviewPridect is used to apply model to classify reviews.



#Zeppelin
Install video: https://www.youtube.com/watch?v=CfhYFqNyjGc
     
