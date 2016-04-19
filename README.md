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
4. Refine the word count result, clean the identical words which are in top position

     
