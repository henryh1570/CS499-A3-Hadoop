Folder hadoop1
==============

contains the MapReduce code for generating a text file that 
contains the user_IDs followed by their review count. Input file is  TrainingRatings.txt

Folder hadoop2
==============

contains the MapReduce code for generating a text file that 
contains the movie_IDs followed by their rating from a particular user 
Input file is TrainingRatings.txt

Folder RatingParser
===================

contains java code to produce a result of the top 10 
users that have the most reviews for the Map-reduce output in folder hadoop1.

Folder MovieParser
==================

contains java code to produce a result of the top 10 movies based on 
highest average ratings from reviewers. Input is the Map-reduce output in 
folder hadoop2 and movie_titles.txt

