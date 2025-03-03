    The main question that I am trying to answer using the Letterboxd movie dataset is whether there is a significant distance in the duration of movies over time. 
    To answer this question, I completed some data preprocessing steps and exploratory analysis. Upon examining the data, I immediately noticed that all of the data 
    tables had an id column that referenced the id of the movies. I discovered that in order to answer my question, I didn't need all of the data tables. I located 
    the three that I needed, movies.csv, genres.csv, and themes.csv, to perform my analysis. I used duckdb to create tables from these csv files and join them together 
    based on the movie id. I did not run into many issues preprocessing the data, however learning the correct syntax and what each function in duckdb performs was an 
    initial issue that took some time for me to overcome. I computed some summary statistics, which were the average length of each movie. To visualize my data, I 
    created faceted histograms of movie times across the different genres. I imported seaborn and matplotlib to crease these plot. I found that most of the movie times 
    were normally distributed, with a mean time between 100 and 140 minutes. While these findings were insightful, I decided to take them one step further by plotting 
    the relationship between movie time and overall rating. There was plenty of variability in rating around the mean movie time for each genre of movie, which I expected. 
    However, what surprised me was that longer movies tended to have higher ratings across the board. With people's attention spans shrinking with the influx of Instagram 
    reels, YouTube shorts, and Tik Tok, I expected longer movies to be met with less satisfaction from the public due to overall changes in human behavior. However, the 
    opposite seemed to be apparent. Looking forward, another interesting question to explore would be why longer movies are rated higher. As I continue with my analysis, 
    I will explore the other files in the dataset to see if other information, like language or country the movie was set in, influences viewer satisfaction. 