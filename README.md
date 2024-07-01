# Movie Analysis with PySpark

## Table of Contents
1. [Introduction](#introduction)
2. [Setup and Installation](#setup-and-installation)
3. [Dataset](#dataset)
4. [Analysis](#analysis)
   - [Action Award-Winning Films](#action-award-winning-films)
   - [Award-Winning Actor Movies](#award-winning-actor-movies)
   - [Top Non-Award Winning Movies](#top-non-award-winning-movies)
   - [Least Popular Movies Before 1980](#least-popular-movies-before-1980)
   - [Average Length by Genre](#average-length-by-genre)
   - [Actors in Comedy and Drama](#actors-in-comedy-and-drama)
   - [Actors in Comedy or Drama](#actors-in-comedy-or-drama)
   - [Non-Comedy Actors](#non-comedy-actors)
   - [Actor Rankings](#actor-rankings)
   - [Movies Per Decade](#movies-per-decade)
   - [Movies Per Year](#movies-per-year)
   - [Movies Per Year and Genre](#movies-per-year-and-genre)
   - [Movies Before 1990](#movies-before-1990)
   - [Long Titles](#long-titles)
5. [Dependencies](#dependencies)
6. [Usage](#usage)
7. [Results](#results)

## Introduction
This project demonstrates how to use PySpark for analyzing a movie dataset. It includes various queries and transformations to extract insights such as popular movies, award-winning movies, and actor-related statistics.

## Setup and Installation
1. **Install PySpark:**
    ```sh
    pip install pyspark
    ```

2. **Prepare Input Data:**
    - Place your input CSV file (`Movies.csv`) in the same directory as your scripts.

## Dataset
The dataset used in this project is assumed to be a CSV file (`Movies.csv`) with the following columns:
- Title
- Year
- Genre
- Director
- Actor
- Actress
- Awards
- Popularity
- Length

## Analysis

### Action Award-Winning Films
- Filters movies that are of the "Action" genre and have won awards.

### Award-Winning Actor Movies
- Extracts movies with award-winning actors and lists the movies along with their directors.

### Top Non-Award Winning Movies
- Lists the top 10 non-award-winning movies based on popularity.

### Least Popular Movies Before 1980
- Lists the 10 least popular movies released before 1980.

### Average Length by Genre
- Calculates the average length of movies for each genre.

### Actors in Comedy and Drama
- Finds actors who have appeared in both comedy and drama movies.

### Actors in Comedy or Drama
- Lists actors who have appeared in either comedy or drama movies.

### Non-Comedy Actors
- Lists actors who have not appeared in comedy movies.

### Actor Rankings
- Provides rankings of actors based on the average, maximum, and minimum popularity of the movies they have appeared in.

### Movies Per Decade
- Counts the number of movies released per decade.

### Movies Per Year
- Counts the number of movies released per year.

### Movies Per Year and Genre
- Counts the number of movies released per year for each genre, filtered by movies longer than 100 minutes.

### Movies Before 1990
- Lists movies released before 1990, sorted by title.

### Long Titles
- Finds movies with titles longer than 50 characters.

## Dependencies
- `pyspark`

## Usage
1. **Start a PySpark Session and Load the Data:**
    ```python
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("MovieAnalysis") \
        .getOrCreate()

    movies_df = spark.read.csv("Movies.csv", header=True, inferSchema=True)
    ```

2. **Run the Analysis:**
    - Filter, transform, and query the data as described in the analysis section.

3. **Show Results:**
    - Display the results of each query using the `show()` method.

## Results
The output of each query will display the respective insights, such as lists of movies, actor statistics, and movie counts based on the analysis performed.
