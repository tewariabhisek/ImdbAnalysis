# app_test_large_data.py
import logging
import sys

import pytest
from unittest.mock import patch
from pyspark.sql import SparkSession
from main.schema import define_schemas
from main.app import ImdbAnalysis

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("LargeDatasetWorkflowTest") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

def test_workflow_large_dataset(spark):
    # Generate 100 sample movie records
    movies_data = [
        (f"tt{i:06d}", "movie", f"Movie {chr(65 + (i % 26))}", f"Movie {chr(65 + (i % 26))}",
         "0", f"{2000 + (i % 20)}", "\\N", f"{90 + (i % 30)}", "Genre")
        for i in range(1, 100001)
    ]

    # Generate 100,000 sample ratings records
    ratings_data = [
        (f"tt{i:06d}", round(6.0 + (i % 4) * 0.5, 1), 400 + (i * 10))
        for i in range(1, 100001)
    ]

    # Generate 100,000 sample principals records
    principals_data = [
        (f"tt{i:06d}", f"{j}", f"nm{1000 + j}", "actor" if j % 2 == 0 else "director", "\\N", "\\N")
        for i in range(1, 100001) for j in range(1, 4)
    ]

    # Define schemas
    movies_schema, ratings_schema, principals_schema = define_schemas()

    test_logger = logging.getLogger("TestLogger")
    test_logger.addHandler(logging.NullHandler())

    args = ["app.py", "principal.tsv", "movies.tsv", "ratings.tsv", "logger.log"]
    with patch("sys.argv", args):
        analysis = ImdbAnalysis(test_logger)

    assert analysis.input_principal_file_path == "principal.tsv"
    assert analysis.input_movie_file_path == "movies.tsv"
    assert analysis.input_rating_file_path == "ratings.tsv"

    # Create DataFrames
    movies_stream = spark.createDataFrame(movies_data, schema=movies_schema)
    ratings_stream = spark.createDataFrame(ratings_data, schema=ratings_schema)
    principals_stream = spark.createDataFrame(principals_data, schema=principals_schema)

    # Run the workflow
    ranked_movies, most_credited, top_titles = analysis.process_data(movies_stream, ratings_stream, principals_stream)

    # Assertions
    # Verify top movies
    assert ranked_movies.count() == 10  # Verify only the top 10 movies are selected
    top_movie = ranked_movies.collect()[0]
    assert "tt" in top_movie["tconst"]  # Ensure a valid movie ID

    # Verify most credited persons
    assert most_credited.count() > 0  # Ensure credited persons are listed

    # Verify top titles
    assert top_titles.count() == 10  # Titles of top movies
    titles_list = [row["Movie Title"] for row in top_titles.collect()]
    # Assert all titles are strings
    assert all(
        isinstance(title, str) for title in titles_list), f"Non-string entries found in top_titles: {titles_list}"  # Ensure titles are strings

    # Print outputs for verification
    print("Ranked Movies:")
    ranked_movies.show()

    print("Most Credited:")
    most_credited.show()

    print("Top Titles:")
    print(top_titles)
