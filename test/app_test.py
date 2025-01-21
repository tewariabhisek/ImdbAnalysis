import pytest
from unittest.mock import patch
from pyspark.sql import SparkSession
from main.schema import define_schemas
from main.app import ImdbAnalysis
import logging

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .master("local") \
        .appName("WorkflowTest") \
        .getOrCreate()

def test_workflow(spark):
    # Sample test data
    movies_data = [
        ("tt001", "movie", "Movie A", "Movie A", "0", "2000", "\\N", "120", "Action"),
        ("tt002", "movie", "Movie B", "Movie B", "0", "2010", "\\N", "90", "Comedy"),
        ("tt003", "short", "Movie C", "Movie C", "0", "2005", "\\N", "30", "Drama"),
        ("tt004", "movie", "Movie D", "Movie D", "0", "2020", "\\N", "110", "Thriller"),
    ]

    ratings_data = [
        ("tt001", 8.5, 600),
        ("tt002", 7.0, 500),
        ("tt003", 9.0, 100),
        ("tt004", 8.0, 700),
    ]

    principals_data = [
        ("tt001", "1", "nm001", "actor", "\\N", "\\N"),
        ("tt001", "2", "nm002", "director", "\\N", "\\N"),
        ("tt002", "1", "nm001", "actor", "\\N", "\\N"),
        ("tt002", "2", "nm003", "writer", "\\N", "\\N"),
        ("tt004", "1", "nm004", "actor", "\\N", "\\N"),
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
    assert analysis.input_rating_file_path ==  "ratings.tsv"

    # Create DataFrames
    movies_stream = spark.createDataFrame(movies_data, schema=movies_schema)
    ratings_stream = spark.createDataFrame(ratings_data, schema=ratings_schema)
    principals_stream = spark.createDataFrame(principals_data, schema=principals_schema)

    # Run the workflow

    ranked_movies, most_credited, top_titles = analysis.process_data(movies_stream, ratings_stream, principals_stream)

    # Assertions
    # Verify top 10 movies
    assert ranked_movies.count() == 3  # Only movies with numVotes >= 500

    top_movie = ranked_movies.collect()[0]
    assert top_movie["tconst"] == "tt004"  # Highest rank based on formula

    # Verify most credited persons
    assert most_credited.count() > 0  # Ensure credited persons are listed

    # Verify top titles
    assert top_titles.count() == 3  # Titles of top movies
    actual_titles = [row["Movie Title"] for row in top_titles.collect()]
    assert actual_titles == ["Movie D", "Movie A", "Movie B"]
