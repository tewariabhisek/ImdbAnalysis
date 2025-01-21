from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType


def define_schemas():
    """
    Define schemas for the datasets.
    """
    movies_schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("titleType", StringType(), True),
        StructField("primaryTitle", StringType(), True),
        StructField("originalTitle", StringType(), True),
        StructField("isAdult", StringType(), True),
        StructField("startYear", StringType(), True),
        StructField("endYear", StringType(), True),
        StructField("runtimeMinutes", StringType(), True),
        StructField("genres", StringType(), True)
    ])

    ratings_schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("averageRating", FloatType(), True),
        StructField("numVotes", IntegerType(), True)
    ])

    principals_schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("ordering", StringType(), True),
        StructField("nconst", StringType(), True),
        StructField("category", StringType(), True),
        StructField("job", StringType(), True),
        StructField("characters", StringType(), True)
    ])

    return movies_schema, ratings_schema, principals_schema
