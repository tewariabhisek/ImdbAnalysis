import sys

from main.logger import Logger
#import os

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, count, mean, desc
#from datetime import datetime

from main.schema import define_schemas

class ImdbAnalysis:

    def __init__(self, logger):
        self.logger = logger
        try:
            #Input file paths
            self.input_principal_file_path = sys.argv[1]
            self.input_movie_file_path = sys.argv[2]
            self.input_rating_file_path = sys.argv[3]

            self.logger.info("ImdbAnalysis initialized with input file paths")

            #Output file paths
            # self.output_principal_file_path = sys.argv[4]
            # self.output_movie_file_path = sys.argv[5]
            # self.output_rating_file_path = sys.argv[6]
        except IndexError:
            self.logger.error("Missing arguments for input/output file paths.")
            sys.exit(1)

    def create_spark_session(self):
        """
        Create and configure a Spark session.
        """
        return SparkSession.builder.master("local[*]").appName("IMDbAnalysis").getOrCreate()


    def read_data(self, file_path, schema, spark):
        try:
            """
            Read the files and store the data in a dataframe
            :return:
            """
            self.logger.info(f"Reading data from {file_path}.")
            df = spark.read.options(header=True, delimiter='\t').schema(schema).csv(file_path)
            self.logger.info(f"Successfully read data from {file_path}.")
            return  df
        except Exception as e:
            self.logger.error(f"Error reading data from {file_path}: {e}")
            raise

    def process_data(self,movies_df, ratings_df, principal_df):

        try:

            """
            Process the streams to compute the top 10 movies and associated information.
            """
            self.logger.info("Processing movie, rating, and principal data.")

            # Join movies and ratings, filter for movies with min 500 votes
            final_joined_df = movies_df.join(ratings_df, movies_df.tconst == ratings_df.tconst, "inner").drop(
                ratings_df.tconst).filter((col("titleType") == "movie") & (col("numVotes") >= 500))

            # Compute average number of votes across all movies
            avg_vote = final_joined_df.select(mean("numVotes")).collect()[0][0]
            self.logger.info(f"Average vote count: {avg_vote}")

            # Calculate ranking
            ranked_data = final_joined_df.withColumn("ranking", ((col("numVotes") / avg_vote) * col("averageRating")).cast(
                IntegerType())).select("tconst", "primaryTitle", "ranking").orderBy(desc("ranking")).limit(10)

            self.logger.info("Successfully calculated top 10 movies based on ranking.")

            # Get top movie IDs for further analysis
            top_movie_df = ranked_data.select("tconst")

            # Find most credited persons for the top 10 movies
            relevant_person = principal_df.join(top_movie_df, principal_df.tconst == top_movie_df.tconst, "inner").drop(
                top_movie_df.tconst).groupBy("nconst").agg(count("tconst").alias("credit_count")).orderBy(
                desc("credit_count")).limit(10)

            # Extract titles of the top movies
            top_titles = ranked_data.select(col("primaryTitle").alias("Movie Title"))

            self.logger.info("Successfully processed streams and obtained top titles and credits.")

            return ranked_data, relevant_person, top_titles
        except Exception as e:
            self.logger.error(f"Error processing data: {e}")
            raise

    # def create_output_path(self,file_path, job_run_date):
    #     return os.path.join(file_path, job_run_date) + "/"

    # def write_data(self, dataframe, file_path):
    #     dataframe.coalesce(1).write.option("header", "true").csv(file_path)

    def write_output_to_console(self, ranked_data, relevant_person, top_titles):
        try:
            self.logger.info("Displaying results on the console...")
            ranked_data.show()
            relevant_person.show()
            top_titles.show()

            self.logger.info("Successfully displayed the results.")
        except Exception as e:
            self.logger.error(f"Error displaying results: {e}")
            raise


def main():
    # Create Spark session
    log_file_path = sys.argv[4]
    log = Logger()
    logger = log.get_logger(log_file_path)

    imdb_analysis = ImdbAnalysis(logger)
    spark = imdb_analysis.create_spark_session()
    try:
        #Get today's date for partition
        #job_run_date = datetime.today().strftime('%Y-%m-%d')

        #Load schema for each file
        movies_schema, ratings_schema, principals_schema = define_schemas()
        logger.info("Created schema for all the input files")

        #Read the files and store in a dataframe
        logger.info("Reading input data...")
        principal_df = imdb_analysis.read_data(imdb_analysis.input_principal_file_path, principals_schema, spark)
        movies_df = imdb_analysis.read_data(imdb_analysis.input_movie_file_path, movies_schema, spark)
        ratings_df = imdb_analysis.read_data(imdb_analysis.input_rating_file_path, ratings_schema, spark)

        #Process the input files
        ranked_data, relevant_person, top_titles = imdb_analysis.process_data(movies_df, ratings_df, principal_df)

        #Generate Output path
        # output_principal_file_path = imdb_analysis.create_output_path(imdb_analysis.output_principal_file_path, job_run_date)
        # output_movie_file_path = imdb_analysis.create_output_path(imdb_analysis.output_movie_file_path, job_run_date)
        # output_rating_file_path = imdb_analysis.create_output_path(imdb_analysis.output_rating_file_path, job_run_date)

        #imdb_analysis.write_data(ranked_data, output_movie_file_path)
        #imdb_analysis.write_data(relevant_person, output_principal_file_path)
        #imdb_analysis.write_data(top_titles, output_rating_file_path)

        imdb_analysis.write_output_to_console(ranked_data, relevant_person, top_titles)
    except Exception as e:
        logger.error(f"Error during processing: {e}")
    finally:
        logger.info("Stopping Spark session...")
        spark.stop()

if __name__ == "__main__":
    main()


