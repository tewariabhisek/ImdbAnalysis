# ImdbAnalysis

This script performs the following tasks:

1. **Retrieve Top 10 Movies**  
   - Identify the top 10 movies with a minimum of 500 votes.  
   - The ranking is determined using the formula:  
     ```
     Ranking Score = (numVotes / averageNumberOfVotes) * averageRating
     ```

2. **List Movie Details**  
   - For the top 10 movies, extract the following details:  
     - The names of the persons who are most frequently credited.  
     - The different titles of the top 10 movies.

# Deployment and Running Steps on a Local Windows Machine

Assuming that the environment setup is already done in the local machine, follow these steps to deploy and run the script on your local machine:

## 1. Prepare the Environment
- Create a ZIP file of the main folder containing the required scripts and dependencies.  
- Place the ZIP file in a folder accessible by Spark.

## 2. Run the Script
- Navigate to the `bin` folder in your Spark installation directory.  
- Use the following command to execute the script locally:

```bash
spark-submit --master local[*] --deploy-mode client --name prodimdb \
--driver-memory 4g --executor-memory 2g --executor-cores 2 \
--conf spark.sql.shuffle.partitions=200 \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.initialExecutors=2 \
--conf spark.dynamicAllocation.maxExecutors=10 \
--py-files <path_to_created_zip_file> <path_to_app.py> <path_to_principals_file> \
<path_to_movies_file> <path_to_ratings_file> <path_to_log_file>
```
# Example Command
```bash
spark-submit --master local[*] --deploy-mode client --name prodimdb \
--driver-memory 4g --executor-memory 2g --executor-cores 2 \
--conf spark.sql.shuffle.partitions=200 \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.initialExecutors=2 \
--conf spark.dynamicAllocation.maxExecutors=10 \
--py-files E:\pycharmprojects\main.zip \
E:\pycharmprojects\ImdbAnalysis\main\app.py \
E:\pycharmprojects\ImdbAnalysis\data\input_files\principal\title.principals.tsv \
E:\pycharmprojects\ImdbAnalysis\data\input_files\movies\title.basics.tsv \
E:\pycharmprojects\ImdbAnalysis\data\input_files\ratings\title.ratings.tsv \
E:\pycharmprojects\ImdbAnalysis\logging\logger.log
```

## Dependencies

Ensure the following dependencies are installed on your local machine to run the program:

1. **Python 3.11**  
   - Download and install Python from the official website.  
   - Make sure Python is added to your system's PATH.

2. **Apache Spark 3.5.4**  
   - Download and set up Apache Spark 3.5.4 from the official website.  
   - Ensure the `SPARK_HOME` environment variable is correctly configured.

3. **Hadoop 3**  
   - Download and install Hadoop 3 winutls file from github.  
   - Make sure the `HADOOP_HOME` environment variable is set and the required binaries are accessible in your PATH.

## Conclusion

## Conclusion

This project involves the development of a batch job designed to generate a daily report. The process assumes that the input file will be prepared each day, and the job will process it to produce the required output.

For testing and demonstration purposes, the output is currently printed to the console. However, in a production environment, the output would be stored in a date-partitioned directory structure. The corresponding code is already included in the script but is currently commented out.

This batch job can also be adapted into a streaming application with minimal changes. By modifying the data reading section to fetch data from a Kafka stream or to read from an input file stream, the application could process real-time or continuously updated data.

### Testing
Attached is a screenshot of the successful execution of the unit test classes. The classes under the `test` folder have been used for testing.

![image](https://github.com/user-attachments/assets/34880e70-6424-4d2d-a703-a3f0742df540)





