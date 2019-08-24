import configparser
import os

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, \
    hour, weekofyear
from pyspark.sql.types import StructType as R, StructField as Fld, \
    DoubleType as Dbl, StringType as Str, IntegerType as Int, \
    DateType as Dat, TimestampType

config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Get or create a Spark Session
    """
    return SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()


def process_song_data(spark, input_data, output_data):
    """ Loads song_data from S3 bucket specified by input_data, then extracts
        the songs and artist tables to load into S3 bucket specified by output_data

        Parameters:
            spark: Spark session
            input_data: location of input data
            output_data: location of output data

        Returns:
            None
    """

    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    songSchema = R([
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_name", Str()),
        Fld("duration", Dbl()),
        Fld("num_songs", Int()),
        Fld("title", Str()),
        Fld("year", Int()),
    ])

    df = spark.read.json(song_data, schema=songSchema)

    # extract and write songs table
    song_fields = ["title", "artist_id", "year", "duration"]
    songs_table = df.select(song_fields) \
                    .dropDuplicates() \
                    .withColumn("song_id", monotonically_increasing_id())
    songs_table.write.partitionBy("year", "artist_id") \
                     .parquet(output_data + 'songs/', mode='overwrite')

    # extract and write artists table
    artists_fields = ["artist_id", "artist_name", "artist_location",
                      "artist_latitude", "artist_longitude"]
    artists_table = df.select(artists_fields).dropDuplicates()
    artists_table.write.parquet(output_data + 'artists/', mode='overwrite')

def process_log_data(spark, input_data, output_data):
    """ Loads log_data from S3 bucket specified by input_data, then extracts the
        users and time tables
        Also loads artist and song tables from S3 bucket specified by output_data
        Finally writes songplays table to S3 bucket specified by output_data

        Parameters:
            spark: Spark session
            input_data: location of input data
            output_data: location of output data

        Returns:
            None
    """

    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    df = spark.read.json(log_data)
    df = df.filter(df.page == 'NextSong')

    # extract and write users table
    users_fields = ["userdId", "firstName", "lastName", "gender", "level"]
    users_table = df.select(users_fields).dropDuplicates()
    users_table.write.parquet(output_data + 'users/', mode='overwrite')

    # extract and write times table
    get_datetime = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("datetime", get_datetime(df.ts))
    time_table = df.select(
        "datetime",
        hour("datetime").alias("hour"),
        dayofmonth("datetime").alias("day"),
        weekofyear("datetime").alias("week"),
        month("datetime").alias("month"),
        year("datetime").alias("year"),
        dayofweek("datetime").alias("weekday"))
    time_table.write.partitionBy("year", "month") \
                    .parquet(output_data + "time/", mode='overwrite')

    # load songs and artists table
    df_songs = spark.read.parquet(output_data + 'songs/*/*/*')
    df_artists = spark.read.parquet(output_data + 'artists/*')

    songs_logs = df.join(df_songs, (df.song == df_songs.title))
    artists_songs_logs = songs_logs.join(df_artists,
                                         (songs_logs.artist == df_artists.name))

    # extract and write songplays times table
    songplays = artists_songs_logs.join(
        time_table,
        artists_songs_logs.ts == time_table.start_time, "left"
    ).drop(artists_songs_logs.year)
    songplays_fields = ["datetime", "userId", "level", "song_id",
                        "artist_id", "sessionId", "location", "userAgent",
                        "year", "month"]
    songplays_table = songplays.select(songplays_field) \
                               .withColumn("songplay_id", monotonically_increasing_id())
    songplays_table.write \
                   .partitionBy("year", "month") \
                   .parquet(output_data + "songplays/", mode='overwrite')


def main():
    spark = create_spark_session()

    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-krchia/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
