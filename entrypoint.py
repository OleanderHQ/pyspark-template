from pyspark.sql import SparkSession

from mylib import greeting


def main():
    greeting()
    spark = SparkSession.builder.appName("oleander-pyspark-template").getOrCreate()

    try:
        spark.sql("SELECT 1 AS value").show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
