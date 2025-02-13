from pyspark.sql import SparkSession, functions as f
from pyspark.sql.window import Window


def main():
    spark = SparkSession.builder \
        .appName("example") \
        .master("spark://spark:7077") \
        .getOrCreate()

    # Read text file as DataFrame
    movies_df = spark.read.text('/usr/local/app/data/raw/Movies.txt') \

    # Add id for each element
    movies_df = movies_df.withColumn(
        "ItemID",
        f.sum(
            f.when(f.col("value").startswith("ITEM "), 1).otherwise(0)
        ).over(Window.orderBy(f.monotonically_increasing_id()))
    )

    # Filter out unnecessary values
    movies_df = movies_df.filter(f.col("value").contains("="))

    # Split `value` column
    split_col = f.split(movies_df["value"], '=')
    movies_df = movies_df.withColumn('Column', split_col.getItem(0)) \
        .withColumn("Value", split_col.getItem(1)) \

    # Pivot rows into columns (Consider arrays)
    movies_df = movies_df.groupby("ItemID").pivot("Column") \
        .agg(f.first(f.col("Value")))

    # movies_df.write.mode("Overwrite").parquet("/usr/local/app/data/processed/movies.parquet")
    movies_df.select("ListPrice").show(100)

    spark.stop()


if __name__ == "__main__":
    main()