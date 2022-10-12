import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.SparkSession

object Producer {
  def main(args: Array[String]): Unit = {

    println("Starting Spark session...")

    val spark = SparkSession.builder()
      .appName("Exo Archi React Producer")
      .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0")
      .config("spark.master", "local").getOrCreate()

    println("Reading csv src/main/resources/metadata.csv")

    // Reading csv file with spark
    val df = spark.read
      .schema(Config.DataSchema)
      .option("header", "true")
      .csv("src/main/resources/metadata.csv")

    println("Writing to kafka.")

    df.toJSON
      .limit(1000)
      .withColumn("key", lit("keyname"))
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "covid-dataset")
      .save()

    println("Writing done...")

  }
}