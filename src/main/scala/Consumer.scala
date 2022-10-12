import com.datastax.spark.connector.cql.CassandraConnector
import com.sksamuel.elastic4s.ElasticApi.{createIndex, properties}
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.fields.TextField
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import com.sksamuel.elastic4s.http.JavaClient
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.functions.{col, exp, from_json}
import org.apache.spark.sql.streaming.OutputMode

object Consumer {
  def main(args: Array[String]): Unit = {
    writeToElastic()
    //    writeToCassandra()
  }

  def writeToCassandra(): Unit = {
    val conf = new SparkConf()
      .set("spark.cassandra.connection.host", "localhost")
      .setMaster("local[*]")
      .setAppName("Exo Archi React Cassandra Consumer");


    CassandraConnector(conf).withSessionDo { session =>
      session.execute("CREATE KEYSPACE IF NOT EXISTS covid WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute("CREATE TABLE IF NOT EXISTS covid.dataset " +
        "(cord_uidsha VARCHAR PRIMARY KEY, source_x VARCHAR, title VARCHAR, doi VARCHAR, pmcid VARCHAR, pubmed_id VARCHAR,"
        + "license VARCHAR, abstract TEXT, publish_time VARCHAR, authors VARCHAR, journal VARCHAR, mag_id VARCHAR,"
        + "who_covidence_id VARCHAR, arxiv_id VARCHAR, pdf_json_files VARCHAR, pmc_json_files VARCHAR, url VARCHAR, s2_id VARCHAR)")
    }

    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "covid-dataset")
      .option("failOnDataLoss", "false")
      .load()

    val query = df
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), Config.DataSchema).alias("data"))
      .select("data.*")
      .writeStream
      .outputMode(OutputMode.Append())
      .format("org.apache.spark.sql.cassandra")
      .option("checkpointLocation", "checkpoint")
      .option("keyspace", "covid")
      .option("table", "dataset")
      .start()

    query.awaitTermination()
  }

  def createElasticIndex(): Unit = {
    println("Create elastic index if not exists...")

    val client = ElasticClient(
      JavaClient(ElasticProperties("http://localhost:9200"))
    )

    client.execute {
      createIndex("covid").mapping(
        properties(
          TextField("cord_uidsha"),
          TextField("source_x"),
          TextField("title"),
          TextField("doi"),
          TextField("pmcid"),
          TextField("pubmed_id"),
          TextField("license"),
          TextField("abstract"),
          TextField("publish_time"),
          TextField("authors"),
          TextField("journal"),
          TextField("mag_id"),
          TextField("who_covidence_id"),
          TextField("arxiv_id"),
          TextField("pdf_json_files"),
          TextField("pmc_json_files"),
          TextField("url"),
          TextField("s2_id")
        )
      )
    }.await

    client.close()
  }

  def writeToElastic(): Unit = {
    createElasticIndex()
    println("Starting Spark session...")

    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.es.nodes", "localhost")
      .set("spark.es.port", "9200")
      .set("spark.es.nodes.wan.only", "true")
      .setAppName("Exo Archi React Elastic Consumer");

    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("ALL")

    println("Starting DStream kafka reading on topic: covid-dataset...")

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "covid-dataset")
      .load()

    println("Creating query to save to ElasticSearch...")

    val query = df
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), Config.DataSchema).alias("data"))
      .select("data.*")
      .writeStream
      .outputMode(OutputMode.Append())
      .format("org.elasticsearch.spark.sql")
      .option("checkpointLocation", "covidCheckpoint")
      .option("es.resource", "covid")
      .start()

    println("Waiting termination...")

    query.awaitTermination()

  }

}