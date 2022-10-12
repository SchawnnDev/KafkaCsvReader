

import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

import java.util.Properties


case class CovidRow(cord_uidsha: String, source_x: String, title: String, doi: String, pmcid: String, pubmed_id: String,
                    license: String, abstract_x: String, publish_time: String, authors: String, journal: String,
                    mag_id: String, who_covidence_id: String, arxiv_id: String, pdf_json_files: String,
                    pmc_json_files: String, url: String, s2_id: String)

object Config {

  def genProperties(serialization: Boolean = true): Properties = {
    val props = new Properties()
    import org.apache.kafka.clients.producer.ProducerConfig
    // For docker launching: props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092")
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.ACKS_CONFIG, "all")

    if (serialization) {
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    } else {
      import org.apache.kafka.clients.consumer.ConsumerConfig
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "simple-consumer")
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    }
    props
  }

  lazy val DataSchema: StructType = StructType(
    List(
      StructField("cord_uidsha", StringType, nullable = true),
      StructField("source_x", StringType, nullable = true),
      StructField("title", StringType, nullable = true),
      StructField("doi", StringType, nullable = true),
      StructField("pmcid", StringType, nullable = true),
      StructField("pubmed_id", StringType, nullable = true),
      StructField("license", StringType, nullable = true),
      StructField("abstract", StringType, nullable = true),
      StructField("publish_time", StringType, nullable = true),
      StructField("authors", StringType, nullable = true),
      StructField("journal", StringType, nullable = true),
      StructField("mag_id", StringType, nullable = true),
      StructField("who_covidence_id", StringType, nullable = true),
      StructField("arxiv_id", StringType, nullable = true),
      StructField("pdf_json_files", StringType, nullable = true),
      StructField("pmc_json_files", StringType, nullable = true),
      StructField("url", StringType, nullable = true),
      StructField("s2_id", StringType, nullable = true)
    )
  )

}