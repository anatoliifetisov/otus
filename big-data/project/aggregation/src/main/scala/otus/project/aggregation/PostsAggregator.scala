package otus.project.aggregation

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import otus.project.common.Dto.{SentimentRow, SummaryRow}
import otus.project.common.KafkaSink
import otus.project.common.constants._
import otus.project.common.implicits._
import otus.project.configuration.{Hive, Kafka, MonitoringSettings}
import spray.json.DefaultJsonProtocol._
import spray.json._

object PostsAggregator {

  def main(args: Array[String]): Unit = {

    val sparkConfiguration = new SparkConf()
      .setAppName("PostsAggregator")
      .setMaster(sys.env.getOrElse("spark.master", "local[*]"))
      .set("hive.metastore.uris", Hive.metastoreUri)

    val spark = SparkSession
      .builder()
      .config(sparkConfiguration)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sqlContext.implicits._

    val tagsToWatch = spark.sparkContext.broadcast(MonitoringSettings.getTagsToWatch(spark).toSet)
    val kafkaSink = spark.sparkContext.broadcast(KafkaSink[String](Kafka.producerConfig))

    // first of all, let's update summary using previous runs' data
    spark.sql("SELECT * FROM ods.posts").as[SentimentRow]
      // settings could've changed since last run
      .filter(x => tagsToWatch.value.contains(x.tag))
      .map(x => (x.tag, sentimentRow2summaryRow(x)))
      // not pretty, but necessary to reduce without materializing entire groups on workers
      .rdd
      // merging summaries by tag
      .reduceByKey(_ + _)
      // selecting summaries only
      .map(x => x._2)
      .toDS
      .write
      .mode(SaveMode.Overwrite)
      .insertInto("ads.summary")


    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(30))

    val updates = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](List(ANALYZED_POSTS_UPDATE), Kafka.getConsumerConfig("aggregator")))

    updates
      .map(x => x.value.parseJson.convertTo[SummaryRow])
      .foreachRDD(rdd => {
        if (!rdd.partitions.isEmpty) {

          val oldSummary = spark.sql(s"SELECT * FROM ads.summary")
            .as[SummaryRow]
            .rdd

          // retrieving tags for every update to be up to date with settings
          val tagsToWatch = spark.sparkContext.broadcast(
            spark.sql("SELECT * FROM settings")
              .as[String]
              .distinct()
              .collect()
              .toSet
          )

          val updatedSummary =
          // concatenating summaries from Hive and from Kafka
            (oldSummary ++ rdd)
              .filter(x => tagsToWatch.value.contains(x.tag))
              .map(x => (x.tag, x))
              .reduceByKey(_ + _)
              .map(x => x._2)

          updatedSummary
            .toDS.as[SummaryRow]
            .write
            .mode(SaveMode.Overwrite)
            .insertInto("ads.summary")

          kafkaSink.value.send(SUMMARY_UPDATE, updatedSummary.collect().toJson.toString)
        }
      })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
