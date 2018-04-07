package otus.project.analysis

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import otus.project.common.Dto.{PostsRow, SentimentRow, SummaryRow}
import otus.project.common.KafkaSink
import otus.project.common.constants._
import otus.project.common.implicits._
import otus.project.configuration.{Hive, Kafka}
import spray.json._

import scala.collection.JavaConversions._

object PostsAnalyzer {

  def findSentiment(line: String): Int = {
    // translated from Java, ergo ugly

    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
    val pipeline = new StanfordCoreNLP(props)
    var mainSentiment = 0
    if (line != null && line.length > 0) {
      var longest = 0
      val annotation = pipeline.process(line)

      for (sentence <- annotation.get(classOf[CoreAnnotations.SentencesAnnotation])) {
        val tree = sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
        val sentiment = RNNCoreAnnotations.getPredictedClass(tree)
        val partText = sentence.toString
        if (partText.length > longest) {
          mainSentiment = sentiment
          longest = partText.length
        }
      }
    }
    mainSentiment
  }

  def main(args: Array[String]): Unit = {

    val sparkConfiguration = new SparkConf()
      .setAppName("PostsAnalyzer")
      .setMaster(sys.env.getOrElse("spark.master", "local[*]"))
      .set("hive.metastore.uris", Hive.metastoreUri)

    val spark = SparkSession
      .builder()
      .config(sparkConfiguration)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sqlContext.implicits._

    val kafkaSink = spark.sparkContext.broadcast(KafkaSink[String](Kafka.producerConfig))

    val sql = "SET hive.exec.dynamic.partition.mode=nonstrict;"
    spark.sql(sql)

    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(10))

    val newTweets = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](List(NEW_POSTS), Kafka.getConsumerConfig("analyzer")))

    newTweets
      .map(x => x.value.parseJson.convertTo[PostsRow])
      .map(x => SentimentRow(x.id, x.text, x.lat, x.lng, findSentiment(x.text), x.tag))
      .foreachRDD(rdd => {
        if (!rdd.partitions.isEmpty) {
          rdd.toDS.as[SentimentRow]
            .write
            .mode(SaveMode.Append)
            .insertInto("ods.posts")

          rdd
            // a little help for compiler to convert SentimentRow to SummaryRow implicitly
            .map[(String, SummaryRow)](x => (x.tag, x))
            // merging updates
            .reduceByKey(_ + _)
            .map(x => x._2)
            .foreach(x => kafkaSink.value.send(ANALYZED_POSTS_UPDATE, x.toJson.toString))
        }
      })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
