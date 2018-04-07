package otus.project.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import otus.project.common.Dto.PostsRow
import otus.project.common.KafkaSink
import otus.project.common.constants._
import otus.project.common.implicits._
import otus.project.configuration.{Hive, Kafka, MonitoringSettings, Twitter}
import spray.json.DefaultJsonProtocol._
import spray.json._
import twitter4j.GeoLocation

object TwitterStreamer {

  implicit class tags2Words(val s: String) extends AnyVal {
    // converts camelCased hashtags in text to words
    def toWords: String = {
      val regex = "#([A-Za-z]+)".r

      // a list of hashtags and their substitutions
      val replacements = regex.findAllIn(s)
        .map(x => (x, x.stripPrefix("#")))
        .map(x => (x._1, if (x._2.forall(_.isUpper))
        // no point in splitting abbreviations
          x._2.toLowerCase
        else
          x._2.replaceAll("([A-Z])", " $1").toLowerCase.trim))
        .toList


      def _replace(s: String, replacements: Seq[(String, String)]): String = replacements match {
        case Nil => s
        case head :: xs => _replace(s.replace(head._1, head._2), xs)
      }

      _replace(s, replacements)
    }
  }

  def main(args: Array[String]): Unit = {

    val sparkConfiguration = new SparkConf()
      .setAppName("TwitterStreamer")
      .setMaster(sys.env.getOrElse("spark.master", "local[*]"))
      .set("hive.metastore.uris", Hive.metastoreUri)

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .config(sparkConfiguration)
      .getOrCreate()

    import spark.sqlContext.implicits._

    var streamingContext = Option.empty[StreamingContext]

    val processThread = new Thread {
      override def run(): Unit = {

        val kafkaSink = spark.sparkContext.broadcast(KafkaSink[String](Kafka.producerConfig))

        var shouldReload = false
        var previousConfig = Set[String]()

        while (true) {
          val tagsToWatch = MonitoringSettings.getTagsToWatch(spark).toSet
          if (previousConfig.nonEmpty && !previousConfig.equals(tagsToWatch))
            shouldReload = true

          if (streamingContext.isEmpty) {

            streamingContext = Option(new StreamingContext(spark.sparkContext, Seconds(1)))

            TwitterUtils.createStream(streamingContext.get, Twitter.auth, tagsToWatch.map(x => s"#$x").toSeq)
              // sentiment analysis for other languages isn't as advanced as for English
              .filter(_.getLang == "en")
              // mapping to pairs of (set of relevant tags, Status)
              .map(x => (x.getHashtagEntities.map(_.getText).toSet & tagsToWatch, x))
              // dropping statuses with no relevant tags
              .filter(x => x._1.nonEmpty)
              // mapping (Set[String], Status) to Set[(Status, String)]
              .map(x => x._1.map(y => (x._2, y)))
              // flattening into (Status, String)
              .flatMap(x => x)
              .map(x => {

                // sometimes a retweet gets truncated and an original tweet stays intact
                val text = Option(x._1.getRetweetedStatus)
                  .map(x => x.getText).getOrElse(x._1.getText)
                  .toWords
                  // removing @mentions, newlines and links
                  .replaceAll("""@[A-Za-z0-9]+|\n|(https|http)?:\/\/(\w|\.|\/|\?|\=|\&|\%)*\b""", " ")
                  // removing extra whitespaces
                  .replaceAll("""\s+""", " ")
                  .trim

                val lat = extractLocation(x._1, loc => loc.getLatitude)
                val lng = extractLocation(x._1, loc => loc.getLongitude)

                PostsRow(x._1.getId, text, lat, lng, x._2)
              })
              .foreachRDD(rdd =>
                rdd.foreachPartition(part =>
                  part.foreach(x => kafkaSink.value.send(NEW_POSTS, x.toJson.toString))))

            // watching for settings updates
            KafkaUtils.createDirectStream[String, String](
              streamingContext.get,
              PreferConsistent,
              Subscribe[String, String](List(SETTINGS_UPDATE), Kafka.getConsumerConfig("twitterStreamer")))
              .foreachRDD(rdd => rdd.map(x => x.value).distinct.collect.toSeq match {
                case Nil => ()
                case _ => rdd.map(x => x.value.parseJson.convertTo[Seq[String]])
                  .flatMap(x => x)
                  .distinct()
                  .toDS
                  .write
                  .mode(SaveMode.Overwrite)
                  .insertInto("default.settings")
              })

            streamingContext.get.start()
            previousConfig = tagsToWatch
          }
          else if (shouldReload) {

            streamingContext.get.stop(stopSparkContext = false, stopGracefully = true)
            streamingContext.get.awaitTermination()
            streamingContext = Option.empty[StreamingContext]
            shouldReload = false
            previousConfig = tagsToWatch
          }
          else {
            Thread.sleep(1000)
          }
        }

        streamingContext.get.stop(stopSparkContext = true, stopGracefully = true)
        streamingContext.get.awaitTermination()
      }
    }

    processThread.start()
    processThread.join()
  }

  // extracts location from a twitter status
  private def extractLocation(status: twitter4j.Status, extractor: GeoLocation => Double): Option[Double] = {
    Option(status.getGeoLocation) match {
      // location is defined, we're good to go
      case Some(location) => Some(extractor(location))
      // location is not defined, let's check is there's a place defined
      case None => Option(status.getPlace) match {
        // place is here, does it have any boundaries set?
        case Some(place) => Option(place.getBoundingBoxCoordinates) match {
          // let's take a top left corner, it's still better than nothing
          case Some(box) => Option(extractor(box(0)(0)))
          // no boundaries, we can't do anything
          case None => None
        }
        // no place set either
        case None => None
      }
    }
  }
}
