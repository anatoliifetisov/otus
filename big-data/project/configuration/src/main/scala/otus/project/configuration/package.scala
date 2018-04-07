package otus.project

import org.apache.kafka.clients.producer.ProducerConfig
import twitter4j.TwitterFactory
import twitter4j.auth.Authorization
import twitter4j.conf.ConfigurationBuilder

package object configuration {

  case class Twitter(consumerKey: String, consumerSecret: String, accessToken: String, accessTokenSecret: String)
  case class Hive(address: String, port: Int)
  case class Kafka(address: String, port: Int)
  case class Zookeeper(address: String, port: Int)
  case class Akka(address: String, port: Int)

  case class ProjectConfig(twitter: Twitter, hive: Hive, kafka: Kafka, zookeeper: Zookeeper, akka: Akka)

  private val config: ProjectConfig = pureconfig.loadConfig[ProjectConfig] match {
    case Right(cfg) => cfg
    case Left(cfg) => throw new ExceptionInInitializerError(cfg.head.description)
  }

  object Twitter {
    val auth: Option[Authorization] = Option(
      new TwitterFactory(
        new ConfigurationBuilder()
          .setOAuthConsumerKey(config.twitter.consumerKey)
          .setOAuthConsumerSecret(config.twitter.consumerSecret)
          .setOAuthAccessToken(config.twitter.accessToken)
          .setOAuthAccessTokenSecret(config.twitter.accessTokenSecret)
          .build())
        .getInstance()
        .getAuthorization
    )
  }

  object Hive {
    val metastoreUri: String = s"thrift://${config.hive.address}:${config.hive.port}"
  }

  object Kafka {
    val producerConfig: Map[String, String] = {
      Map(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"${config.kafka.address}:${config.kafka.port}",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer"
      )
    }

    def getConsumerConfig(groupId: String): Map[String, String] = {
      producerConfig + (
        "group.id" -> groupId,
        "zookeeper.connect" -> s"${config.zookeeper.address}:${config.zookeeper.port}",
        "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
      )
    }
  }

  object Akka {
    val address: String = config.akka.address
    val port: Int = config.akka.port
  }

}
