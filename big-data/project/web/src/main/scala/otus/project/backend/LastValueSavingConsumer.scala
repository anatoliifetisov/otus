package otus.project.backend

import java.util.Collections
import java.util.concurrent.Executors

import otus.project.configuration._

import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object LastValueSavingConsumer {

  def apply(groupId: String, topic: String): LastValueSavingConsumer = {
    new LastValueSavingConsumer(Kafka.getConsumerConfig(groupId), topic)
  }
}

class LastValueSavingConsumer(val properties: Map[String, Object], val topic: String) {

  private val consumer = new KafkaConsumer[String, String](properties)

  var lastValue: String = "[]"

  def run(): Unit = {
    consumer.subscribe(Collections.singletonList(this.topic))
    Executors.newSingleThreadExecutor.execute(new Runnable {
      override def run(): Unit = {
        while (true) {
          val records = consumer.poll(1000)
          for (record <- records.asScala) {
            lastValue = record.value
          }
        }
      }
    })
  }
}