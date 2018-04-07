package otus.project.common

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import collection.JavaConversions._

// not pretty, but efficient hack to make kafka sinks broadcastable
object KafkaSink {
  def apply[V](config: Map[String, Object]): KafkaSink[V] = {
    val f = () => {
      val producer = new KafkaProducer[String, V](config)

      sys.addShutdownHook {
        producer.close()
      }
      producer
    }
    new KafkaSink(f)
  }
}

class KafkaSink[V](createProducer: () => KafkaProducer[String, V]) extends Serializable {

  lazy val producer = createProducer()

  def send(topic: String, value: V): Unit = producer.send(new ProducerRecord(topic, value))
}