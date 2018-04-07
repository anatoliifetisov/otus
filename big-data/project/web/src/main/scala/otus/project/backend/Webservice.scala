package otus.project.backend

import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.server.{Directives, Route}
import otus.project.common.KafkaSink
import otus.project.common.constants._
import otus.project.configuration.Kafka

class Webservice extends Directives {

  private val kafkaConsumer = LastValueSavingConsumer("web", SUMMARY_UPDATE)
  kafkaConsumer.run()

  def route: Route =
    get {
      pathSingleSlash {
        getFromResource("index.html")
      } ~
      path("style.css") {
        getFromResource("style.css")
      } ~
      path("script.js") {
        getFromResource("script.js")
      } ~
      path("latestSummary") {
        complete(Option(kafkaConsumer.lastValue))
      }
    } ~
    post {
      path("settings") {
        entity(as[String]) {
          ent => KafkaSink[String](Kafka.producerConfig).send(SETTINGS_UPDATE, ent)
          complete(StatusCode.int2StatusCode(200))
        }
      }
    }
}