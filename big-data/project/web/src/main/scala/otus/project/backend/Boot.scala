package otus.project.backend

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import otus.project.configuration._

import scala.util.{Failure, Success}

object Boot extends App {
  implicit val system: ActorSystem = ActorSystem()
  import system.dispatcher
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val service = new Webservice
  val binding = Http().bindAndHandle(service.route, Akka.address, Akka.port)
  binding.onComplete {
    case Success(b) =>
      val localAddress = b.localAddress
      println(s"Server is listening on ${localAddress.getHostName}:${localAddress.getPort}")
    case Failure(e) =>
      println(s"Binding failed with ${e.getMessage}")
      system.terminate()
  }
}