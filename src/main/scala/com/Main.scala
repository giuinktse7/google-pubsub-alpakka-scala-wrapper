package com


import java.util.Calendar

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.googlecloud.pubsub.PubSubMessage
import akka.stream.scaladsl.Source

import scala.concurrent.duration._


object Main extends App {
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()

  println(System.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

  val topic = "elvis-events"
  val subscription = "dev"

  def currentTime() = Calendar.getInstance().getTime.toString

  val timeSource = Source.tick(1.second, 1.second, PubSubMessage).map(_ => PubSubMessage(
    data = currentTime(),
    messageId = "1"
  ))

  Pubsub.updateSubscription(topic, subscription)


  Pubsub.publishFromSource(timeSource, topic)


  Pubsub.subscribe(subscription) { message =>
    println(s"Message: ${message.message.data}\nid: ${message.message.messageId}\n\n")
  }

  println("Running..")
}