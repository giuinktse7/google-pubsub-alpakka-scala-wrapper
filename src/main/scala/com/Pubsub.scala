package com

import java.nio.charset.StandardCharsets
import java.util.Base64

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.googlecloud.pubsub._
import akka.stream.alpakka.googlecloud.pubsub.scaladsl.GooglePubSub
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.google.cloud.pubsub.v1.SubscriptionAdminClient
import com.google.pubsub.v1.{ListSubscriptionsRequest, ProjectName, PushConfig}

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.Try
import scala.concurrent.duration._
import spray.json._
import spray.json.DefaultJsonProtocol._

import scala.collection.JavaConverters._

/**
  * API that simplifies the usage of alpakka's Google PubSub bindings for scala.
  * @see https://github.com/akka/alpakka/blob/master/docs/src/main/paradox/google-cloud-pub-sub.md
  */
object Pubsub {
  var pubsubConfig: Option[PubSubConfig] = None

  /**
    * Convenience method for publish(PubSubMessage, topic).
    */
  def publish(message: String, topic: String)(implicit system: ActorSystem, mat: ActorMaterializer): Future[Seq[Seq[String]]] = {
    val id = util.Random.alphanumeric.take(15).mkString
    publish(PubSubMessage(message, id), topic)
  }

  def updateSubscription(topic: String, subscription: String)(implicit system: ActorSystem): Unit = {
    withConfig { cfg =>
      try {
        val client = SubscriptionAdminClient.create()
        val req =
          ListSubscriptionsRequest
            .newBuilder()
            .setProject(ProjectName.of(cfg.projectId).toString)
            .build()

          val subscriptionExists = client
            .listSubscriptions(req)
            .iterateAll()
            .asScala
            .exists(_.getName.split("/").last == subscription)

        if (!subscriptionExists) {
          println(s"Updating subscription $subscription on topic $topic.")
          client.createSubscription(
            s"projects/${cfg.projectId}/subscriptions/$subscription",
            s"projects/${cfg.projectId}/topics/$topic",
            PushConfig.newBuilder().build(),
            10)
        }
      }
    }
  }

  def publishFromSource[Mat](source: Source[PubSubMessage, Mat], topic: String)(implicit system: ActorSystem, mat: ActorMaterializer) = {
    withConfig { cfg =>
      val publishFlow = GooglePubSub.publish(topic, cfg)
      source.map(encodeMessage).groupedWithin(10, 1.seconds).map(PublishRequest.apply).via(publishFlow).runWith(Sink.seq)
    }
  }

  private def encodeMessage(message: PubSubMessage): PubSubMessage = {
    val encodedData = new String(Base64.getEncoder.encode(message.data.getBytes()), StandardCharsets.UTF_8)
    message.copy(data = encodedData)
  }

  def publish(message: PubSubMessage, topic: String)(implicit system: ActorSystem, mat: ActorMaterializer): Future[Seq[Seq[String]]] = {
    val request = PublishRequest(immutable.Seq(encodeMessage(message)))

    val source: Source[PublishRequest, NotUsed] = Source.single(request)

    withConfig { cfg =>
      val publishFlow = GooglePubSub.publish(topic, cfg)
      source.via(publishFlow).runWith(Sink.seq)
    }
  }

  def subscribeStream(subscription: String)(implicit system: ActorSystem, mat: ActorMaterializer): Source[DecodedReceivedMessage, NotUsed] = {
    withConfig { config =>
      val ackSink: Sink[AcknowledgeRequest, Future[Done]] = GooglePubSub.acknowledge(subscription, config)

      val subscribeMessageSource: Source[ReceivedMessage, NotUsed] = GooglePubSub.subscribe(subscription, config)

      val batchAckSink =
        Flow[ReceivedMessage].map(_.ackId).groupedWithin(10, 1.seconds).map(AcknowledgeRequest.apply).to(ackSink)

      subscribeMessageSource.alsoTo(batchAckSink).map(new DecodedReceivedMessage(_))
    }
  }

  def subscribe[A](subscription: String)(onReceiveMessage: DecodedReceivedMessage => Unit)(implicit system: ActorSystem, mat: ActorMaterializer) = {
    subscribeStream(subscription).runForeach(onReceiveMessage)
  }

  def withConfig[A](f: PubSubConfig => A)(implicit system: ActorSystem): A = {
    // newConfig

    pubsubConfig.map(f) match {
      case Some(res) => println("Used config."); res
      case None =>
        loadCredentials()
        f(pubsubConfig.get)
    }
  }

  class DecodedReceivedMessage(private val receivedMessage: ReceivedMessage) {
    val ackId = receivedMessage.ackId
    val message = {
      val pubsubMessage = receivedMessage.message
      val decodedData = new String(Base64.getDecoder.decode(pubsubMessage.data), StandardCharsets.UTF_8)
      pubsubMessage.copy(data = decodedData)
    }
  }

  /**
    * Load the Google-PubSub credential file. Should only be called once upon startup.
    */
  def loadCredentials()(implicit system: ActorSystem): Unit = {
    import scala.io.Source

    val credentialsFile = System.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    val credentialsData = Try(Source.fromFile(credentialsFile).mkString.parseJson.asJsObject().fields).toEither.left.map(_.getMessage)

    def getCredentialFor(credentials: Map[String, JsValue])(key: String) = {
      credentials
        .get(key)
        .map(_.convertTo[String].filterNot(c => c == '\"' ))
        .toRight(s"Could not find JSON key '$key' in credential file at '$credentialsFile.'")
    }

    val loadedConfig = for {
      getCredential <- credentialsData.map(getCredentialFor)
      privateKey <- getCredential("private_key")
      clientEmail <- getCredential("client_email")
      projectId <- getCredential("project_id")
    } yield PubSubConfig(projectId, clientEmail, privateKey)

    pubsubConfig = loadedConfig.toOption

    loadedConfig match {
      case Left(err) => throw new CredentialException(err)
      case _ => ()
    }
  }

  class CredentialException(message: String) extends Exception(message) {
    def this(message: String, cause: Throwable) {
      this(message)
      initCause(cause)
    }

    def this(cause: Throwable) {
      this(Option(cause).map(_.toString).orNull, cause)
    }

    def this() {
      this(null: String)
    }
  }

  class PubsubConfigException(message: String) extends Exception(message) {
    def this(message: String, cause: Throwable) {
      this(message)
      initCause(cause)
    }

    def this(cause: Throwable) {
      this(Option(cause).map(_.toString).orNull, cause)
    }

    def this() {
      this(null: String)
    }
  }
}