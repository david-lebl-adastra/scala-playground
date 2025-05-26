package lebldavi.y25.persistent_events

import zio._
import zio.prelude.Newtype
import zio.stm.{ STM, TMap, TQueue }
import zio.stream._

import java.util.UUID

import io.circe._
//import io.circe.generic.extras.Configuration
import io.circe.cursor._
import io.circe.syntax._
import scala.util.control.NoStackTrace

object types           {
  type EventType = EventType.Type
  object EventType extends Newtype[String]
}
import types._

case class MessageEnvelope[+A] private (
    payload: A,
    eventType: EventType,
    metadata: MessageEnvelope.Metadata = MessageEnvelope.Metadata(),
  )
object MessageEnvelope {
  case class Metadata(eventId: UUID = UUID.randomUUID(), version: Int = 1)

  implicit val metadataCodec: Codec[Metadata] = deriveCodec
}

// type class
trait EventTypeInfo[A] {
  def eventType: EventType
  def version: Int
}

object EventTypeInfo     {
  def apply[A: EventTypeInfo]: EventTypeInfo[A] = implicitly
}

case class SerializedMessage(
    metadata: MessageEnvelope.Metadata,
    eventType: String,
    payload: Json,
  )
object SerializedMessage {
  implicit val seCodec: Codec[SerializedMessage] = deriveCodec
}

object UserApp {
  sealed trait Command
  case class CreateUser(name: String) extends Command
  case class GetUser()                extends Command

  implicit val cmdEventType: EventTypeInfo[Command]         = new EventTypeInfo[Command] {
    val eventType: EventType = EventType("user.command")
    val version: Int         = 1
  }
  implicit val cmdCreateUserType: EventTypeInfo[CreateUser] = new EventTypeInfo[CreateUser] {
    val eventType: EventType = EventType("user.command")
    val version: Int         = 1
  }

//  import io.circe.generic.extras.semiauto._
  implicit val cmdDevConfig: Configuration = Configuration.default.withDiscriminator("$type")
  implicit val cmdCodec: Codec[Command]    = deriveConfiguredCodec
}

object OrderApp {
  sealed trait Command
  case class PrepareOrder(order: String) extends Command
  case class ShipOrder()                 extends Command

  implicit val cmdEventType: EventTypeInfo[Command]          = new EventTypeInfo[Command] {
    val eventType: EventType = EventType("order.command")
    val version: Int         = 1
  }
  implicit val cmdPrepOrderType: EventTypeInfo[PrepareOrder] = new EventTypeInfo[PrepareOrder] {
    val eventType: EventType = EventType("order.command")
    val version: Int         = 1
  }

//  import io.circe.generic.extras.semiauto._
  implicit val cmdDevConfig: Configuration = Configuration.default.withDiscriminator("$type")
  implicit val cmdCodec: Codec[Command]    = deriveConfiguredCodec
}

class InMemoryEventStorage private (queue: TQueue[String]) {
  def push(message: SerializedMessage): UIO[Unit] =
    ZIO.log(s"persisting event: ${message.eventType}") *>
      queue.offer(message.asJson.noSpaces).commit.unit

  def take: Task[SerializedMessage] = takeWhen(_ => true).someOrFailException

  def takeWhen(when: SerializedMessage => Boolean): Task[Option[SerializedMessage]] =
    queue
      .take
      .map(decode[SerializedMessage](_))
      .flatMap(STM.fromEither(_))
      .asSomeError
      .filterOrFail(when)(None)
      .commit // event will not be taken from the queue if the commit fails, STM is amazing <3
      .unsome
      .tap(a => ZIO.log(s"retrieving event: ${a.map(_.eventType)}"))

  // fixme - no filter
  def stream: ZStream[Any, Nothing, SerializedMessage] =
    ZStream.fromTQueue(queue).map(decode[SerializedMessage](_).toOption).collectSome
}
object InMemoryEventStorage                                {
  def make: UIO[InMemoryEventStorage] =
    TQueue.unbounded[String].commit.map(new InMemoryEventStorage(_))
}

trait MessageHandler[-A] {
  def handle(message: MessageEnvelope[A]): Task[Unit]
}

trait SMessageDecoder[A] {
  def decode(a: SerializedMessage): Either[SMessageDecoder.Error, MessageEnvelope[A]]
  def messageType: EventType
  def eventTypeInfo: EventTypeInfo[A] // fixme - remove
}
object SMessageDecoder   {
  def apply[A: SMessageDecoder]: SMessageDecoder[A] = implicitly

  sealed trait Error                                             extends NoStackTrace with Product with Serializable
  case class Mismatch[+A](msg: A, expected: String, got: String) extends Error {
    override def toString: String = s"Event type mismatch. Expected: $expected, got: $got"
  }
  case class WrongDecoding(reason: String)                       extends Error

  implicit def createMessageDecoder[A: EventTypeInfo: Decoder]: SMessageDecoder[A] = new SMessageDecoder[A] {
    override def decode(message: SerializedMessage): Either[Error, MessageEnvelope[A]] = {
      val targetEventType = EventTypeInfo[A].eventType
      if (message.eventType == EventType.unwrap(targetEventType))
        message
          .payload
          .as[A]
          .map { event =>
            MessageEnvelope[A](
              metadata = message.metadata,
              eventType = EventType(message.eventType),
              payload = event,
            )
          }
          .left
          .map(e => WrongDecoding(e.message))
      else Left(Mismatch(message, EventType.unwrap(targetEventType), message.eventType))
    }

    override def messageType: EventType = EventTypeInfo[A].eventType

    override def eventTypeInfo: EventTypeInfo[A] = EventTypeInfo[A]
  }
}

object SerializedMessageCodec {
  implicit class EventToEnvelopeOps[A: EventTypeInfo](a: A) {
    def toEnvelope: MessageEnvelope[A] = MessageEnvelope(
      payload = a,
      eventType = EventTypeInfo[A].eventType,
    )
  }

  implicit class EventToMessageOps[A: EventTypeInfo: Encoder](a: A) {
    def toMessage: SerializedMessage = {
      val event = a.toEnvelope
      SerializedMessage(
        metadata = event.metadata,
        eventType = EventType.unwrap(event.eventType),
        payload = event.payload.asJson,
      )
    }
  }
}

case class SMessageHandler[A](
    handler: MessageHandler[A],
    decoder: SMessageDecoder[A], // fixme - zio schema would be nicer
  )

object SMessageHandler {
  def apply[A: SMessageHandler]: SMessageHandler[A] = implicitly

  implicit def createAnyHandler[A: SMessageDecoder](handler: MessageHandler[A]): SMessageHandler[Any] =
    new SMessageHandler(handler, SMessageDecoder[A]).asInstanceOf[SMessageHandler[Any]]
}

class MessageSubscriber private (
    store: InMemoryEventStorage,
    handlers: TMap[EventType, SMessageHandler[_]],
  ) {

  def registerSMessageHandler[A](
      handler: SMessageHandler[A]
    ): UIO[Unit] = handlers.put(handler.decoder.messageType, handler).commit

  def registerHandlerAndDecoder[A](
      handler: MessageHandler[A],
      decoder: SMessageDecoder[A],
    ): UIO[Unit] = registerSMessageHandler(SMessageHandler(handler, decoder))

  def registerHandler[A: SMessageDecoder](handler: MessageHandler[A]): UIO[Unit] =
    registerSMessageHandler(SMessageHandler(handler, SMessageDecoder[A]))

  def subscribe[A: EventTypeInfo]: Task[Unit] = for {
    sh     <- handlers.get(EventTypeInfo[A].eventType).someOrFailException.commit
    handler = sh.handler.asInstanceOf[MessageHandler[A]]
    decoder = sh.decoder.asInstanceOf[SMessageDecoder[A]]
    // not a gratest way to subscribe if there for multiple subscribers
    _      <- store
                .takeWhen(_.eventType == EventType.unwrap(decoder.messageType))
                .some
                .map(decoder.decode)
                .flatMap(ZIO.fromEither(_).asSomeError)
                .flatMap(handler.handle(_).asSomeError)
                .unsome
                .logError
                .forever
  } yield ()

  def subscribeAll: Task[Unit] = for {
    items <- handlers.toList.commit
    _     <- ZIO.foreachDiscard(items) {
               case (_, SMessageHandler(_, decoder)) =>
                 subscribe(decoder.eventTypeInfo).fork
             }
    _     <- ZIO.never
  } yield ()

  def stream[A: EventTypeInfo]: ZStream[Any, Throwable, MessageEnvelope[A]] = ZStream.fromZIO {
    for {
      SMessageHandler(
        handler,
        decoder,
      ) <- handlers.get(EventTypeInfo[A].eventType).someOrFailException.commit
    } yield store
      .stream
      .map(decoder.asInstanceOf[SMessageDecoder[A]].decode(_).toOption)
      .collectSome
  }.flatten

}

object MessageSubscriber {
  def make(store: InMemoryEventStorage) =
    TMap.empty[EventType, SMessageHandler[_]].commit.map(new MessageSubscriber(store, _))

  def makeWithHandlers(
      store: InMemoryEventStorage,
      handlers: SMessageHandler[Any]*
    ) = make(store).tap { ms =>
    ZIO.foreachDiscard(handlers)(ms.registerSMessageHandler)
  }
}

object Playground extends ZIOAppDefault {
  import SerializedMessageCodec._

  val userHandler = new MessageHandler[UserApp.Command] {
    override def handle(message: MessageEnvelope[UserApp.Command]): Task[Unit] =
      ZIO.log(s"User message handled: $message")
  }

  val orderHandler = new MessageHandler[OrderApp.Command] {
    override def handle(message: MessageEnvelope[OrderApp.Command]): Task[Unit] =
      ZIO.log(s"Order message handled: $message")
  }

  def run = for {
    _     <- ZIO.logInfo("Starting...")
    store <- InMemoryEventStorage.make
    sub   <- MessageSubscriber.makeWithHandlers(
               store,
               // registered handlers immediately
               userHandler,
               orderHandler,
             )
    // send events
    _     <- store.push((UserApp.CreateUser("user1"): UserApp.Command).toMessage)
    _     <- store.push((OrderApp.PrepareOrder("big order"): OrderApp.Command).toMessage)
    // re-registered handlers later
    _     <- sub.registerHandler(userHandler)
    _     <- sub.registerHandler(orderHandler)
    // handle events
    _     <- sub.subscribeAll.fork
    // or handle single event
    _     <- sub.subscribe[UserApp.Command].fork
    _     <- sub.subscribe[OrderApp.Command].fork
    // or as stream
    //    _     <- sub.stream[UserApp.Command].mapZIO(a => ZIO.log(s"Processing stream item: ${a}")).runDrain.fork

    // send another event
    _ <- store.push((UserApp.CreateUser("user2"): UserApp.Command).toMessage)

    _ <- ZIO.sleep(1.second)
    _ <- ZIO.logInfo("Done")
  } yield ()
}
