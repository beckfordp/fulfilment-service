package fulfilmentservice.domain

import cats.syntax.all._
import sttp.tapir.{Codec, CodecFormat, DecodeResult, Schema}

import java.util.UUID

/** Opaque order identifier (order-service's own id), parsed once at the HTTP or
  * Kafka boundary (parse, don't validate - scala-fp.md law 5).
  */
opaque type OrderId = UUID

object OrderId {

  def parse(raw: String): Either[String, OrderId] =
    Either
      .catchOnly[IllegalArgumentException](UUID.fromString(raw))
      .leftMap(_ => s"Invalid order id: '$raw'")

  def fromUuid(value: UUID): OrderId = value

  def uuid(value: OrderId): UUID = value

  def render(value: OrderId): String = value.toString

  given Codec[String, OrderId, CodecFormat.TextPlain] =
    Codec.string.mapDecode(raw =>
      parse(raw) match {
        case Right(id) => DecodeResult.Value(id)
        case Left(msg) =>
          DecodeResult.Error(raw, new IllegalArgumentException(msg))
      }
    )(render)

  given Schema[OrderId] = Schema.string[OrderId]
}
