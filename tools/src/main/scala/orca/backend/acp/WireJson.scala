package orca.backend.acp

import orca.json.RawJson

import com.github.plokhotnyuk.jsoniter_scala.core.*

import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker

import scala.util.control.NonFatal

private[acp] object WireJson:
  given JsonValueCodec[String] = JsonCodecMaker.make
  given JsonValueCodec[Long] = JsonCodecMaker.make
  given JsonValueCodec[Int] = JsonCodecMaker.make
  given JsonValueCodec[Map[String, RawJson]] = JsonCodecMaker.make

  given [A](using element: JsonValueCodec[A]): JsonValueCodec[Vector[A]] with
    def nullValue: Vector[A] = Vector.empty
    def encodeValue(values: Vector[A], out: JsonWriter): Unit =
      out.writeArrayStart()
      values.foreach(element.encodeValue(_, out))
      out.writeArrayEnd()
    def decodeValue(in: JsonReader, default: Vector[A]): Vector[A] =
      if !in.isNextToken('[') then in.decodeError("Expected array")
      val values = Vector.newBuilder[A]
      if !in.isNextToken(']') then
        in.rollbackToken()
        var more = true
        while more do
          val _ = values += element.decodeValue(in, element.nullValue)
          if in.isNextToken(']') then more = false
          else if !in.isCurrentToken(',') then
            in.decodeError("Expected ',' or ']'")
      values.result()

  // RawJson skips subtrees without validating their strings or primitive values.
  def validate(raw: RawJson): Either[AcpProtocolException, Unit] =
    read[Unit](raw)(using syntaxCodec)

  private val syntaxCodec: JsonValueCodec[Unit] = new JsonValueCodec[Unit]:
    def nullValue: Unit = ()
    def encodeValue(value: Unit, out: JsonWriter): Unit = out.writeNull()
    def decodeValue(in: JsonReader, default: Unit): Unit = value(in, 0)

    private def value(in: JsonReader, depth: Int): Unit =
      if depth > 128 then in.decodeError("ACP JSON nesting exceeds 128 levels")
      in.nextToken() match
        case '{' =>
          if !in.isNextToken('}') then
            in.rollbackToken()
            var more = true
            while more do
              val _ = in.readKeyAsString()
              value(in, depth + 1)
              if in.isNextToken('}') then more = false
              else if !in.isCurrentToken(',') then
                in.decodeError("Expected ',' or '}'")
        case '[' =>
          if !in.isNextToken(']') then
            in.rollbackToken()
            var more = true
            while more do
              value(in, depth + 1)
              if in.isNextToken(']') then more = false
              else if !in.isCurrentToken(',') then
                in.decodeError("Expected ',' or ']'")
        case '"' =>
          in.rollbackToken()
          val _ = in.readString(null)
        case 't' | 'f' =>
          in.rollbackToken()
          val _ = in.readBoolean()
        case 'n' => in.readNullOrError((), "Expected null")
        case b if b == '-' || (b >= '0' && b <= '9') =>
          in.rollbackToken()
          val _ = in.readDouble()
        case _ => in.decodeError("Expected JSON value")
  // Union codecs parse nested raw values; reentrant calls keep their buffers separate.
  def read[A: JsonValueCodec](raw: RawJson): Either[AcpProtocolException, A] =
    try Right(readFromStringReentrant[A](raw.value))
    catch
      case NonFatal(error) =>
        Left(new AcpProtocolException(s"Invalid ACP JSON: ${error.getMessage}"))

  def readObject[A: JsonValueCodec](
      raw: RawJson
  ): Either[AcpProtocolException, A] =
    try
      val _ = fields(raw)
      read[A](raw)
    catch
      case NonFatal(error) =>
        Left(
          new AcpProtocolException(s"Invalid ACP object: ${error.getMessage}")
        )

  def write[A: JsonValueCodec](value: A): RawJson = RawJson(
    writeToStringReentrant(value)
  )

  def required[A: JsonValueCodec](
      fields: Map[String, RawJson],
      name: String
  ): A =
    val raw = fields.getOrElse(
      name,
      throw new IllegalArgumentException(s"Missing $name")
    )
    require(raw.value.trim != "null", s"Null $name")
    readFromStringReentrant[A](raw.value)

  def codec[A](decode: RawJson => A, encode: A => RawJson): JsonValueCodec[A] =
    new JsonValueCodec[A]:
      def nullValue: A = null.asInstanceOf[A]
      def decodeValue(in: JsonReader, default: A): A =
        decode(summon[JsonValueCodec[RawJson]].decodeValue(in, RawJson("null")))
      def encodeValue(value: A, out: JsonWriter): Unit =
        summon[JsonValueCodec[RawJson]].encodeValue(encode(value), out)

  def stringCodec[A](
      decode: String => A,
      encode: A => String
  ): JsonValueCodec[A] =
    codec(
      raw =>
        val value = readFromStringReentrant[String](raw.value)
        require(value != null, "Expected string")
        decode(value)
      ,
      value => write(encode(value))
    )

  /** Reject duplicate keys instead of silently choosing one envelope's id or
    * method.
    */
  def fields(raw: RawJson): Map[String, RawJson] =
    readFromStringReentrant[Fields](raw.value).values

  private case class Fields(values: Map[String, RawJson])
  private object Fields:
    given JsonValueCodec[Fields] with
      def nullValue: Fields = Fields(Map.empty)
      def encodeValue(value: Fields, out: JsonWriter): Unit =
        out.writeRawVal(
          writeToStringReentrant(value.values).getBytes(
            java.nio.charset.StandardCharsets.UTF_8
          )
        )
      def decodeValue(in: JsonReader, default: Fields): Fields =
        if !in.isNextToken('{') then in.decodeError("Expected object")
        var values = Map.empty[String, RawJson]
        if !in.isNextToken('}') then
          in.rollbackToken()
          var more = true
          while more do
            val key = in.readKeyAsString()
            if values.contains(key) then in.decodeError(s"Duplicate key: $key")
            val value =
              summon[JsonValueCodec[RawJson]].decodeValue(in, RawJson("null"))
            values = values.updated(key, value)
            if in.isNextToken('}') then more = false
            else if !in.isCurrentToken(',') then
              in.decodeError("Expected ',' or '}'")
        Fields(values)
