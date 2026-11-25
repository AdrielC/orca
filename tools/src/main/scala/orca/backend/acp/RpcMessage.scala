package orca.backend.acp

import orca.json.RawJson

import com.github.plokhotnyuk.jsoniter_scala.core.*

import WireJson.given

private[backend] enum RpcMessage:
  case Request(id: RequestId, method: AcpMethod, params: Option[RawJson])
  case Notification(method: AcpMethod, params: Option[RawJson])
  case Response(id: RequestId, result: Either[RpcError, RawJson])

private[backend] object RpcMessage:
  given JsonValueCodec[RpcMessage] =
    WireJson.codec(decodeMessage, encodeMessage)

  private def decodeMessage(raw: RawJson): RpcMessage =
    WireJson.validate(raw).fold(throw _, identity)
    val fields = WireJson.fields(raw)
    require(
      WireJson.required[String](fields, "jsonrpc") == "2.0",
      "Expected JSON-RPC 2.0"
    )
    fields.get("method") match
      case Some(_) =>
        require(
          !fields.contains("result") && !fields.contains("error"),
          "Mixed request and response"
        )
        val method =
          AcpMethod.parse(WireJson.required[String](fields, "method"))
        val params = fields.get("params")
        params.foreach: p =>
          require(
            p.value.trim.startsWith("{") || p.value.trim.startsWith("["),
            "Invalid params"
          )
        fields.get("id") match
          case Some(id) =>
            RpcMessage.Request(
              readFromStringReentrant[RequestId](id.value),
              method,
              params
            )
          case None => RpcMessage.Notification(method, params)
      case None =>
        require(!fields.contains("params"), "Response contains params")
        val id = readFromStringReentrant[RequestId](
          fields
            .getOrElse("id", throw new IllegalArgumentException("Missing id"))
            .value
        )
        (fields.get("result"), fields.get("error")) match
          case (Some(result), None) => RpcMessage.Response(id, Right(result))
          case (None, Some(error)) =>
            val errorFields = WireJson.fields(error)
            val _ = WireJson.required[String](errorFields, "message")
            RpcMessage.Response(
              id,
              Left(readFromStringReentrant[RpcError](error.value))
            )
          case _ =>
            throw new IllegalArgumentException(
              "Response needs exactly one result or error"
            )

  private def encodeMessage(message: RpcMessage): RawJson =
    val base = Map("jsonrpc" -> WireJson.write("2.0"))
    val fields = message match
      case RpcMessage.Request(id, method, params) =>
        base ++ Map(
          "id" -> WireJson.write(id),
          "method" -> WireJson.write(method.wire)
        ) ++
          params.map("params" -> _)
      case RpcMessage.Notification(method, params) =>
        base ++ Map("method" -> WireJson.write(method.wire)) ++ params.map(
          "params" -> _
        )
      case RpcMessage.Response(id, result) =>
        base ++ Map("id" -> WireJson.write(id)) ++ (result match
          case Right(value) => Map("result" -> value)
          case Left(error)  => Map("error" -> WireJson.write(error))
        )
    WireJson.write(fields)
