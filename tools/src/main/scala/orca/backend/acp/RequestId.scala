package orca.backend.acp

import orca.json.RawJson

import com.github.plokhotnyuk.jsoniter_scala.core.*

import WireJson.given

private[backend] enum RequestId:
  case Number(value: Long)
  case Text(value: String)
  case Null

private[backend] object RequestId:
  given JsonValueCodec[RequestId] = WireJson.codec(
    raw =>
      raw.value.trim match
        case "null" => RequestId.Null
        case s if s.startsWith("\"") =>
          RequestId.Text(readFromStringReentrant[String](s))
        case s => RequestId.Number(readFromStringReentrant[Long](s)),
    {
      case RequestId.Number(value) => WireJson.write(value)
      case RequestId.Text(value)   => WireJson.write(value)
      case RequestId.Null          => RawJson("null")
    }
  )
