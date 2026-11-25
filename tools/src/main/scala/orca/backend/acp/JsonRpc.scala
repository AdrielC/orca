package orca.backend.acp

import orca.json.RawJson

import com.github.plokhotnyuk.jsoniter_scala.core.writeToStringReentrant

/** One JSON-RPC 2.0 object per line; batches are not ACP stdio frames. Invalid
  * frames fail decoding rather than being mistaken for notifications.
  */
private[backend] object JsonRpc:
  def decode(line: String): Either[AcpProtocolException, RpcMessage] =
    WireJson.read[RpcMessage](RawJson(line))

  def encode(message: RpcMessage): String = writeToStringReentrant(message)
