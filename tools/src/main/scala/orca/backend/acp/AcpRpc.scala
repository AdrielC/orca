package orca.backend.acp

import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec

/** Outbound JSON-RPC frames for the shared ACP client. */
private[backend] object AcpRpc:
  def request[A: JsonValueCodec](
      id: RequestId,
      method: AcpMethod,
      value: A
  ): String =
    JsonRpc.encode(RpcMessage.Request(id, method, Some(WireJson.write(value))))

  def notification[A: JsonValueCodec](method: AcpMethod, value: A): String =
    JsonRpc.encode(RpcMessage.Notification(method, Some(WireJson.write(value))))

  def encodeResponse(frame: RpcMessage.Response): String = JsonRpc.encode(frame)
