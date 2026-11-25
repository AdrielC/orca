package orca.backend.acp

import orca.json.RawJson

import com.github.plokhotnyuk.jsoniter_scala.macros.ConfiguredJsonValueCodec

private[backend] final case class RpcError(
    code: Int,
    message: String,
    data: Option[RawJson] = None
) derives ConfiguredJsonValueCodec

private[backend] object RpcError:
  def methodNotFound(method: AcpMethod): RpcError =
    RpcError(code = -32601, message = s"Unsupported ACP method: ${method.wire}")

  def invalidParams: RpcError =
    RpcError(code = -32602, message = "Invalid ACP request parameters")
