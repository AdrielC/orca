package orca.backend.acp

import orca.OrcaFlowException

/** Recoverable setup/handshake failure before a prompt turn starts. */
private[backend] sealed abstract class AcpSetupFailure(message: String)
    extends OrcaFlowException(message)

private[backend] object AcpSetupFailure:
  case object ProtocolFailed extends AcpSetupFailure("ACP protocol failed")
  final case class RequestTimedOut(method: AcpMethod)
      extends AcpSetupFailure(s"Timed out waiting for ${method.wire}")
  case object StdoutClosedEarly
      extends AcpSetupFailure("ACP stdout closed before every reply")
  case object StdoutClosed extends AcpSetupFailure("ACP stdout closed")
  case object StdinClosed extends AcpSetupFailure("ACP stdin closed")
  final case class StdinWriteFailed(detail: String)
      extends AcpSetupFailure(s"ACP stdin closed: $detail")
  case object CleanedUp
      extends AcpSetupFailure("ACP client cleaned up before the reply arrived")
  final case class CapabilityUnsupported(detail: String)
      extends AcpSetupFailure(detail)
  final case class DecodeFailure(detail: String) extends AcpSetupFailure(detail)
  final case class RpcFailure(rpc: RpcError)
      extends AcpSetupFailure(rpc.message)
  final case class UnexpectedResult(method: AcpMethod, detail: String)
      extends AcpSetupFailure(s"Unexpected ${method.wire} result: $detail")
  final case class WaitFailed(detail: String)
      extends AcpSetupFailure(s"Failed waiting for reply: $detail")
