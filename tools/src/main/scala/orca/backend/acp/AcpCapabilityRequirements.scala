package orca.backend.acp

import AcpProtocol.*

private[backend] final case class AcpCapabilityRequirements(
    protocolVersion: Int,
    requireLoadSession: Boolean,
    requireSessionList: Boolean
):
  def validate(
      response: InitializeResponse
  ): Either[AcpSetupFailure, InitializeResponse] =
    val checks = List(
      Option.when(response.protocolVersion != protocolVersion)(
        AcpSetupFailure.CapabilityUnsupported(
          s"Unsupported ACP protocol version ${response.protocolVersion}"
        )
      ),
      Option.when(
        requireLoadSession && !response.agentCapabilities.loadSession
      )(
        AcpSetupFailure.CapabilityUnsupported(
          "Agent does not advertise loadSession"
        )
      ),
      Option.when(
        requireSessionList &&
          response.agentCapabilities.sessionCapabilities.list.isEmpty
      )(
        AcpSetupFailure.CapabilityUnsupported(
          "Agent does not advertise session list"
        )
      )
    ).flatten
    checks.headOption.fold(Right(response))(Left(_))

private[backend] object AcpCapabilityRequirements:
  val none: AcpCapabilityRequirements = AcpCapabilityRequirements(
    protocolVersion = AcpProtocol.Version,
    requireLoadSession = false,
    requireSessionList = false
  )
