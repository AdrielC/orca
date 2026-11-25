package orca.backend.acp

import AcpProtocol.*

import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** Tunables for one process-scoped ACP client. */
private[backend] final case class AcpClientConfig(
    requestTimeout: FiniteDuration,
    exitTimeout: FiniteDuration,
    exitPollInterval: FiniteDuration,
    initialize: InitializeRequest,
    capabilityRequirements: AcpCapabilityRequirements,
    nowNanos: () => Long
)

private[backend] object AcpClientConfig:
  val defaultInitialize: InitializeRequest = InitializeRequest(
    protocolVersion = AcpProtocol.Version,
    clientCapabilities = ClientCapabilities(),
    clientInfo = Some(Implementation(name = "orca", version = "0.x"))
  )

  val ExitPollInterval: FiniteDuration = 25.millis

  def apply(
      requestTimeout: FiniteDuration,
      exitTimeout: FiniteDuration,
      initialize: InitializeRequest,
      capabilityRequirements: AcpCapabilityRequirements
  ): AcpClientConfig =
    AcpClientConfig(
      requestTimeout = requestTimeout,
      exitTimeout = exitTimeout,
      exitPollInterval = ExitPollInterval,
      initialize = initialize,
      capabilityRequirements = capabilityRequirements,
      nowNanos = () => System.nanoTime()
    )

  def default: AcpClientConfig = AcpClientConfig(
    requestTimeout = 30.seconds,
    exitTimeout = 5.seconds,
    initialize = defaultInitialize,
    capabilityRequirements = AcpCapabilityRequirements.none
  )

  def forTests(
      requestTimeout: FiniteDuration,
      exitTimeout: FiniteDuration,
      initialize: InitializeRequest = defaultInitialize,
      capabilityRequirements: AcpCapabilityRequirements =
        AcpCapabilityRequirements.none,
      exitPollInterval: FiniteDuration = ExitPollInterval,
      nowNanos: () => Long = () => System.nanoTime()
  ): AcpClientConfig =
    AcpClientConfig(
      requestTimeout = requestTimeout,
      exitTimeout = exitTimeout,
      exitPollInterval = exitPollInterval,
      initialize = initialize,
      capabilityRequirements = capabilityRequirements,
      nowNanos = nowNanos
    )
