package orca.backend.acp

import orca.agents.BackendTag

import AcpProtocol.*

/** Everything a provider-agnostic ACP turn needs beyond transport config. */
private[backend] final case class AcpTurnSpec[B <: BackendTag](
    cwd: String,
    mcpServers: Vector[McpServer],
    prompt: String,
    outputSchema: Option[String],
    sessionStart: AcpSessionStart,
    openingPrompt: Option[String]
)

private[backend] enum AcpSessionStart:
  case Fresh
  case Load(sessionId: AcpSessionId)
