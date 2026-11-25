package orca.backend.acp

import WireJson.given

object AcpWireFixtures:

  def agentChunk(sessionId: String, text: String): String =
    s"""{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"$sessionId","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":${WireJson
        .write(text)
        .value}}}}}"""

  def toolStarted(sessionId: String, toolCallId: String): String =
    s"""{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"$sessionId","update":{"sessionUpdate":"tool_call","toolCallId":"$toolCallId","title":"run"}}}"""

  def toolCompleted(
      sessionId: String,
      toolCallId: String,
      kind: String
  ): String =
    s"""{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"$sessionId","update":{"sessionUpdate":"tool_call_update","toolCallId":"$toolCallId","kind":"$kind","status":"completed","rawOutput":[1,2]}}}"""
