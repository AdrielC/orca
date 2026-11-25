package orca.backend.acp

import orca.json.RawJson

import com.github.plokhotnyuk.jsoniter_scala.core.*
import com.github.plokhotnyuk.jsoniter_scala.macros.{
  CodecMakerConfig,
  ConfiguredJsonValueCodec
}

import WireJson.given

private[backend] opaque type AcpSessionId = String
private[backend] object AcpSessionId:
  def apply(value: String): AcpSessionId = value
  extension (id: AcpSessionId) def value: String = id
  given JsonValueCodec[AcpSessionId] = WireJson.stringCodec(apply, _.value)

private[backend] opaque type ToolCallId = String
private[backend] object ToolCallId:
  def apply(value: String): ToolCallId = value
  extension (id: ToolCallId) def value: String = id
  given JsonValueCodec[ToolCallId] = WireJson.stringCodec(apply, _.value)

private[backend] opaque type PermissionOptionId = String
private[backend] object PermissionOptionId:
  def apply(value: String): PermissionOptionId = value
  extension (id: PermissionOptionId) def value: String = id
  given JsonValueCodec[PermissionOptionId] =
    WireJson.stringCodec(apply, _.value)

/** ACP v1 wire DTOs. Capability defaults follow v1; unknown payloads remain
  * opaque.
  */
private[backend] object AcpProtocol:
  val Version: Int = 1
  private inline given CodecMakerConfig =
    CodecMakerConfig.withRequireCollectionFields(true).withTransientEmpty(false)

  final case class Implementation(
      name: String,
      version: String,
      title: Option[String] = None
  ) derives ConfiguredJsonValueCodec
  final case class FileSystemCapabilities(
      readTextFile: Boolean = false,
      writeTextFile: Boolean = false
  ) derives ConfiguredJsonValueCodec
  final case class ClientCapabilities(
      fs: FileSystemCapabilities = FileSystemCapabilities(),
      terminal: Boolean = false
  ) derives ConfiguredJsonValueCodec
  final case class InitializeRequest(
      protocolVersion: Int,
      clientCapabilities: ClientCapabilities,
      clientInfo: Option[Implementation] = None
  ) derives ConfiguredJsonValueCodec
  final case class McpCapabilities(http: Boolean = false, sse: Boolean = false)
      derives ConfiguredJsonValueCodec
  final case class SessionCapabilities(list: Option[RawJson] = None)
      derives ConfiguredJsonValueCodec
  final case class AgentCapabilities(
      loadSession: Boolean = false,
      mcpCapabilities: McpCapabilities = McpCapabilities(),
      sessionCapabilities: SessionCapabilities = SessionCapabilities(),
      promptCapabilities: Option[RawJson] = None
  ) derives ConfiguredJsonValueCodec
  final case class AuthMethod(
      id: String,
      name: String,
      description: Option[String] = None
  ) derives ConfiguredJsonValueCodec
  final case class InitializeResponse(
      protocolVersion: Int,
      agentCapabilities: AgentCapabilities = AgentCapabilities(),
      authMethods: Vector[AuthMethod] = Vector.empty,
      agentInfo: Option[Implementation] = None
  ) derives ConfiguredJsonValueCodec

  final case class EnvVariable(name: String, value: String)
      derives ConfiguredJsonValueCodec
  final case class HttpHeader(name: String, value: String)
      derives ConfiguredJsonValueCodec
  enum McpServer:
    case Stdio(
        name: String,
        command: String,
        args: Vector[String],
        env: Vector[EnvVariable]
    )
    case Http(name: String, url: String, headers: Vector[HttpHeader])
    case Sse(name: String, url: String, headers: Vector[HttpHeader])
    case Unknown(transport: String, raw: RawJson)

  object McpServer:
    private case class StdioWire(
        name: String,
        command: String,
        args: Vector[String],
        env: Vector[EnvVariable]
    ) derives ConfiguredJsonValueCodec
    private case class RemoteWire(
        `type`: String,
        name: String,
        url: String,
        headers: Vector[HttpHeader]
    ) derives ConfiguredJsonValueCodec
    given JsonValueCodec[McpServer] = WireJson.codec(
      raw =>
        val fields = WireJson.fields(raw)
        val transport =
          fields.get("type").map(_ => WireJson.required[String](fields, "type"))
        transport match
          case None =>
            val w = readFromStringReentrant[StdioWire](raw.value)
            Stdio(
              name = w.name,
              command = w.command,
              args = w.args,
              env = w.env
            )
          case Some("http" | "sse") =>
            val w = readFromStringReentrant[RemoteWire](raw.value)
            if w.`type` == "http" then
              Http(name = w.name, url = w.url, headers = w.headers)
            else Sse(name = w.name, url = w.url, headers = w.headers)
          case Some(other) => Unknown(other, raw),
      {
        case Stdio(name, command, args, env) =>
          WireJson.write(
            StdioWire(name = name, command = command, args = args, env = env)
          )
        case Http(name, url, headers) =>
          WireJson.write(
            RemoteWire(
              `type` = "http",
              name = name,
              url = url,
              headers = headers
            )
          )
        case Sse(name, url, headers) =>
          WireJson.write(
            RemoteWire(
              `type` = "sse",
              name = name,
              url = url,
              headers = headers
            )
          )
        case Unknown(_, raw) => raw
      }
    )

  final case class NewSessionRequest(cwd: String, mcpServers: Vector[McpServer])
      derives ConfiguredJsonValueCodec
  final case class LoadSessionRequest(
      sessionId: AcpSessionId,
      cwd: String,
      mcpServers: Vector[McpServer]
  ) derives ConfiguredJsonValueCodec
  final case class NewSessionResponse(
      sessionId: AcpSessionId,
      modes: Option[RawJson] = None,
      configOptions: Option[RawJson] = None
  ) derives ConfiguredJsonValueCodec
  final case class LoadSessionResponse(
      modes: Option[RawJson] = None,
      configOptions: Option[RawJson] = None
  ) derives ConfiguredJsonValueCodec
  final case class ListSessionsRequest(
      cwd: Option[String] = None,
      cursor: Option[String] = None
  ) derives ConfiguredJsonValueCodec
  final case class SessionInfo(
      sessionId: AcpSessionId,
      cwd: String,
      title: Option[String] = None,
      updatedAt: Option[String] = None
  ) derives ConfiguredJsonValueCodec
  final case class ListSessionsResponse(
      sessions: Vector[SessionInfo],
      nextCursor: Option[String] = None
  ) derives ConfiguredJsonValueCodec

  enum ContentBlock:
    case Text(text: String)
    case Unknown(kind: String, raw: RawJson)
  object ContentBlock:
    private case class TextWire(`type`: String, text: String)
        derives ConfiguredJsonValueCodec
    given JsonValueCodec[ContentBlock] = WireJson.codec(
      raw =>
        val fields = WireJson.fields(raw)
        WireJson.required[String](fields, "type") match
          case "text" => Text(WireJson.required[String](fields, "text"))
          case other  => Unknown(other, raw),
      {
        case Text(text) =>
          WireJson.write(TextWire(`type` = "text", text = text))
        case Unknown(_, raw) => raw
      }
    )

  final case class PromptRequest(
      sessionId: AcpSessionId,
      prompt: Vector[ContentBlock]
  ) derives ConfiguredJsonValueCodec
  final case class CancelNotification(sessionId: AcpSessionId)
      derives ConfiguredJsonValueCodec

  enum StopReason(val wire: String):
    case EndTurn extends StopReason("end_turn")
    case MaxTokens extends StopReason("max_tokens")
    case MaxTurnRequests extends StopReason("max_turn_requests")
    case Refusal extends StopReason("refusal")
    case Cancelled extends StopReason("cancelled")
    case Unknown(raw: String) extends StopReason(raw)
  object StopReason:
    private val byWire: Map[String, StopReason] = Vector(
      EndTurn,
      MaxTokens,
      MaxTurnRequests,
      Refusal,
      Cancelled
    ).map(value => value.wire -> value).toMap

    given JsonValueCodec[StopReason] = WireJson.stringCodec(
      raw => byWire.getOrElse(raw, Unknown(raw)),
      _.wire
    )
  final case class PromptResponse(stopReason: StopReason)
      derives ConfiguredJsonValueCodec

  enum ToolKind(val wire: String):
    case Read extends ToolKind("read")
    case Edit extends ToolKind("edit")
    case Delete extends ToolKind("delete")
    case Move extends ToolKind("move")
    case Search extends ToolKind("search")
    case Execute extends ToolKind("execute")
    case Think extends ToolKind("think")
    case Fetch extends ToolKind("fetch")
    case SwitchMode extends ToolKind("switch_mode")
    case Other extends ToolKind("other")
    case Unknown(raw: String) extends ToolKind(raw)
  object ToolKind:
    private val byWire: Map[String, ToolKind] = Vector(
      Read,
      Edit,
      Delete,
      Move,
      Search,
      Execute,
      Think,
      Fetch,
      SwitchMode,
      Other
    ).map(value => value.wire -> value).toMap

    given JsonValueCodec[ToolKind] = WireJson.stringCodec(
      raw => byWire.getOrElse(raw, Unknown(raw)),
      _.wire
    )
  enum ToolStatus(val wire: String):
    case Pending extends ToolStatus("pending")
    case InProgress extends ToolStatus("in_progress")
    case Completed extends ToolStatus("completed")
    case Failed extends ToolStatus("failed")
    case Unknown(raw: String) extends ToolStatus(raw)
  object ToolStatus:
    private val byWire: Map[String, ToolStatus] = Vector(
      Pending,
      InProgress,
      Completed,
      Failed
    ).map(value => value.wire -> value).toMap

    given JsonValueCodec[ToolStatus] = WireJson.stringCodec(
      raw => byWire.getOrElse(raw, Unknown(raw)),
      _.wire
    )
  final case class ToolLocation(path: String, line: Option[Long] = None)
      derives ConfiguredJsonValueCodec
  final case class ToolCall(
      toolCallId: ToolCallId,
      title: String,
      kind: ToolKind = ToolKind.Other,
      status: ToolStatus = ToolStatus.Pending,
      content: Vector[RawJson] = Vector.empty,
      locations: Vector[ToolLocation] = Vector.empty,
      rawInput: Option[RawJson] = None,
      rawOutput: Option[RawJson] = None
  ) derives ConfiguredJsonValueCodec
  final case class ToolCallUpdate(
      toolCallId: ToolCallId,
      title: Option[String] = None,
      kind: Option[ToolKind] = None,
      status: Option[ToolStatus] = None,
      content: Option[Vector[RawJson]] = None,
      locations: Option[Vector[ToolLocation]] = None,
      rawInput: Option[RawJson] = None,
      rawOutput: Option[RawJson] = None
  ) derives ConfiguredJsonValueCodec

  enum ChunkKind(val wire: String):
    case User extends ChunkKind("user_message_chunk")
    case Agent extends ChunkKind("agent_message_chunk")
    case Thought extends ChunkKind("agent_thought_chunk")
  enum SessionUpdate:
    case Chunk(kind: ChunkKind, content: ContentBlock)
    case ToolStarted(tool: ToolCall)
    case ToolUpdated(tool: ToolCallUpdate)
    case Unknown(kind: String, raw: RawJson)
  object SessionUpdate:
    given JsonValueCodec[SessionUpdate] = WireJson.codec(
      raw =>
        val fields = WireJson.fields(raw)
        def chunk(kind: ChunkKind): SessionUpdate =
          Chunk(kind, WireJson.required[ContentBlock](fields, "content"))
        WireJson.required[String](fields, "sessionUpdate") match
          case "user_message_chunk"  => chunk(ChunkKind.User)
          case "agent_message_chunk" => chunk(ChunkKind.Agent)
          case "agent_thought_chunk" => chunk(ChunkKind.Thought)
          case "tool_call" =>
            ToolStarted(readFromStringReentrant[ToolCall](raw.value))
          case "tool_call_update" =>
            ToolUpdated(readFromStringReentrant[ToolCallUpdate](raw.value))
          case other => Unknown(other, raw),
      {
        case SessionUpdate.Unknown(_, raw) => raw
        case update =>
          val (kind, fields) = update match
            case Chunk(kind, content) =>
              (kind.wire, Map("content" -> WireJson.write(content)))
            case ToolStarted(tool) =>
              ("tool_call", WireJson.fields(WireJson.write(tool)))
            case ToolUpdated(tool) =>
              ("tool_call_update", WireJson.fields(WireJson.write(tool)))
            case Unknown(_, _) =>
              throw new IllegalStateException("Unknown update handled above")
          WireJson.write(fields.updated("sessionUpdate", WireJson.write(kind)))
      }
    )
  final case class SessionNotification(
      sessionId: AcpSessionId,
      update: SessionUpdate
  ) derives ConfiguredJsonValueCodec

  enum PermissionKind(val wire: String):
    case AllowOnce extends PermissionKind("allow_once")
    case AllowAlways extends PermissionKind("allow_always")
    case RejectOnce extends PermissionKind("reject_once")
    case RejectAlways extends PermissionKind("reject_always")
    case Unknown(raw: String) extends PermissionKind(raw)
  object PermissionKind:
    private val byWire: Map[String, PermissionKind] = Vector(
      AllowOnce,
      AllowAlways,
      RejectOnce,
      RejectAlways
    ).map(value => value.wire -> value).toMap

    given JsonValueCodec[PermissionKind] = WireJson.stringCodec(
      raw => byWire.getOrElse(raw, Unknown(raw)),
      _.wire
    )
  final case class PermissionOption(
      optionId: PermissionOptionId,
      name: String,
      kind: PermissionKind
  ) derives ConfiguredJsonValueCodec
  final case class RequestPermissionRequest(
      sessionId: AcpSessionId,
      toolCall: ToolCallUpdate,
      options: Vector[PermissionOption]
  ) derives ConfiguredJsonValueCodec
  enum PermissionOutcome:
    case Cancelled
    case Selected(optionId: PermissionOptionId)
    case Unknown(kind: String, raw: RawJson)
  object PermissionOutcome:
    given JsonValueCodec[PermissionOutcome] = WireJson.codec(
      raw =>
        val fields = WireJson.fields(raw)
        WireJson.required[String](fields, "outcome") match
          case "cancelled" => Cancelled
          case "selected" =>
            Selected(WireJson.required[PermissionOptionId](fields, "optionId"))
          case other => Unknown(other, raw),
      {
        case Cancelled =>
          WireJson.write(Map("outcome" -> WireJson.write("cancelled")))
        case Selected(id) =>
          WireJson.write(
            Map(
              "outcome" -> WireJson.write("selected"),
              "optionId" -> WireJson.write(id)
            )
          )
        case Unknown(_, raw) => raw
      }
    )
  final case class RequestPermissionResponse(outcome: PermissionOutcome)
      derives ConfiguredJsonValueCodec
