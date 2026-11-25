package orca.backend.acp

import orca.json.RawJson

import com.github.plokhotnyuk.jsoniter_scala.core.*

import AcpProtocol.*

class AcpCodecTest extends munit.FunSuite:
  private def decode[A: JsonValueCodec](json: String): A =
    WireJson
      .read[A](RawJson(json))
      .fold(error => fail(error.getMessage), identity)

  test(
    "initialize accepts absent optional capabilities and unknown extensions"
  ):
    val response = decode[InitializeResponse](
      """{"protocolVersion":1,"futureCapability":{"enabled":true}}"""
    )
    assertEquals(response.protocolVersion, Version)
    assertEquals(response.agentCapabilities.loadSession, false)
    assertEquals(response.agentInfo, None)
    assertEquals(response.authMethods, Vector.empty)

  test("initialize decodes load, session list and HTTP/SSE MCP capabilities"):
    val response = decode[InitializeResponse](
      """{"protocolVersion":1,"agentCapabilities":{"loadSession":true,
        |"sessionCapabilities":{"list":{}},"mcpCapabilities":{"http":true,"sse":true}},
        |"agentInfo":{"name":"cursor","version":"2026.11.22"},
        |"authMethods":[{"id":"cursor_login","name":"Cursor login"}]}""".stripMargin
    )
    assert(response.agentCapabilities.loadSession)
    assert(response.agentCapabilities.sessionCapabilities.list.isDefined)
    assertEquals(
      response.agentCapabilities.mcpCapabilities,
      McpCapabilities(http = true, sse = true)
    )
    assertEquals(response.authMethods.map(_.id), Vector("cursor_login"))

  test(
    "new session carries cwd and stdio MCP configuration without a transport tag"
  ):
    val request = decode[NewSessionRequest](
      """{"cwd":"/project","mcpServers":[{"name":"repo","command":"/bin/mcp",
        |"args":["--read-only"],"env":[{"name":"MODE","value":"test"}]}]}""".stripMargin
    )
    assertEquals(request.cwd, "/project")
    assertEquals(
      request.mcpServers,
      Vector(
        McpServer.Stdio(
          name = "repo",
          command = "/bin/mcp",
          args = Vector("--read-only"),
          env = Vector(EnvVariable(name = "MODE", value = "test"))
        )
      )
    )
    val encoded = WireJson.fields(WireJson.write(request))
    assert(encoded.contains("mcpServers"))
    assertEquals(decode[NewSessionRequest](writeToString(request)), request)

  test("load session carries HTTP and SSE MCP headers"):
    val request = decode[LoadSessionRequest](
      """{"sessionId":"sess-a","cwd":"/project","mcpServers":[
        |{"type":"http","name":"http","url":"http://localhost/mcp","headers":[{"name":"X-Test","value":"yes"}]},
        |{"type":"sse","name":"sse","url":"http://localhost/sse","headers":[]}]}""".stripMargin
    )
    assertEquals(request.sessionId, AcpSessionId("sess-a"))
    assertEquals(
      request.mcpServers.head,
      McpServer.Http(
        name = "http",
        url = "http://localhost/mcp",
        headers = Vector(HttpHeader(name = "X-Test", value = "yes"))
      )
    )
    assertEquals(decode[LoadSessionRequest](writeToString(request)), request)

  test("new session requires the MCP server list even when it would be empty"):
    assert(
      WireJson.read[NewSessionRequest](RawJson("""{"cwd":"/project"}""")).isLeft
    )

  test("new session encodes the required empty MCP list"):
    val json = WireJson.write(NewSessionRequest("/project", Vector.empty))
    assertEquals(WireJson.fields(json).get("mcpServers"), Some(RawJson("[]")))

  test(
    "initialize requests pin ACP v1 and advertise no filesystem or terminal access"
  ):
    val json = WireJson.write(InitializeRequest(Version, ClientCapabilities()))
    val request = decode[InitializeRequest](json.value)
    assertEquals(request.protocolVersion, 1)
    assertEquals(request.clientCapabilities.terminal, false)
    assertEquals(request.clientCapabilities.fs.readTextFile, false)
    assertEquals(request.clientCapabilities.fs.writeTextFile, false)

  test("excessively nested extension payloads fail with a protocol error"):
    val payload = "[" * 130 + "null" + "]" * 130
    val json =
      s"""{"jsonrpc":"2.0","method":"vendor/note","params":{"data":$payload}}"""
    assert(JsonRpc.decode(json).isLeft)

  test("new session response retains optional mode and config payloads"):
    val response = decode[NewSessionResponse](
      """{"sessionId":"sess-a","modes":{"currentModeId":"agent","availableModes":[]},"configOptions":[]}"""
    )
    assertEquals(response.sessionId, AcpSessionId("sess-a"))
    assert(response.modes.isDefined)
    assertEquals(response.configOptions.map(_.value), Some("[]"))

  test("load response does not require a new session id"):
    assertEquals(decode[LoadSessionResponse]("{}"), LoadSessionResponse())

  test("list sessions decodes pagination and nullable metadata"):
    val response = decode[ListSessionsResponse](
      """{"sessions":[{"sessionId":"sess-a","cwd":"/project","title":null,"updatedAt":"2026-11-22T12:00:00Z"}],"nextCursor":"page-2"}"""
    )
    assertEquals(response.nextCursor, Some("page-2"))
    assertEquals(response.sessions.head.title, None)
    assertEquals(response.sessions.head.cwd, "/project")

  test("list request permits omitted cwd and cursor"):
    assertEquals(decode[ListSessionsRequest]("{}"), ListSessionsRequest())

  test("prompt encodes v1 text content rather than a Scala sum discriminator"):
    val prompt = PromptRequest(
      AcpSessionId("sess-a"),
      Vector(ContentBlock.Text("hello\nworld"))
    )
    val expected =
      """{"sessionId":"sess-a","prompt":[{"type":"text","text":"hello\nworld"}]}"""
    assertEquals(decode[PromptRequest](expected), prompt)
    assertEquals(
      WireJson.fields(WireJson.write(prompt)),
      WireJson.fields(RawJson(expected))
    )

  test("cancel is a session-scoped notification with no request id"):
    val message = RpcMessage.Notification(
      AcpMethod.Cancel,
      Some(WireJson.write(CancelNotification(AcpSessionId("sess-a"))))
    )
    val json = JsonRpc.encode(message)
    assert(!WireJson.fields(RawJson(json)).contains("id"))
    assertEquals(JsonRpc.decode(json), Right(message))

  test("unknown content preserves the complete subtree"):
    val raw =
      """{"type":"future_media","payload":{"bytes":[1,2]},"_meta":{"x":true}}"""
    val value = decode[ContentBlock](raw)
    assertEquals(value, ContentBlock.Unknown("future_media", RawJson(raw)))
    assertEquals(writeToString(value), raw)

  test("thought chunks decode independently of assistant message chunks"):
    val notification = decode[SessionNotification](
      """{"sessionId":"sess-a","update":{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"thinking"}}}"""
    )
    assertEquals(
      notification.update,
      SessionUpdate.Chunk(ChunkKind.Thought, ContentBlock.Text("thinking"))
    )

  test("unknown session updates retain extension payloads"):
    val raw =
      """{"sessionUpdate":"vendor_progress", "percent":50,"_meta":{"trace":"abc"}}"""
    val value = decode[SessionUpdate](raw)
    assertEquals(value, SessionUpdate.Unknown("vendor_progress", RawJson(raw)))
    assertEquals(writeToString(value), raw)

  test("tool call creation applies v1 status and kind defaults"):
    val value = decode[SessionUpdate](
      """{"sessionUpdate":"tool_call","toolCallId":"call-1","title":"Read source"}"""
    )
    assertEquals(
      value,
      SessionUpdate.ToolStarted(ToolCall(ToolCallId("call-1"), "Read source"))
    )

  test("tool call updates retain raw arguments and optional status"):
    val value = decode[ToolCallUpdate](
      """{"toolCallId":"call-1","kind":"execute","rawInput":{"command":"sbt test"},"rawOutput":[1,2]}"""
    )
    assertEquals(value.kind, Some(ToolKind.Execute))
    assertEquals(value.status, None)
    assertEquals(
      value.rawInput.map(_.value),
      Some("""{"command":"sbt test"}""")
    )
    assertEquals(value.rawOutput.map(_.value), Some("[1,2]"))

  test("unknown tool status remains explicit"):
    assertEquals(decode[ToolStatus]("\"paused\""), ToolStatus.Unknown("paused"))

  test("permission request accepts a minimal partial tool call"):
    val request = decode[RequestPermissionRequest](
      """{"sessionId":"sess-a","toolCall":{"toolCallId":"call-1"},"options":[
        |{"optionId":"once","name":"Allow once","kind":"allow_once"},
        |{"optionId":"no","name":"Reject","kind":"reject_once"}]}""".stripMargin
    )
    assertEquals(request.toolCall.title, None)
    assertEquals(
      request.options.map(_.kind),
      Vector(PermissionKind.AllowOnce, PermissionKind.RejectOnce)
    )

  test("unknown permission kinds do not become allow options"):
    val option = decode[PermissionOption](
      """{"optionId":"x","name":"Allow","kind":"vendor_default"}"""
    )
    assertEquals(option.kind, PermissionKind.Unknown("vendor_default"))

  test("selected permission response has the required nested outcome"):
    val response = RequestPermissionResponse(
      PermissionOutcome.Selected(PermissionOptionId("once"))
    )
    assertEquals(
      writeToString(response),
      """{"outcome":{"outcome":"selected","optionId":"once"}}"""
    )

  test("cancelled permission response has no selected option"):
    val response = RequestPermissionResponse(PermissionOutcome.Cancelled)
    assertEquals(
      writeToString(response),
      """{"outcome":{"outcome":"cancelled"}}"""
    )

  test("unknown permission outcomes preserve their full payload"):
    val raw =
      """{"outcome":"vendor_review", "reason":{"ticket":"review-1"},"_meta":{"future":true}}"""
    val outcome = decode[PermissionOutcome](raw)
    assertEquals(
      outcome,
      PermissionOutcome.Unknown("vendor_review", RawJson(raw))
    )
    assertEquals(writeToString(outcome), raw)

  test("unknown stop reasons remain explicit"):
    val response = decode[PromptResponse]("""{"stopReason":"future_stop"}""")
    assertEquals(response.stopReason, StopReason.Unknown("future_stop"))

  for id <- Vector(
      RequestId.Number(Long.MaxValue),
      RequestId.Number(-1),
      RequestId.Text("12"),
      RequestId.Text("quoted\"\n"),
      RequestId.Null
    )
  do
    test(s"JSON-RPC preserves request id $id"):
      val message = RpcMessage.Request(
        id,
        AcpMethod.Unknown("vendor/blocking"),
        Some(RawJson("{}"))
      )
      assertEquals(JsonRpc.decode(JsonRpc.encode(message)), Right(message))

  test("null result remains a response rather than an absent result"):
    assertEquals(
      JsonRpc.decode("""{"jsonrpc":"2.0","id":1,"result":null}"""),
      Right(RpcMessage.Response(RequestId.Number(1), Right(RawJson("null"))))
    )

  test("valid error response preserves code, message, data and string id"):
    val error = RpcError(
      code = -32000,
      message = "Authentication required",
      data = Some(RawJson("{\"retry\":false}"))
    )
    val message = RpcMessage.Response(RequestId.Text("init"), Left(error))
    assertEquals(JsonRpc.decode(JsonRpc.encode(message)), Right(message))

  private val invalidEnvelopes = Vector(
    "missing version" -> """{"id":1,"result":{}}""",
    "wrong version" -> """{"jsonrpc":"1.0","id":1,"result":{}}""",
    "batch" -> "[]",
    "null" -> "null",
    "non-object" -> "42",
    "truncated JSON" -> """{"jsonrpc":"2.0","id":1""",
    "trailing data" -> """{"jsonrpc":"2.0","id":1,"result":{}} {}""",
    "invalid extension value" -> """{"jsonrpc":"2.0","method":"vendor/note","params":{"value":oops}}""",
    "invalid extension escape" -> """{"jsonrpc":"2.0","method":"vendor/note","params":{"value":"\q"}}""",
    "null method" -> """{"jsonrpc":"2.0","id":1,"method":null}""",
    "boolean id" -> """{"jsonrpc":"2.0","id":true,"result":{}}""",
    "fractional id" -> """{"jsonrpc":"2.0","id":1.5,"result":{}}""",
    "overflow id" -> """{"jsonrpc":"2.0","id":9223372036854775808,"result":{}}""",
    "response missing id" -> """{"jsonrpc":"2.0","result":{}}""",
    "response missing result" -> """{"jsonrpc":"2.0","id":1}""",
    "both result and error" -> """{"jsonrpc":"2.0","id":1,"result":null,"error":{"code":-1,"message":"bad"}}""",
    "mixed request and response" -> """{"jsonrpc":"2.0","id":1,"method":"initialize","result":{}}""",
    "scalar params" -> """{"jsonrpc":"2.0","method":"vendor/note","params":true}""",
    "duplicate id" -> """{"jsonrpc":"2.0","id":1,"id":2,"result":{}}""",
    "null error" -> """{"jsonrpc":"2.0","id":1,"error":null}""",
    "error missing code" -> """{"jsonrpc":"2.0","id":1,"error":{"message":"bad"}}""",
    "error null message" -> """{"jsonrpc":"2.0","id":1,"error":{"code":-1,"message":null}}"""
  )
  for (name, json) <- invalidEnvelopes do
    test(s"rejects invalid envelope: $name"):
      assert(JsonRpc.decode(json).isLeft)
