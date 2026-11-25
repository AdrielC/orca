package orca.backend.acp

import orca.json.RawJson

import AcpProtocol.*

class AcpRouterTest extends munit.FunSuite:
  private val promptId = RequestId.Number(7)

  private def expect(
      router: AcpRouter,
      id: RequestId,
      method: AcpMethod
  ): AcpRouter =
    router.expect(id, method).fold(error => fail(error.getMessage), identity)

  private def waiting: AcpRouter =
    expect(AcpRouter.empty, promptId, AcpMethod.Prompt)

  test("responses correlate by id when setup replies arrive in reverse order"):
    val first =
      expect(AcpRouter.empty, RequestId.Text("new"), AcpMethod.NewSession)
    val second = expect(first, RequestId.Number(2), AcpMethod.ListSessions)
    val (third, listed) =
      second.receive("""{"jsonrpc":"2.0","id":2,"result":{"sessions":[]}}""")
    assertEquals(
      listed,
      AcpInbound.Completed(
        RequestId.Number(2),
        AcpMethod.ListSessions,
        Right(AcpResult.SessionsListed(ListSessionsResponse(Vector.empty)))
      )
    )
    val (last, created) = third.receive(
      """{"jsonrpc":"2.0","id":"new","result":{"sessionId":"sess-a"}}"""
    )
    assertEquals(
      created,
      AcpInbound.Completed(
        RequestId.Text("new"),
        AcpMethod.NewSession,
        Right(
          AcpResult.SessionCreated(NewSessionResponse(AcpSessionId("sess-a")))
        )
      )
    )
    assertEquals(last.pending, Map.empty)

  test("string and numeric ids with the same spelling are distinct"):
    val router = expect(waiting, RequestId.Text("7"), AcpMethod.LoadSession)
    val (next, event) =
      router.receive("""{"jsonrpc":"2.0","id":"7","result":{}}""")
    assertEquals(
      event,
      AcpInbound.Completed(
        RequestId.Text("7"),
        AcpMethod.LoadSession,
        Right(AcpResult.SessionLoaded(LoadSessionResponse()))
      )
    )
    assertEquals(next.pending, waiting.pending)

  test("an interleaved update does not consume a pending prompt response"):
    val (next, event) = waiting.receive(
      """{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sess-a","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"hello"}}}}"""
    )
    assertEquals(next, waiting)
    assertEquals(
      event,
      AcpInbound.Updated(
        SessionNotification(
          AcpSessionId("sess-a"),
          SessionUpdate.Chunk(ChunkKind.Agent, ContentBlock.Text("hello"))
        )
      )
    )

  test("permission requests are distinct from notifications and responses"):
    val (next, event) = waiting.receive(
      """{"jsonrpc":"2.0","id":"permission-1","method":"session/request_permission","params":{"sessionId":"sess-a","toolCall":{"toolCallId":"call-1"},"options":[]}}"""
    )
    event match
      case AcpInbound.PermissionRequested(id, request) =>
        assertEquals(id, RequestId.Text("permission-1"))
        assertEquals(request.toolCall.toolCallId, ToolCallId("call-1"))
      case other => fail(s"Expected permission request, got $other")
    assertEquals(next, waiting)

  test(
    "an incoming request may reuse an outgoing request id without completing it"
  ):
    val (next, event) = waiting.receive(
      """{"jsonrpc":"2.0","id":7,"method":"cursor/create_plan","params":{}}"""
    )
    assertEquals(next, waiting)
    assertEquals(
      event,
      AcpInbound.Reply(
        RpcMessage.Response(
          promptId,
          Left(RpcError.methodNotFound(AcpMethod.Unknown("cursor/create_plan")))
        )
      )
    )

  test(
    "unsupported blocking extension receives a serializable method-not-found error"
  ):
    val (_, event) = waiting.receive(
      """{"jsonrpc":"2.0","id":"question","method":"cursor/ask_question","params":{}}"""
    )
    event match
      case AcpInbound.Reply(response) =>
        val json = JsonRpc.encode(response)
        assertEquals(JsonRpc.decode(json), Right(response))
        assertEquals(response.id, RequestId.Text("question"))
        assertEquals(response.result.left.toOption.map(_.code), Some(-32601))
      case other => fail(s"Expected error reply, got $other")

  test(
    "unsupported client filesystem request receives an error rather than being ignored"
  ):
    val (_, event) = waiting.receive(
      """{"jsonrpc":"2.0","id":5,"method":"fs/read_text_file","params":{"path":"/project/file"}}"""
    )
    assertEquals(
      event,
      AcpInbound.Reply(
        RpcMessage.Response(
          RequestId.Number(5),
          Left(RpcError.methodNotFound(AcpMethod.Unknown("fs/read_text_file")))
        )
      )
    )

  test("unknown notifications are retained without emitting replies"):
    val json =
      """{"jsonrpc":"2.0","method":"cursor/update_todos","params":{"todos":[]}}"""
    val (next, event) = waiting.receive(json)
    assertEquals(next, waiting)
    assertEquals(
      event,
      AcpInbound.Ignored(
        RpcMessage.Notification(
          AcpMethod.Unknown("cursor/update_todos"),
          Some(RawJson("{\"todos\":[]}"))
        )
      )
    )

  test("unknown response ids cannot complete a pending prompt"):
    val (next, event) = waiting.receive(
      """{"jsonrpc":"2.0","id":8,"result":{"stopReason":"end_turn"}}"""
    )
    assertEquals(next, waiting)
    assertEquals(event, AcpInbound.UnexpectedResponse(RequestId.Number(8)))

  test("duplicate responses are unsolicited after the first completion"):
    val json = """{"jsonrpc":"2.0","id":7,"result":{"stopReason":"end_turn"}}"""
    val (next, first) = waiting.receive(json)
    assertEquals(
      first,
      AcpInbound.Completed(
        promptId,
        AcpMethod.Prompt,
        Right(AcpResult.PromptCompleted(PromptResponse(StopReason.EndTurn)))
      )
    )
    assertEquals(next.receive(json)._2, AcpInbound.UnexpectedResponse(promptId))

  test("valid remote errors complete only the matching pending call"):
    val router = expect(waiting, RequestId.Number(1), AcpMethod.Initialize)
    val (next, event) = router.receive(
      """{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"Auth required","data":{"method":"cursor_login"}}}"""
    )
    assertEquals(next, waiting)
    assertEquals(
      event,
      AcpInbound.Completed(
        RequestId.Number(1),
        AcpMethod.Initialize,
        Left(
          RpcError(
            code = -32000,
            message = "Auth required",
            data = Some(RawJson("{\"method\":\"cursor_login\"}"))
          )
        )
      )
    )

  test("null id errors are uncorrelated"):
    val (next, event) = waiting.receive(
      """{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}}"""
    )
    assertEquals(next, waiting)
    assertEquals(event, AcpInbound.UnexpectedResponse(RequestId.Null))

  test(
    "malformed envelopes surface a terminal protocol diagnostic without replying"
  ):
    val (next, event) = waiting.receive("not JSON")
    assertEquals(next, waiting)
    assert(event.isInstanceOf[AcpInbound.Malformed])

  test("invalid known results terminate their matching pending request"):
    val (next, event) =
      waiting.receive("""{"jsonrpc":"2.0","id":7,"result":{}}""")
    assertEquals(next.pending, Map.empty)
    event match
      case AcpInbound.InvalidResponse(id, _) => assertEquals(id, promptId)
      case other => fail(s"Expected invalid response, got $other")

  test("null known results cannot pass as successful load responses"):
    val router =
      expect(AcpRouter.empty, RequestId.Number(1), AcpMethod.LoadSession)
    val (_, event) =
      router.receive("""{"jsonrpc":"2.0","id":1,"result":null}""")
    assert(event.isInstanceOf[AcpInbound.InvalidResponse])

  test("invalid permission params receive invalid-params with the original id"):
    val (next, event) = waiting.receive(
      """{"jsonrpc":"2.0","id":5,"method":"session/request_permission","params":{}}"""
    )
    assertEquals(next, waiting)
    assertEquals(
      event,
      AcpInbound.Reply(
        RpcMessage.Response(RequestId.Number(5), Left(RpcError.invalidParams))
      )
    )

  test("a null permission tool call receives invalid-params"):
    val (_, event) = waiting.receive(
      """{"jsonrpc":"2.0","id":5,"method":"session/request_permission","params":{"sessionId":"sess-a","toolCall":null,"options":[]}}"""
    )
    assertEquals(
      event,
      AcpInbound.Reply(
        RpcMessage.Response(RequestId.Number(5), Left(RpcError.invalidParams))
      )
    )

  test("a null permission option list receives invalid-params"):
    val (_, event) = waiting.receive(
      """{"jsonrpc":"2.0","id":5,"method":"session/request_permission","params":{"sessionId":"sess-a","toolCall":{"toolCallId":"call-1"},"options":null}}"""
    )
    assertEquals(
      event,
      AcpInbound.Reply(
        RpcMessage.Response(RequestId.Number(5), Left(RpcError.invalidParams))
      )
    )
  test("malformed session updates are terminal diagnostics"):
    val (_, event) = waiting.receive(
      """{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sess-a"}}"""
    )
    assert(event.isInstanceOf[AcpInbound.Malformed])

  test("pending request ids cannot be overwritten"):
    assert(waiting.expect(promptId, AcpMethod.NewSession).isLeft)

  test("null ids cannot be registered for outgoing correlation"):
    assert(AcpRouter.empty.expect(RequestId.Null, AcpMethod.Initialize).isLeft)

  test("notifications cannot be registered as pending outgoing requests"):
    assert(AcpRouter.empty.expect(RequestId.Number(1), AcpMethod.Cancel).isLeft)
