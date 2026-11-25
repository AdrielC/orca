package orca.backend.acp

import orca.{AgentTurnFailed, OrcaInteractiveCancelled}
import orca.agents.{AutoApprove, BackendTag, ToolSet, WireSessionId}
import orca.backend.{ApprovalDecision, TurnEvent}
import orca.backend.TurnEventConformance
import orca.events.TurnDebit
import orca.subprocess.FakePipedCliProcess

import ox.{fork, forkDiscard, supervised, timeout}

import java.util.concurrent.LinkedBlockingQueue
import scala.concurrent.duration.*

class AcpLiveTurnTest extends munit.FunSuite:

  private def testConfig: AcpClientConfig =
    AcpClientConfig.forTests(
      requestTimeout = 2.seconds,
      exitTimeout = 50.millis
    )

  private final class TurnFake extends FakePipedCliProcess():
    private val written = LinkedBlockingQueue[String]()
    private val scripted = scala.collection.mutable.Queue[() => Unit]()

    override def writeLine(line: String): Unit =
      super.writeLine(line)
      val _ = written.offer(line)

    def awaitWrite(bound: FiniteDuration = 5.seconds): String =
      timeout(bound)(Option(written.take()).getOrElse(""))

    def script(steps: (() => Unit)*): Unit = scripted.enqueueAll(steps)

    def runScript(): Unit =
      while scripted.nonEmpty do
        val _ = awaitWrite()
        scripted.dequeue()()

  private def respondInit(peer: TurnFake, id: Long = 1): Unit =
    peer.enqueueStdout(
      s"""{"jsonrpc":"2.0","id":$id,"result":{"protocolVersion":1}}"""
    )

  private def respondNew(
      peer: TurnFake,
      id: Long = 2,
      sessionId: String = "sess-a"
  ): Unit =
    peer.enqueueStdout(
      s"""{"jsonrpc":"2.0","id":$id,"result":{"sessionId":"$sessionId"}}"""
    )

  private def respondLoad(peer: TurnFake, id: Long): Unit =
    peer.enqueueStdout(s"""{"jsonrpc":"2.0","id":$id,"result":{}}""")

  private def respondPrompt(
      peer: TurnFake,
      id: Long,
      stop: String = "end_turn"
  ): Unit =
    peer.enqueueStdout(
      s"""{"jsonrpc":"2.0","id":$id,"result":{"stopReason":"$stop"}}"""
    )

  private def spec(
      prompt: String = "hello",
      sessionStart: AcpSessionStart = AcpSessionStart.Fresh,
      outputSchema: Option[String] = None
  ): AcpTurnSpec[BackendTag.Cursor.type] =
    AcpTurnSpec(
      cwd = "/work",
      mcpServers = Vector.empty,
      prompt = prompt,
      outputSchema = outputSchema,
      sessionStart = sessionStart,
      openingPrompt = None
    )

  private def policy(
      tools: ToolSet = ToolSet.Full,
      autoApprove: AutoApprove = AutoApprove.All
  ): AcpPermissionPolicy =
    AcpPermissionPolicy(tools, autoApprove)

  test("first fresh prompt streams assistant text with message boundaries"):
    val peer = TurnFake()
    supervised:
      forkDiscard:
        peer.script(
          () => respondInit(peer),
          () => respondNew(peer),
          () =>
            peer.enqueueStdout(AcpWireFixtures.agentChunk("sess-a", "answer"))
            respondPrompt(peer, id = 3)
        )
        peer.runScript()
      val live = AcpLiveTurn.start(
        peer,
        testConfig,
        spec(outputSchema = Some("""{"type":"object"}""")),
        policy()
      )
      assertEquals(live.outputSchema, Some("""{"type":"object"}"""))
      val events = live.events.toList
      assertEquals(
        events,
        List(
          TurnEvent.AssistantTextDelta("answer"),
          TurnEvent.AssistantMessageEnd
        )
      )
      TurnEventConformance.assertGrammar(events, completedNormally = true)
      val result = live.awaitResult().toOption.get
      assertEquals(result.output, "answer")
      assertEquals(
        result.wireId,
        WireSessionId[BackendTag.Cursor.type]("sess-a")
      )

  test("thought chunks are streamed but excluded from turn output"):
    val peer = TurnFake()
    supervised:
      forkDiscard:
        peer.script(
          () => respondInit(peer),
          () => respondNew(peer),
          () =>
            peer.enqueueStdout(
              """{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sess-a","update":{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"secret"}}}}"""
            )
            peer.enqueueStdout(AcpWireFixtures.agentChunk("sess-a", "answer"))
            respondPrompt(peer, id = 3)
        )
        peer.runScript()
      val live = AcpLiveTurn.start(peer, testConfig, spec(), policy())
      val events = live.events.toList
      assert(events.contains(TurnEvent.AssistantThinkingDelta("secret")))
      assertEquals(live.awaitResult().toOption.get.output, "answer")

  test("load replay before prompt is suppressed"):
    val peer = TurnFake()
    supervised:
      forkDiscard:
        peer.script(
          () => respondInit(peer),
          () =>
            peer.enqueueStdout(
              AcpWireFixtures.agentChunk("sess-a", "history-1")
            )
            peer.enqueueStdout(
              AcpWireFixtures.agentChunk("sess-a", "history-2")
            )
            respondLoad(peer, id = 2)
          ,
          () =>
            peer.enqueueStdout(AcpWireFixtures.agentChunk("sess-a", "live"))
            respondPrompt(peer, id = 3)
        )
        peer.runScript()
      val live = AcpLiveTurn.start(
        peer,
        testConfig,
        spec(sessionStart = AcpSessionStart.Load(AcpSessionId("sess-a"))),
        policy()
      )
      val events = live.events.toList
      assert(!events.exists:
        case TurnEvent.AssistantTextDelta(text) =>
          text == "history-1" || text == "history-2"
        case _ => false
      )
      TurnEventConformance.assertGrammar(events, completedNormally = true)
      val result = live.awaitResult().toOption.get
      assertEquals(result.output, "live")
      assertEquals(
        result.wireId,
        WireSessionId[BackendTag.Cursor.type]("sess-a")
      )

  test("trailing chunk before prompt result is included in output"):
    val peer = TurnFake()
    supervised:
      forkDiscard:
        peer.script(
          () => respondInit(peer),
          () => respondNew(peer),
          () =>
            peer.enqueueStdout(AcpWireFixtures.agentChunk("sess-a", "part-a "))
            peer.enqueueStdout(AcpWireFixtures.agentChunk("sess-a", "part-b"))
            respondPrompt(peer, id = 3)
        )
        peer.runScript()
      val live = AcpLiveTurn.start(peer, testConfig, spec(), policy())
      val _ = live.events.toList
      assertEquals(live.awaitResult().toOption.get.output, "part-a part-b")

  test("refusal stop reason fails the turn after prompt begins"):
    val peer = TurnFake()
    supervised:
      forkDiscard:
        peer.script(
          () => respondInit(peer),
          () => respondNew(peer),
          () => respondPrompt(peer, id = 3, stop = "refusal")
        )
        peer.runScript()
      val live = AcpLiveTurn.start(peer, testConfig, spec(), policy())
      val _ = live.events.toList
      val failed = intercept[AgentTurnFailed](live.awaitResult())
      assert(failed.getMessage.contains("refusal"))

  test("max_tokens stop reason fails the turn"):
    val peer = TurnFake()
    supervised:
      forkDiscard:
        peer.script(
          () => respondInit(peer),
          () => respondNew(peer),
          () => respondPrompt(peer, id = 3, stop = "max_tokens")
        )
        peer.runScript()
      val live = AcpLiveTurn.start(peer, testConfig, spec(), policy())
      val _ = live.events.toList
      val failed = intercept[AgentTurnFailed](live.awaitResult())
      assert(failed.getMessage.contains("max_tokens"))

  test("unknown stop reason fails the turn"):
    val peer = TurnFake()
    supervised:
      forkDiscard:
        peer.script(
          () => respondInit(peer),
          () => respondNew(peer),
          () => respondPrompt(peer, id = 3, stop = "future_stop")
        )
        peer.runScript()
      val live = AcpLiveTurn.start(peer, testConfig, spec(), policy())
      val _ = live.events.toList
      val failed = intercept[AgentTurnFailed](live.awaitResult())
      assert(failed.getMessage.contains("future_stop"))

  test("wire cancelled stop reason returns OrcaInteractiveCancelled"):
    val peer = TurnFake()
    supervised:
      forkDiscard:
        peer.script(
          () => respondInit(peer),
          () => respondNew(peer),
          () => respondPrompt(peer, id = 3, stop = "cancelled")
        )
        peer.runScript()
      val live = AcpLiveTurn.start(peer, testConfig, spec(), policy())
      val _ = live.events.toList
      live.awaitResult() match
        case Left(c: OrcaInteractiveCancelled) =>
          assertEquals(c.debit, TurnDebit.Unobserved)
        case other => fail(s"expected interactive cancel, got $other")

  test("interactive cancel returns OrcaInteractiveCancelled"):
    val peer = TurnFake()
    supervised:
      forkDiscard:
        peer.script(
          () => respondInit(peer),
          () => respondNew(peer),
          () => ()
        )
        peer.runScript()
      val live = AcpLiveTurn.start(peer, testConfig, spec(), policy())
      val waiter = fork(live.cancel())
      waiter.join()
      live.awaitResult() match
        case Left(c: OrcaInteractiveCancelled) =>
          assertEquals(c.debit, TurnDebit.Unobserved)
        case other => fail(s"expected interactive cancel, got $other")
      assert(peer.writes.exists(_.contains("session/cancel")))

  test("successful turn teardown does not send session cancel"):
    val peer = TurnFake()
    supervised:
      forkDiscard:
        peer.script(
          () => respondInit(peer),
          () => respondNew(peer),
          () => respondPrompt(peer, id = 3)
        )
        peer.runScript()
      val live = AcpLiveTurn.start(peer, testConfig, spec(), policy())
      val _ = live.events.toList
      assert(live.awaitResult().isRight)
      assertEquals(peer.writes.count(_.contains("session/cancel")), 0)
      assert(peer.isStdinClosed)

  test("cancel during pending permission without an approving consumer"):
    val peer = TurnFake()
    supervised:
      forkDiscard:
        peer.script(
          () => respondInit(peer),
          () => respondNew(peer),
          () =>
            peer.enqueueStdout(
              """{"jsonrpc":"2.0","id":9,"method":"session/request_permission","params":{"sessionId":"sess-a","toolCall":{"toolCallId":"c1","kind":"execute"},"options":[{"optionId":"once","name":"Allow","kind":"allow_once"}]}}"""
            )
        )
        peer.runScript()
      val live = AcpLiveTurn.start(peer, testConfig, spec(), policy())
      val event = timeout(5.seconds)(live.events.next())
      assert(event.isInstanceOf[TurnEvent.Approval])
      timeout(5.seconds)(live.cancel())
      live.awaitResult() match
        case Left(_: OrcaInteractiveCancelled) => ()
        case other => fail(s"expected cancel, got $other")

  test("tool start and completed update map to TurnEvents"):
    val peer = TurnFake()
    supervised:
      forkDiscard:
        peer.script(
          () => respondInit(peer),
          () => respondNew(peer),
          () =>
            peer.enqueueStdout(AcpWireFixtures.toolStarted("sess-a", "call-1"))
            peer.enqueueStdout(
              AcpWireFixtures.toolCompleted("sess-a", "call-1", "execute")
            )
            respondPrompt(peer, id = 3)
        )
        peer.runScript()
      val live = AcpLiveTurn.start(peer, testConfig, spec(), policy())
      val events = live.events.toList
      TurnEventConformance.assertGrammar(events, completedNormally = true)
      assert(events.contains(TurnEvent.AssistantToolCall("other", "")))
      assert(events.exists:
        case TurnEvent.ToolResult(Some("execute"), true, "[1,2]") => true
        case _                                                    => false
      )

  test("governable permission routes through approval closure"):
    val peer = TurnFake()
    supervised:
      forkDiscard:
        peer.script(
          () => respondInit(peer),
          () => respondNew(peer),
          () =>
            peer.enqueueStdout(
              """{"jsonrpc":"2.0","id":9,"method":"session/request_permission","params":{"sessionId":"sess-a","toolCall":{"toolCallId":"c1","kind":"execute"},"options":[{"optionId":"once","name":"Allow","kind":"allow_once"}]}}"""
            ),
          () => respondPrompt(peer, id = 3)
        )
        peer.runScript()
      val live = AcpLiveTurn.start(peer, testConfig, spec(), policy())
      val _ = fork:
        live.events.foreach:
          case TurnEvent.Approval(req) => req.respond(ApprovalDecision.Allow)
          case _                       => ()
      assert(live.awaitResult().isRight)
      timeout(500.millis):
        var ok = peer.writes.exists(_.contains("""outcome":"selected"""))
        while !ok do
          ox.sleep(5.millis)
          ok = peer.writes.exists(_.contains("""outcome":"selected"""))
        ()

  test("channel deny maps to cancelled permission outcome"):
    val peer = TurnFake()
    supervised:
      forkDiscard:
        peer.script(
          () => respondInit(peer),
          () => respondNew(peer),
          () =>
            peer.enqueueStdout(
              """{"jsonrpc":"2.0","id":9,"method":"session/request_permission","params":{"sessionId":"sess-a","toolCall":{"toolCallId":"c1","kind":"execute"},"options":[{"optionId":"once","name":"Allow","kind":"allow_once"}]}}"""
            ),
          () => respondPrompt(peer, id = 3)
        )
        peer.runScript()
      val live = AcpLiveTurn.start(peer, testConfig, spec(), policy())
      val _ = fork:
        live.events.foreach:
          case TurnEvent.Approval(req) => req.respond(ApprovalDecision.Deny)
          case _                       => ()
      assert(live.awaitResult().isRight)
      assert(peer.writes.exists(_.contains("""outcome":"cancelled""")))
      assert(!peer.writes.exists(_.contains("""outcome":"selected""")))

  test("non-full toolset denies permission without widening"):
    val peer = TurnFake()
    supervised:
      forkDiscard:
        peer.script(
          () => respondInit(peer),
          () => respondNew(peer),
          () =>
            peer.enqueueStdout(
              """{"jsonrpc":"2.0","id":9,"method":"session/request_permission","params":{"sessionId":"sess-a","toolCall":{"toolCallId":"c1","kind":"execute"},"options":[{"optionId":"once","name":"Allow","kind":"allow_once"}]}}"""
            ),
          () => respondPrompt(peer, id = 3)
        )
        peer.runScript()
      val live =
        AcpLiveTurn.start(
          peer,
          testConfig,
          spec(),
          policy(tools = ToolSet.ReadOnly)
        )
      val events = live.events.toList
      assert(!events.exists(_.isInstanceOf[TurnEvent.Approval]))
      assert(live.awaitResult().isRight)
      assert(peer.writes.exists(_.contains("""outcome":"cancelled""")))
