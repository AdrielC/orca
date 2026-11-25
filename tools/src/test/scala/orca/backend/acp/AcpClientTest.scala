package orca.backend.acp

import orca.subprocess.{FakePipedCliProcess, PipedCliDiagnostics}

import AcpProtocol.*

import ox.{Ox, fork, forkDiscard, supervised, timeout}

import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable.ListBuffer

import scala.concurrent.duration.*

class AcpClientTest extends munit.FunSuite:

  private def testConfig(
      extra: AcpClientConfig => AcpClientConfig = identity
  ): AcpClientConfig =
    extra(
      AcpClientConfig.forTests(
        requestTimeout = 2.seconds,
        exitTimeout = 50.millis
      )
    )

  private final class NotifyingFake(
      initiallyAlive: Boolean = true
  ) extends FakePipedCliProcess(initiallyAlive):
    private val written = LinkedBlockingQueue[String]()

    override def writeLine(line: String): Unit =
      super.writeLine(line)
      val _ = written.offer(line)

    def awaitWrite(bound: FiniteDuration = 5.seconds): String =
      timeout(bound)(Option(written.take()).getOrElse(""))

  private def respondInitialize(peer: NotifyingFake, id: Long = 1): Unit =
    peer.enqueueStdout(
      s"""{"jsonrpc":"2.0","id":$id,"result":{"protocolVersion":1}}"""
    )

  private def respondNew(
      peer: NotifyingFake,
      id: Long = 2,
      sessionId: String = "sess-a"
  ): Unit =
    peer.enqueueStdout(
      s"""{"jsonrpc":"2.0","id":$id,"result":{"sessionId":"$sessionId"}}"""
    )

  private def respondLoad(peer: NotifyingFake, id: Long): Unit =
    peer.enqueueStdout(s"""{"jsonrpc":"2.0","id":$id,"result":{}}""")

  private def withClient[T](
      peer: NotifyingFake,
      config: AcpClientConfig = testConfig()
  )(body: Ox ?=> AcpClient => T): T =
    supervised:
      val client = AcpClient.start(peer, config = config)
      try body(client)
      finally client.cleanup()

  test("a prompt can finish after the setup request deadline"):
    val peer = NotifyingFake()
    val config = testConfig(_.copy(requestTimeout = 25.millis))
    withClient(peer, config): client =>
      forkDiscard:
        val _ = peer.awaitWrite()
        ox.sleep(100.millis)
        peer.enqueueStdout(
          """{"jsonrpc":"2.0","id":1,"result":{"stopReason":"end_turn"}}"""
        )
      val request =
        PromptRequest(
          AcpSessionId("sess-a"),
          Vector(ContentBlock.Text("hello"))
        )
      assertEquals(
        timeout(2.seconds)(client.prompt(request)),
        Right(PromptResponse(StopReason.EndTurn))
      )

  test("in-flight setup calls correlate when replies arrive in reverse order"):
    val peer = NotifyingFake()
    supervised:
      val client = AcpClient.start(peer, config = testConfig())
      try
        val initialized = fork(client.initialize())
        val _ = peer.awaitWrite()
        val created = fork(
          client.newSession(NewSessionRequest("/work", Vector.empty))
        )
        val _ = peer.awaitWrite()
        respondNew(peer)
        respondInitialize(peer)
        assert(initialized.join().isRight)
        assert(
          created.join().map(_.sessionId).contains(AcpSessionId("sess-a"))
        )
      finally client.cleanup()

  test("loadSession completes after initialize"):
    val peer = NotifyingFake()
    withClient(peer): client =>
      forkDiscard:
        val _ = peer.awaitWrite()
        respondInitialize(peer)
        val _ = peer.awaitWrite()
        respondLoad(peer, id = 2)
      assert(client.initialize().isRight)
      assert(
        client
          .loadSession(
            LoadSessionRequest(AcpSessionId("sess-a"), "/work", Vector.empty)
          )
          .isRight
      )

  test(
    "interleaved permission requests are answered without completing pending"
  ):
    val peer = NotifyingFake()
    val decided = java.util.concurrent.atomic.AtomicInteger(0)
    val permission = new AcpPermissionDecider:
      def decide(request: RequestPermissionRequest): PermissionOutcome =
        decided.incrementAndGet()
        PermissionOutcome.Cancelled
    supervised:
      val client =
        AcpClient.start(peer, config = testConfig(), permission = permission)
      try
        forkDiscard:
          val _ = peer.awaitWrite()
          peer.enqueueStdout(
            """{"jsonrpc":"2.0","id":5,"method":"session/request_permission","params":{"sessionId":"sess-a","toolCall":{"toolCallId":"call-1"},"options":[]}}"""
          )
          respondInitialize(peer)
        assert(client.initialize().isRight)
        assertEquals(decided.get(), 1)
        val _ = peer.awaitWrite()
        assertEquals(peer.writes.size, 2)
        assert(peer.writes.last.contains("""outcome":"cancelled"""))
      finally client.cleanup()

  test("permission approval encodes a selected option on the wire"):
    val peer = NotifyingFake()
    val permission = new AcpPermissionDecider:
      def decide(request: RequestPermissionRequest): PermissionOutcome =
        PermissionOutcome.Selected(PermissionOptionId("once"))
    supervised:
      val client =
        AcpClient.start(peer, config = testConfig(), permission = permission)
      try
        forkDiscard:
          val _ = peer.awaitWrite()
          peer.enqueueStdout(
            """{"jsonrpc":"2.0","id":5,"method":"session/request_permission","params":{"sessionId":"sess-a","toolCall":{"toolCallId":"call-1"},"options":[{"optionId":"once","name":"Allow","kind":"allow_once"}]}}"""
          )
          respondInitialize(peer)
        assert(client.initialize().isRight)
        assert(peer.writes.exists(_.contains("""outcome":"selected""")))
        assert(peer.writes.exists(_.contains("""optionId":"once""")))
      finally client.cleanup()

  test("unsupported blocking server requests receive method-not-found replies"):
    val peer = NotifyingFake()
    withClient(peer): client =>
      forkDiscard:
        val _ = peer.awaitWrite()
        peer.enqueueStdout(
          """{"jsonrpc":"2.0","id":"q","method":"cursor/ask_question","params":{}}"""
        )
        respondInitialize(peer)
      assert(client.initialize().isRight)
      assert(peer.writes.exists(_.contains("""-32601""")))

  test("session updates reach the onUpdate handler"):
    val peer = NotifyingFake()
    val updates = ListBuffer.empty[SessionNotification]
    supervised:
      val client = AcpClient.start(
        peer,
        config = testConfig(),
        onUpdate = updates.addOne
      )
      try
        forkDiscard:
          val _ = peer.awaitWrite()
          peer.enqueueStdout(
            """{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sess-a","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"hi"}}}}"""
          )
          respondInitialize(peer)
        assert(client.initialize().isRight)
        assertEquals(updates.size, 1)
        assertEquals(updates.head.sessionId, AcpSessionId("sess-a"))
      finally client.cleanup()

  test("setup rpc errors surface as AcpSetupFailure"):
    val peer = NotifyingFake()
    withClient(peer): client =>
      forkDiscard:
        val _ = peer.awaitWrite()
        peer.enqueueStdout(
          """{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"Auth required"}}"""
        )
      client.initialize() match
        case Left(AcpSetupFailure.RpcFailure(error)) =>
          assert(error.message.contains("Auth required"))
        case other => fail(s"expected rpc failure, got $other")

  test("eof before every reply is a setup failure"):
    val peer = NotifyingFake()
    withClient(peer): client =>
      forkDiscard:
        val _ = peer.awaitWrite()
        peer.closeStdout()
      client.initialize() match
        case Left(AcpSetupFailure.StdoutClosedEarly) => ()
        case other => fail(s"expected eof, got $other")

  test("new rpc after stdout eof fails immediately"):
    val peer = NotifyingFake()
    withClient(peer): client =>
      forkDiscard:
        val _ = peer.awaitWrite()
        peer.closeStdout()
      val _ = client.initialize()
      client.newSession(NewSessionRequest("/work", Vector.empty)) match
        case Left(AcpSetupFailure.StdoutClosed) => ()
        case other => fail(s"expected fast reject, got $other")

  test("request timeout fails and cancel remains responsive"):
    val peer = NotifyingFake()
    supervised:
      val client =
        AcpClient.start(
          peer,
          config = testConfig(_.copy(requestTimeout = 100.millis))
        )
      try
        forkDiscard:
          val _ = peer.awaitWrite()
        client.initialize() match
          case Left(AcpSetupFailure.RequestTimedOut(_)) => ()
          case other => fail(s"expected timeout, got $other")
        client.cancel(None)
        assert(peer.isStdinClosed)
        client.initialize() match
          case Left(
                AcpSetupFailure.ProtocolFailed | AcpSetupFailure.StdoutClosed
              ) =>
            ()
          case other => fail(s"expected terminal client, got $other")
      finally client.cleanup()

  test("malformed stdout during setup fails remaining calls"):
    val peer = NotifyingFake()
    withClient(peer): client =>
      forkDiscard:
        val _ = peer.awaitWrite()
        peer.enqueueStdout("not-json")
      client.initialize() match
        case Left(AcpSetupFailure.DecodeFailure(msg)) =>
          assert(msg.contains("Invalid ACP JSON"))
        case other => fail(s"expected decode failure, got $other")
      client.newSession(NewSessionRequest("/work", Vector.empty)) match
        case Left(AcpSetupFailure.ProtocolFailed) => ()
        case other => fail(s"expected protocol failed, got $other")

  test("stdin write failure rejects further rpcs"):
    val peer = NotifyingFake()
    supervised:
      val client = AcpClient.start(peer, config = testConfig())
      try
        peer.closeStdin()
        client.initialize() match
          case Left(AcpSetupFailure.StdinWriteFailed(_)) => ()
          case other => fail(s"expected stdin failure, got $other")
        client.newSession(NewSessionRequest("/work", Vector.empty)) match
          case Left(AcpSetupFailure.ProtocolFailed) => ()
          case other => fail(s"expected protocol failed, got $other")
      finally client.cleanup()

  test("stderr pressure does not block stdout handling"):
    val peer = NotifyingFake()
    withClient(peer): client =>
      forkDiscard:
        val _ = peer.awaitWrite()
        (1 to 200).foreach(i => peer.enqueueStderr(s"noise-$i"))
        respondInitialize(peer)
        peer.closeStderr()
      assert(client.initialize().isRight)
      assertEquals(
        client.stderrTail.size,
        PipedCliDiagnostics.StderrTailCapacity
      )

  test("completeSuccessfully closes stdin and exits before cleanup"):
    val peer = NotifyingFake()
    supervised:
      val client = AcpClient.start(peer, config = testConfig())
      try
        forkDiscard:
          val _ = peer.awaitWrite()
          respondInitialize(peer)
        assert(client.initialize().isRight)
        client.completeSuccessfully()
        assert(peer.isStdinClosed)
        assert(!peer.isAlive)
        client.cleanup()
        client.cleanup()
      finally client.cleanup()

  test("explicit cancel does not close stdin after successful completion"):
    val peer = NotifyingFake()
    supervised:
      val client = AcpClient.start(peer, config = testConfig())
      try
        forkDiscard:
          val _ = peer.awaitWrite()
          respondInitialize(peer)
        assert(client.initialize().isRight)
        client.completeSuccessfully()
        assert(peer.isStdinClosed)
        client.cancel(Some(AcpSessionId("sess-a")))
        assertEquals(peer.writes.count(_.contains("session/cancel")), 0)
      finally client.cleanup()

  test("cancel sends session cancel before teardown when not completed"):
    val peer = NotifyingFake()
    supervised:
      val client = AcpClient.start(peer, config = testConfig())
      try
        forkDiscard:
          val _ = peer.awaitWrite()
        client.cancel(Some(AcpSessionId("sess-a")))
        assert(peer.writes.exists(_.contains("session/cancel")))
        assert(peer.isStdinClosed)
      finally client.cleanup()

  test("capability validation rejects unsupported protocol versions"):
    val peer = NotifyingFake()
    withClient(peer): client =>
      forkDiscard:
        val _ = peer.awaitWrite()
        peer.enqueueStdout(
          """{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":2}}"""
        )
      client.initialize() match
        case Left(AcpSetupFailure.CapabilityUnsupported(msg)) =>
          assert(msg.contains("protocol version"))
        case other => fail(s"expected capability rejection, got $other")

  test("capability validation rejects agents missing loadSession"):
    val peer = NotifyingFake()
    val config = testConfig(
      _.copy(
        capabilityRequirements = AcpCapabilityRequirements(
          protocolVersion = AcpProtocol.Version,
          requireLoadSession = true,
          requireSessionList = false
        )
      )
    )
    withClient(peer, config): client =>
      forkDiscard:
        val _ = peer.awaitWrite()
        respondInitialize(peer)
      client.initialize() match
        case Left(AcpSetupFailure.CapabilityUnsupported(msg)) =>
          assert(msg.contains("loadSession"))
        case other => fail(s"expected loadSession rejection, got $other")

  test("capability validation rejects agents missing session list"):
    val peer = NotifyingFake()
    val config = testConfig(
      _.copy(
        capabilityRequirements = AcpCapabilityRequirements(
          protocolVersion = AcpProtocol.Version,
          requireLoadSession = false,
          requireSessionList = true
        )
      )
    )
    withClient(peer, config): client =>
      forkDiscard:
        val _ = peer.awaitWrite()
        respondInitialize(peer)
      client.initialize() match
        case Left(AcpSetupFailure.CapabilityUnsupported(msg)) =>
          assert(msg.contains("session list"))
        case other => fail(s"expected session list rejection, got $other")

  test("blocked stdout read unblocks on cleanup"):
    val peer = NotifyingFake()
    supervised:
      val client = AcpClient.start(peer, config = testConfig())
      val waiter = fork(client.initialize())
      val _ = peer.awaitWrite()
      client.cleanup()
      waiter.join() match
        case Left(AcpSetupFailure.CleanedUp) => ()
        case other => fail(s"expected cleanup failure, got $other")
