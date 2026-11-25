package orca.backend.acp

import orca.subprocess.{PipedCliDiagnostics, PipedCliProcess}

import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec

import AcpProtocol.*

import ox.{
  Fork,
  Ox,
  UnsupervisedFork,
  discard,
  fork,
  forkDiscard,
  forkUnsupervised,
  sleep,
  timeout
}
import ox.channels.{Actor, ActorRef, Channel}

import java.util.concurrent.atomic.AtomicReference

import scala.util.control.NonFatal

/** Process-per-turn ACP transport: one stdout reader, serialized stdin writes,
  * concurrent stderr draining, and Ox-owned forks.
  */
private[backend] final class AcpClient private (
    config: AcpClientConfig,
    connection: ActorRef[AcpConnection],
    process: PipedCliProcess,
    reader: UnsupervisedFork[Unit],
    stderr: Fork[Unit],
    stderrSnapshot: AtomicReference[Vector[String]]
):
  def initialize(): Either[AcpSetupFailure, InitializeResponse] =
    call(AcpMethod.Initialize, config.initialize).flatMap:
      case AcpResult.Initialized(value) =>
        config.capabilityRequirements.validate(value)
      case other =>
        Left(
          AcpSetupFailure.UnexpectedResult(AcpMethod.Initialize, other.toString)
        )

  def newSession(
      request: NewSessionRequest
  ): Either[AcpSetupFailure, NewSessionResponse] =
    call(AcpMethod.NewSession, request).flatMap:
      case AcpResult.SessionCreated(value) => Right(value)
      case other =>
        Left(
          AcpSetupFailure.UnexpectedResult(AcpMethod.NewSession, other.toString)
        )

  def loadSession(
      request: LoadSessionRequest
  ): Either[AcpSetupFailure, LoadSessionResponse] =
    call(AcpMethod.LoadSession, request).flatMap:
      case AcpResult.SessionLoaded(value) => Right(value)
      case other =>
        Left(
          AcpSetupFailure.UnexpectedResult(
            AcpMethod.LoadSession,
            other.toString
          )
        )

  /** Graceful teardown after a successful turn: close stdin and wait briefly
    * for the process to exit before a forcible tree kill.
    */
  def completeSuccessfully(): Unit =
    connection.ask(_.markCompletedSuccessfully())
    waitForProcessExit()

  /** Explicit cancellation while a turn may still be active. Does not send a
    * synthetic cancel after [[completeSuccessfully]].
    */
  def cancel(sessionId: Option[AcpSessionId] = None): Unit =
    connection.ask(_.markCancelled(sessionId))
    waitForProcessExit()

  /** Idempotent process teardown; safe from `finally` after success or failure.
    */
  def cleanup(): Unit =
    connection.ask(_.markCleanedUp())
    try reader.join()
    catch case NonFatal(_) => ()
    try stderr.join()
    catch case NonFatal(_) => ()

  /** Non-blocking snapshot of the capped stderr tail collected so far. Does not
    * wait for stderr EOF; call before [[cleanup]] if you need lines after the
    * process has finished draining.
    */
  def stderrTail: Vector[String] = stderrSnapshot.get()

  private def waitForProcessExit(): Unit =
    val started = config.nowNanos()
    val limit = config.exitTimeout.toNanos
    while process.tryExitCode.isEmpty &&
      config.nowNanos() - started < limit
    do sleep(config.exitPollInterval)
    if process.tryExitCode.isEmpty then connection.ask(_.destroyProcess())

  private def call[A: JsonValueCodec](
      method: AcpMethod,
      params: A
  ): Either[AcpSetupFailure, AcpResult] =
    val wait = connection.ask(_.beginRequest(method, params))
    try timeout(config.requestTimeout)(wait.receive())
    catch
      case _: java.util.concurrent.TimeoutException =>
        connection.tell(_.abandonRequest(wait))
        Left(AcpSetupFailure.RequestTimedOut(method))
      case NonFatal(error) =>
        connection.tell(_.abandonRequest(wait))
        Left(AcpSetupFailure.WaitFailed(error.getMessage))

private[backend] object AcpClient:
  def start(
      process: PipedCliProcess,
      config: AcpClientConfig,
      permission: AcpPermissionDecider = AcpPermissionDecider.defaultCancelled,
      onUpdate: SessionNotification => Unit = _ => ()
  )(using Ox): AcpClient =
    val stderrSnapshot = AtomicReference(Vector.empty[String])
    val stderr = fork(drainStderr(process, stderrSnapshot))
    val updates = Channel.buffered[SessionNotification](256)
    forkDiscard:
      try
        Iterator
          .continually(updates.receiveOrClosed())
          .takeWhile(!_.isInstanceOf[ox.channels.ChannelClosed])
          .foreach:
            case n: SessionNotification => onUpdate(n)
            case _                      => ()
      catch case NonFatal(_) => ()
    val pushUpdate: SessionNotification => Unit =
      n => updates.sendOrClosed(n).discard
    val actor =
      Actor.create(AcpConnection(process, permission, pushUpdate))
    val reader = forkUnsupervised(readStdout(process, actor))
    new AcpClient(
      config,
      actor,
      process,
      reader,
      stderr,
      stderrSnapshot
    )

  private def drainStderr(
      process: PipedCliProcess,
      snapshot: AtomicReference[Vector[String]]
  ): Unit =
    val capacity = PipedCliDiagnostics.StderrTailCapacity
    var tail = Vector.empty[String]
    try
      var kept = 0
      for line <- process.stderrLines do
        if kept < capacity then
          tail = (tail :+ line).takeRight(capacity)
          kept += 1
        snapshot.set(tail)
    catch case NonFatal(_) => ()
    snapshot.set(tail)

  private def readStdout(
      process: PipedCliProcess,
      connection: ActorRef[AcpConnection]
  ): Unit =
    try for line <- process.stdoutLines do connection.ask(_.onLine(line))
    catch case NonFatal(e) => connection.ask(_.onReaderFailed(e))
    finally connection.ask(_.onReaderEof())

private enum ConnectionPhase:
  case Active, ProtocolFailed, Completed, Cancelled, CleanedUp

/** Actor-owned router, pending replies, and stdin serialization. */
private final class AcpConnection(
    process: PipedCliProcess,
    permission: AcpPermissionDecider,
    pushUpdate: SessionNotification => Unit
):
  private var router: AcpRouter = AcpRouter.empty
  private var nextId: Long = 1L
  private var pending =
    Map.empty[RequestId, Channel[Either[AcpSetupFailure, AcpResult]]]
  private var phase: ConnectionPhase = ConnectionPhase.Active
  private var readerEof = false

  private def acceptsInbound: Boolean = phase == ConnectionPhase.Active
  private def acceptsOutbound: Boolean =
    phase == ConnectionPhase.Active && !readerEof

  def beginRequest[A: JsonValueCodec](
      method: AcpMethod,
      params: A
  ): Channel[Either[AcpSetupFailure, AcpResult]] =
    val reply = Channel.buffered[Either[AcpSetupFailure, AcpResult]](1)
    val early: Either[AcpSetupFailure, Unit] =
      if phase == ConnectionPhase.ProtocolFailed then
        Left(AcpSetupFailure.ProtocolFailed)
      else if readerEof || !acceptsOutbound then
        Left(AcpSetupFailure.StdoutClosed)
      else if phase != ConnectionPhase.Active then
        Left(AcpSetupFailure.ProtocolFailed)
      else Right(())
    early match
      case Left(failure) =>
        reply.sendOrClosed(Left(failure)).discard
        reply.doneOrClosed().discard
        reply
      case Right(_) =>
        val id = RequestId.Number(nextId)
        nextId += 1
        router.expect(id, method) match
          case Left(error) =>
            reply
              .sendOrClosed(
                Left(AcpSetupFailure.DecodeFailure(error.getMessage))
              )
              .discard
            reply.doneOrClosed().discard
            reply
          case Right(next) =>
            router = next
            pending = pending.updated(id, reply)
            try process.writeLine(AcpRpc.request(id, method, params))
            catch
              case e: java.io.IOException =>
                pending = pending - id
                router = router.abandon(id)
                val failure = AcpSetupFailure.StdinWriteFailed(e.getMessage)
                reply.sendOrClosed(Left(failure)).discard
                reply.doneOrClosed().discard
                latchProtocolFailed()
            reply

  def abandonRequest(
      wait: Channel[Either[AcpSetupFailure, AcpResult]]
  ): Unit =
    pending
      .collectFirst { case (id, ch) if ch eq wait => id }
      .foreach: id =>
        pending = pending - id
        router = router.abandon(id)
    wait.doneOrClosed().discard
    latchProtocolFailed()

  def onLine(line: String): Unit =
    if !acceptsInbound then return
    val (next, inbound) = router.receive(line)
    router = next
    inbound match
      case AcpInbound.Completed(id, _, result) =>
        pending
          .get(id)
          .foreach: reply =>
            val mapped =
              result.left.map(e => AcpSetupFailure.RpcFailure(e))
            reply.sendOrClosed(mapped).discard
            reply.doneOrClosed().discard
        pending = pending - id
      case AcpInbound.PermissionRequested(id, request) =>
        val outcome = normalizedOutcome(request, permission.decide(request))
        replyPermission(id, outcome)
      case AcpInbound.Reply(response) =>
        writeSafely(AcpRpc.encodeResponse(response))
      case AcpInbound.Updated(notification) =>
        pushUpdate(notification)
      case AcpInbound.Ignored(_)            => ()
      case AcpInbound.UnexpectedResponse(_) => ()
      case AcpInbound.Malformed(error) =>
        latchProtocolFailed(AcpSetupFailure.DecodeFailure(error.getMessage))
      case AcpInbound.InvalidResponse(id, error) =>
        val failure = AcpSetupFailure.DecodeFailure(error.getMessage)
        pending
          .get(id)
          .foreach: reply =>
            reply.sendOrClosed(Left(failure)).discard
            reply.doneOrClosed().discard
        pending = pending - id
        // Fail globally only when stdout ended and nothing is still pending.
        if pending.isEmpty && readerEof then latchProtocolFailed(failure)

  def onReaderEof(): Unit =
    readerEof = true
    if pending.nonEmpty then failAllPending(AcpSetupFailure.StdoutClosedEarly)

  def onReaderFailed(cause: Throwable): Unit =
    latchProtocolFailed(AcpSetupFailure.DecodeFailure(cause.getMessage))

  def markCompletedSuccessfully(): Unit =
    if phase != ConnectionPhase.Active then return
    phase = ConnectionPhase.Completed
    closeStdin()

  def markCancelled(sessionId: Option[AcpSessionId]): Unit =
    phase match
      case ConnectionPhase.CleanedUp | ConnectionPhase.Completed => return
      case ConnectionPhase.Cancelled => closeStdin(); return
      case _                         => phase = ConnectionPhase.Cancelled
    sessionId.foreach: id =>
      writeSafely(AcpRpc.notification(AcpMethod.Cancel, CancelNotification(id)))
    closeStdin()

  def markCleanedUp(): Unit =
    if phase == ConnectionPhase.CleanedUp then return
    phase = ConnectionPhase.CleanedUp
    closeStdin()
    destroyProcess()
    failAllPending(AcpSetupFailure.CleanedUp)

  def destroyProcess(): Unit =
    if process.tryExitCode.isEmpty then process.destroyForciblyTree()

  private def normalizedOutcome(
      request: RequestPermissionRequest,
      outcome: PermissionOutcome
  ): PermissionOutcome =
    outcome match
      case PermissionOutcome.Selected(optionId)
          if !request.options.exists(_.optionId == optionId) =>
        PermissionOutcome.Cancelled
      case other => other

  private def replyPermission(
      id: RequestId,
      outcome: PermissionOutcome
  ): Unit =
    val body = RequestPermissionResponse(outcome)
    val frame: RpcMessage.Response =
      RpcMessage.Response(id, Right(WireJson.write(body)))
    writeSafely(AcpRpc.encodeResponse(frame))

  private def writeSafely(line: String): Unit =
    if phase == ConnectionPhase.ProtocolFailed then return
    if phase != ConnectionPhase.Active && phase != ConnectionPhase.Cancelled
    then return
    try process.writeLine(line)
    catch
      case _: java.io.IOException =>
        latchProtocolFailed(AcpSetupFailure.StdinClosed)

  private def closeStdin(): Unit =
    try process.closeStdin()
    catch case NonFatal(_) => ()

  private def latchProtocolFailed(
      failure: AcpSetupFailure = AcpSetupFailure.ProtocolFailed
  ): Unit =
    if phase == ConnectionPhase.ProtocolFailed then return
    phase = ConnectionPhase.ProtocolFailed
    failAllPending(failure)

  private def failAllPending(error: AcpSetupFailure): Unit =
    val draining = pending
    pending = Map.empty
    draining.values.foreach: reply =>
      reply.sendOrClosed(Left(error)).discard
      reply.doneOrClosed().discard
