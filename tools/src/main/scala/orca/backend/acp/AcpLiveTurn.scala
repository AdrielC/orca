package orca.backend.acp

import orca.agents.{BackendTag, WireSessionId}
import orca.backend.{
  AgentResult,
  ApprovalDecision,
  ChannelEvent,
  LiveTurn,
  LiveTurnEvents,
  TurnEvent,
  TurnMessageGrammar
}
import orca.events.{TurnDebit, Usage}
import orca.subprocess.PipedCliProcess
import orca.{AgentTurnFailed, OrcaFlowException, OrcaInteractiveCancelled}

import AcpProtocol.*

import ox.{Ox, UnsupervisedFork, discard, forkUnsupervised}
import ox.channels.{Actor, ActorRef, Channel, ChannelClosed}

import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

private type AcpTurnOutcome[B <: BackendTag] =
  Either[OrcaInteractiveCancelled | AgentTurnFailed, AgentResult[B]]

private enum AcpTeardown:
  case Success, InteractiveCancel, Aborted

private final case class AcpTurnSettlement[B <: BackendTag](
    outcome: AcpTurnOutcome[B],
    teardown: AcpTeardown
)

/** A [[LiveTurn]] over the shared [[AcpClient]] for one prompt in a session. */
private[backend] object AcpLiveTurn:

  private[backend] val MaxOutputChars: Int = 4 * 1024 * 1024

  // `cancelled` is observed from `Live.cancel()` on an arbitrary thread while the
  // reader fork owns teardown; an actor cannot receive cross-thread cancel without
  // ask/tell races on the same ref.
  def start[B <: BackendTag](
      process: PipedCliProcess,
      config: AcpClientConfig,
      spec: AcpTurnSpec[B],
      policy: AcpPermissionPolicy
  )(using Ox): LiveTurn[B] =
    val events = Channel.buffered[TurnEvent](LiveTurnEvents.ChannelCapacity)
    val cancelled = new AtomicBoolean(false)
    val turnActor = Actor.create(AcpTurnPipe(events, policy))
    val permissionDecider = new AcpPermissionDecider:
      def decide(request: RequestPermissionRequest): PermissionOutcome =
        turnActor.ask(_.requestApproval(request)) match
          case None => policy.defaultOutcome
          case Some(gate) =>
            try
              gate.receiveOrClosed() match
                case decision: ApprovalDecision =>
                  policy.toOutcome(decision, request)
                case _ => policy.defaultOutcome
            finally
              gate.doneOrClosed().discard
              turnActor.tell(_.forgetApproval(gate))
    val client =
      AcpClient.start(
        process,
        config = config,
        permission = permissionDecider,
        onUpdate = notification => turnActor.ask(_.onUpdate(notification))
      )

    try
      client.initialize() match
        case Left(failure) => throwSetup(failure)
        case Right(_)      => ()
      val sessionId = openSession(client, spec)
      turnActor.tell(_.bindSession(sessionId))
      spec.openingPrompt.foreach(text =>
        events.sendOrClosed(TurnEvent.UserMessage(text)).discard
      )
      val reader =
        forkUnsupervised(
          runTurn[B](client, sessionId, spec.prompt, turnActor, cancelled)
        )
      Live(spec, client, sessionId, events, cancelled, turnActor, reader)
    catch
      case NonFatal(t) =>
        client.cleanup()
        throw t

  private def throwSetup(failure: AcpSetupFailure): Nothing =
    throw failure: OrcaFlowException

  private def openSession(
      client: AcpClient,
      spec: AcpTurnSpec[?]
  ): AcpSessionId =
    spec.sessionStart match
      case AcpSessionStart.Fresh =>
        client
          .newSession(NewSessionRequest(spec.cwd, spec.mcpServers))
          .fold(throwSetup, _.sessionId)
      case AcpSessionStart.Load(id) =>
        client
          .loadSession(LoadSessionRequest(id, spec.cwd, spec.mcpServers))
          .fold(throwSetup, _ => id)

  private def runTurn[B <: BackendTag](
      client: AcpClient,
      sessionId: AcpSessionId,
      prompt: String,
      turnActor: ActorRef[AcpTurnPipe],
      cancelled: AtomicBoolean
  ): AcpTurnSettlement[B] =
    var settlement: AcpTurnSettlement[B] = AcpTurnSettlement(
      Left(turnFailed(new IllegalStateException("uninitialized ACP turn"))),
      AcpTeardown.Aborted
    )
    try
      turnActor.ask(_.beginTurn())
      val request = PromptRequest(sessionId, Vector(ContentBlock.Text(prompt)))
      settlement = client.prompt(request) match
        case Left(_) if cancelled.get() =>
          AcpTurnSettlement(
            Left(new OrcaInteractiveCancelled(TurnDebit.Unobserved)),
            AcpTeardown.InteractiveCancel
          )
        case Left(failure) =>
          AcpTurnSettlement(Left(turnFailed(failure)), AcpTeardown.Aborted)
        case Right(response) =>
          turnActor.ask(_.finishTurn(response, cancelled))
    catch
      case f: AgentTurnFailed =>
        settlement = AcpTurnSettlement(Left(f), AcpTeardown.Aborted)
      case NonFatal(t) if cancelled.get() =>
        settlement = AcpTurnSettlement(
          Left(new OrcaInteractiveCancelled(TurnDebit.Unobserved, describe(t))),
          AcpTeardown.InteractiveCancel
        )
      case NonFatal(t) =>
        settlement = AcpTurnSettlement(Left(turnFailed(t)), AcpTeardown.Aborted)
    finally
      turnActor.tell(_.closeEvents())
      settlement.teardown match
        case AcpTeardown.Success           => client.completeSuccessfully()
        case AcpTeardown.InteractiveCancel => client.cancel(Some(sessionId))
        case AcpTeardown.Aborted           => client.cleanup()
    settlement

  private def turnFailed(failure: AcpSetupFailure): AgentTurnFailed =
    AgentTurnFailed(
      describeSetupFailure(failure),
      TurnDebit.Unobserved,
      failure
    )

  private def turnFailed(cause: Throwable): AgentTurnFailed =
    AgentTurnFailed(describe(cause), TurnDebit.Unobserved, cause)

  private def describeSetupFailure(failure: AcpSetupFailure): String =
    failure match
      case AcpSetupFailure.RpcFailure(rpc) => rpc.message
      case other                           => other.getMessage

  private def describe(t: Throwable): String =
    Option(t.getMessage).filter(_.nonEmpty).getOrElse(t.toString)

  private final class Live[B <: BackendTag](
      spec: AcpTurnSpec[B],
      client: AcpClient,
      sessionId: AcpSessionId,
      eventChannel: Channel[TurnEvent],
      cancelled: AtomicBoolean,
      turnActor: ActorRef[AcpTurnPipe],
      reader: UnsupervisedFork[AcpTurnSettlement[B]]
  ) extends LiveTurn[B]:
    def outputSchema: Option[String] = spec.outputSchema
    def canAskUser: Boolean = false

    def events: Iterator[TurnEvent] =
      Iterator
        .continually(eventChannel.receiveOrClosed())
        .takeWhile(!_.isInstanceOf[ChannelClosed])
        .collect { case e: TurnEvent => e }

    def awaitResult(): Either[OrcaInteractiveCancelled, AgentResult[B]] =
      reader.join().outcome match
        case Right(result)                     => Right(result)
        case Left(c: OrcaInteractiveCancelled) => Left(c)
        case Left(f: AgentTurnFailed)          => throw f

    def cancel(): Unit =
      cancelled.set(true)
      eventChannel.doneOrClosed().discard
      turnActor.tell(_.requestCancel())
      client.cancel(Some(sessionId))
      reader.join().discard

/** Actor-owned turn state: replay suppression, message grammar, permissions. */
private final class AcpTurnPipe(
    events: Channel[TurnEvent],
    policy: AcpPermissionPolicy
):
  private enum Phase:
    case SuppressReplay, InTurn

  private var phase: Phase = Phase.SuppressReplay
  private var sessionId: Option[AcpSessionId] = None
  private var activitySinceMessageEnd = false
  private val text = StringBuilder()
  private var startedTools = Set.empty[ToolCallId]
  private var userCancelled = false
  private var approvalGates = Set.empty[Channel[ApprovalDecision]]

  def bindSession(id: AcpSessionId): Unit = sessionId = Some(id)

  def beginTurn(): Unit =
    phase = Phase.InTurn
    activitySinceMessageEnd = false

  def requestCancel(): Unit =
    userCancelled = true
    closeApprovals()

  def closeEvents(): Unit =
    closeApprovals()
    if phase == Phase.InTurn then closeOpenMessage()
    events.doneOrClosed().discard

  def onUpdate(notification: SessionNotification): Unit =
    if phase != Phase.InTurn then return
    if !sessionId.contains(notification.sessionId) then return
    notification.update match
      case SessionUpdate.Chunk(ChunkKind.User, _) => ()
      case SessionUpdate.Chunk(ChunkKind.Thought, content) =>
        emitThinking(content)
      case SessionUpdate.Chunk(ChunkKind.Agent, content) =>
        emitAgentChunk(content)
      case SessionUpdate.ToolStarted(tool) =>
        // ACP may repeat tool_call start notifications for one toolCallId.
        if !startedTools(tool.toolCallId) then
          startedTools = startedTools + tool.toolCallId
          emit(
            TurnEvent.AssistantToolCall(toolKindName(tool), rawToolInput(tool))
          )
      case SessionUpdate.ToolUpdated(update) =>
        emitToolResult(update)
      case SessionUpdate.Unknown(_, _) => ()

  // The permission worker awaits the answer so the actor can still cancel it.
  def requestApproval(
      request: RequestPermissionRequest
  ): Option[Channel[ApprovalDecision]] =
    if phase != Phase.InTurn || userCancelled || !policy.governable ||
      !sessionId.contains(request.sessionId)
    then None
    else
      val gate = Channel.buffered[ApprovalDecision](1)
      approvalGates = approvalGates + gate
      emit(
        TurnEvent.Approval(
          ChannelEvent.ApproveTool(
            policy.toolName(request),
            policy.rawInput(request),
            decision => gate.sendOrClosed(decision).discard
          )
        )
      )
      Some(gate)

  def forgetApproval(gate: Channel[ApprovalDecision]): Unit =
    approvalGates = approvalGates - gate

  def finishTurn[B <: BackendTag](
      response: PromptResponse,
      cancelled: AtomicBoolean
  ): AcpTurnSettlement[B] =
    phase = Phase.SuppressReplay
    closeOpenMessage()
    val id = sessionId.getOrElse(
      throw new IllegalStateException("ACP session id unset before finishTurn")
    )
    val output = text.toString
    val wireId = WireSessionId[B](id.value)
    val result = AgentResult[B](wireId, output, Usage.empty, None)
    if cancelled.get() || userCancelled then
      AcpTurnSettlement(
        Left(new OrcaInteractiveCancelled(TurnDebit.Unobserved)),
        AcpTeardown.InteractiveCancel
      )
    else
      response.stopReason match
        case StopReason.EndTurn =>
          AcpTurnSettlement(Right(result), AcpTeardown.Success)
        case StopReason.Cancelled =>
          AcpTurnSettlement(
            Left(new OrcaInteractiveCancelled(TurnDebit.Unobserved)),
            AcpTeardown.InteractiveCancel
          )
        case StopReason.Refusal | StopReason.MaxTokens |
            StopReason.MaxTurnRequests =>
          AcpTurnSettlement(
            Left(
              new AgentTurnFailed(
                s"ACP turn ended: ${response.stopReason.wire}",
                TurnDebit.Unobserved
              )
            ),
            AcpTeardown.Aborted
          )
        case StopReason.Unknown(raw) =>
          AcpTurnSettlement(
            Left(
              new AgentTurnFailed(
                s"ACP turn ended with unknown stop reason: $raw",
                TurnDebit.Unobserved
              )
            ),
            AcpTeardown.Aborted
          )

  private def emitAgentChunk(content: ContentBlock): Unit =
    content match
      case ContentBlock.Text(value) if value.nonEmpty =>
        if text.length + value.length > AcpLiveTurn.MaxOutputChars then
          throw new AgentTurnFailed(
            s"ACP agent output exceeded ${AcpLiveTurn.MaxOutputChars} characters",
            TurnDebit.Unobserved
          )
        text.append(value)
        emit(TurnEvent.AssistantTextDelta(value))
      case _ => ()

  private def emitThinking(content: ContentBlock): Unit =
    content match
      case ContentBlock.Text(value) if value.nonEmpty =>
        emit(TurnEvent.AssistantThinkingDelta(value))
      case _ => ()

  private def emitToolResult(update: ToolCallUpdate): Unit =
    update.status match
      case Some(ToolStatus.Completed) | Some(ToolStatus.Failed) =>
        val ok = update.status.contains(ToolStatus.Completed)
        val name = update.kind.map(_.wire)
        val body = update.rawOutput.map(_.value).getOrElse("")
        emit(TurnEvent.ToolResult(name, ok = ok, body))
      case _ => ()

  private def toolKindName(tool: ToolCall): String = tool.kind.wire

  private def rawToolInput(tool: ToolCall): String =
    tool.rawInput.map(_.value).getOrElse("")

  private def emit(event: TurnEvent): Unit =
    events.sendOrClosed(event).discard
    activitySinceMessageEnd =
      TurnMessageGrammar.activityAfter(activitySinceMessageEnd, event)

  private def closeOpenMessage(): Unit =
    if activitySinceMessageEnd then emit(TurnEvent.AssistantMessageEnd)

  private def closeApprovals(): Unit =
    approvalGates.foreach(_.doneOrClosed().discard)
    approvalGates = Set.empty
