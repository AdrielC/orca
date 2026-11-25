package orca.backend.acp

import orca.json.RawJson

import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec

import AcpProtocol.*

private[backend] enum AcpResult:
  case Initialized(value: InitializeResponse)
  case SessionCreated(value: NewSessionResponse)
  case SessionLoaded(value: LoadSessionResponse)
  case SessionsListed(value: ListSessionsResponse)
  case PromptCompleted(value: PromptResponse)

private[backend] enum AcpInbound:
  case Completed(
      id: RequestId,
      method: AcpMethod,
      result: Either[RpcError, AcpResult]
  )
  case PermissionRequested(id: RequestId, request: RequestPermissionRequest)
  case Updated(notification: SessionNotification)
  case Reply(response: RpcMessage.Response)
  case Ignored(notification: RpcMessage.Notification)
  case UnexpectedResponse(id: RequestId)
  case InvalidResponse(id: RequestId, error: AcpProtocolException)
  case Malformed(error: AcpProtocolException)

/** Pure connection-local correlation. The transport owns serialization and
  * writes. Malformed frames and invalid responses are terminal protocol
  * failures for the caller; they must not be silently dropped while a request
  * is waiting. Unknown notifications and unsolicited/duplicate responses leave
  * pending calls intact.
  */
private[backend] final case class AcpRouter private (
    pending: Map[RequestId, AcpMethod]
):
  def expect(
      id: RequestId,
      method: AcpMethod
  ): Either[AcpProtocolException, AcpRouter] =
    if id == RequestId.Null then
      Left(new AcpProtocolException("Cannot correlate a null request id"))
    else if pending.contains(id) then
      Left(new AcpProtocolException(s"Duplicate pending ACP id: $id"))
    else
      method match
        case AcpMethod.Initialize | AcpMethod.NewSession |
            AcpMethod.LoadSession | AcpMethod.ListSessions | AcpMethod.Prompt =>
          Right(copy(pending = pending.updated(id, method)))
        case _ =>
          Left(
            new AcpProtocolException(
              s"Not an ACP client request: ${method.wire}"
            )
          )

  def receive(line: String): (AcpRouter, AcpInbound) =
    JsonRpc.decode(line) match
      case Left(error)    => (this, AcpInbound.Malformed(error))
      case Right(message) => route(message)

  private def route(message: RpcMessage): (AcpRouter, AcpInbound) =
    message match
      case RpcMessage.Response(id, result) =>
        pending.get(id) match
          case None => (this, AcpInbound.UnexpectedResponse(id))
          case Some(method) =>
            val next = copy(pending = pending - id)
            result match
              case Left(error) =>
                (next, AcpInbound.Completed(id, method, Left(error)))
              case Right(raw) =>
                decodeResult(method, raw) match
                  case Right(value) =>
                    (next, AcpInbound.Completed(id, method, Right(value)))
                  case Left(error) =>
                    (next, AcpInbound.InvalidResponse(id, error))
      case RpcMessage.Request(id, AcpMethod.RequestPermission, params) =>
        readParams[RequestPermissionRequest](params) match
          case Right(value) => (this, AcpInbound.PermissionRequested(id, value))
          case Left(_) =>
            (
              this,
              AcpInbound.Reply(
                RpcMessage.Response(id, Left(RpcError.invalidParams))
              )
            )
      case RpcMessage.Request(id, method, _) =>
        (
          this,
          AcpInbound.Reply(
            RpcMessage.Response(id, Left(RpcError.methodNotFound(method)))
          )
        )
      case RpcMessage.Notification(AcpMethod.Update, params) =>
        readParams[SessionNotification](params) match
          case Right(value) => (this, AcpInbound.Updated(value))
          case Left(error)  => (this, AcpInbound.Malformed(error))
      case notification: RpcMessage.Notification =>
        (this, AcpInbound.Ignored(notification))

  private def readParams[A: JsonValueCodec](
      params: Option[RawJson]
  ): Either[AcpProtocolException, A] =
    params match
      case Some(raw) => WireJson.readObject[A](raw)
      case None      => Left(new AcpProtocolException("Missing ACP params"))

  private def decodeResult(
      method: AcpMethod,
      raw: RawJson
  ): Either[AcpProtocolException, AcpResult] =
    method match
      case AcpMethod.Initialize =>
        WireJson
          .readObject[InitializeResponse](raw)
          .map(AcpResult.Initialized.apply)
      case AcpMethod.NewSession =>
        WireJson
          .readObject[NewSessionResponse](raw)
          .map(AcpResult.SessionCreated.apply)
      case AcpMethod.LoadSession =>
        WireJson
          .readObject[LoadSessionResponse](raw)
          .map(AcpResult.SessionLoaded.apply)
      case AcpMethod.ListSessions =>
        WireJson
          .readObject[ListSessionsResponse](raw)
          .map(
            AcpResult.SessionsListed.apply
          )
      case AcpMethod.Prompt =>
        WireJson
          .readObject[PromptResponse](raw)
          .map(AcpResult.PromptCompleted.apply)
      case _ =>
        Left(
          new AcpProtocolException(s"Not an ACP result method: ${method.wire}")
        )

private[backend] object AcpRouter:
  def empty: AcpRouter = AcpRouter(Map.empty)
