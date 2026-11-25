package orca.backend.acp

private[backend] enum AcpMethod(val wire: String):
  case Initialize extends AcpMethod("initialize")
  case NewSession extends AcpMethod("session/new")
  case LoadSession extends AcpMethod("session/load")
  case ListSessions extends AcpMethod("session/list")
  case Prompt extends AcpMethod("session/prompt")
  case Cancel extends AcpMethod("session/cancel")
  case Update extends AcpMethod("session/update")
  case RequestPermission extends AcpMethod("session/request_permission")
  case Unknown(raw: String) extends AcpMethod(raw)

private[backend] object AcpMethod:
  private val byWire: Map[String, AcpMethod] = Vector(
    Initialize,
    NewSession,
    LoadSession,
    ListSessions,
    Prompt,
    Cancel,
    Update,
    RequestPermission
  ).map(method => method.wire -> method).toMap

  def parse(raw: String): AcpMethod = byWire.getOrElse(raw, Unknown(raw))
