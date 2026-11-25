package orca.backend.acp

import orca.agents.{AutoApprove, ToolSet}
import orca.backend.ApprovalDecision

import AcpProtocol.*

/** Constrains interactive approval decisions before they are honored on the
  * wire. Interactive approval is only available on [[ToolSet.Full]] turns;
  * every other tier fails closed.
  */
private[backend] final class AcpPermissionPolicy(
    tools: ToolSet,
    autoApprove: AutoApprove
):
  def governable: Boolean = tools == ToolSet.Full

  /** Tool identity for policy checks — the wire kind, not a display title. */
  def toolName(request: RequestPermissionRequest): String =
    request.toolCall.kind.getOrElse(ToolKind.Other).wire

  def rawInput(request: RequestPermissionRequest): String =
    request.toolCall.rawInput.map(_.value).getOrElse("")

  /** Maps a channel Allow/Deny into an ACP outcome only when [[ToolSet.Full]]
    * and [[autoApprove]] permit the tool; otherwise the wire answer stays
    * cancelled.
    */
  def toOutcome(
      decision: ApprovalDecision,
      request: RequestPermissionRequest
  ): PermissionOutcome =
    val allowed = decision match
      case ApprovalDecision.Deny => false
      case ApprovalDecision.Allow =>
        autoApprove match
          case AutoApprove.All         => true
          case AutoApprove.Only(tools) => tools.contains(toolName(request))
    if !allowed then PermissionOutcome.Cancelled
    else
      request.options
        .find(_.kind == PermissionKind.AllowOnce)
        .orElse(
          request.options.find(_.kind == PermissionKind.AllowAlways)
        )
        .fold(PermissionOutcome.Cancelled): o =>
          PermissionOutcome.Selected(o.optionId)

  def defaultOutcome: PermissionOutcome = PermissionOutcome.Cancelled
