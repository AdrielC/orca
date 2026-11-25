package orca.backend.acp

import AcpProtocol.*

/** Answers inbound `session/request_permission` calls while the transport is
  * active. Implementations must not widen a restriction the caller already
  * chose.
  */
private[backend] trait AcpPermissionDecider:
  def decide(request: RequestPermissionRequest): PermissionOutcome

private[backend] object AcpPermissionDecider:
  /** Fail-closed gating: every request is answered as `outcome: cancelled`. */
  val defaultCancelled: AcpPermissionDecider =
    (_: RequestPermissionRequest) => PermissionOutcome.Cancelled
