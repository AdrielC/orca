package orca.backend.acp

import orca.agents.{AutoApprove, ToolSet}
import orca.backend.ApprovalDecision

import AcpProtocol.*

class AcpPermissionPolicyTest extends munit.FunSuite:

  private def request(kind: ToolKind): RequestPermissionRequest =
    RequestPermissionRequest(
      AcpSessionId("sess-a"),
      ToolCallUpdate(
        toolCallId = ToolCallId("call-1"),
        kind = Some(kind)
      ),
      Vector(
        PermissionOption(
          PermissionOptionId("once"),
          "Allow once",
          PermissionKind.AllowOnce
        )
      )
    )

  test("non-full tiers are not governable"):
    val policy = AcpPermissionPolicy(ToolSet.ReadOnly, AutoApprove.All)
    assert(!policy.governable)

  test("policy constrains allow to the configured auto-approve set"):
    val policy =
      AcpPermissionPolicy(ToolSet.Full, AutoApprove.Only(Set("read")))
    val req = request(ToolKind.Execute)
    assertEquals(
      policy.toOutcome(ApprovalDecision.Allow, req),
      PermissionOutcome.Cancelled
    )

  test("allow selects the once option under full auto-approve"):
    val policy = AcpPermissionPolicy(ToolSet.Full, AutoApprove.All)
    assertEquals(
      policy.toOutcome(ApprovalDecision.Allow, request(ToolKind.Read)),
      PermissionOutcome.Selected(PermissionOptionId("once"))
    )

  test("allow honors auto-approve only when the tool kind is listed"):
    val policy =
      AcpPermissionPolicy(ToolSet.Full, AutoApprove.Only(Set("read")))
    assertEquals(
      policy.toOutcome(ApprovalDecision.Allow, request(ToolKind.Read)),
      PermissionOutcome.Selected(PermissionOptionId("once"))
    )

  test("deny always cancels on the wire"):
    val policy = AcpPermissionPolicy(ToolSet.Full, AutoApprove.All)
    assertEquals(
      policy.toOutcome(ApprovalDecision.Deny, request(ToolKind.Execute)),
      PermissionOutcome.Cancelled
    )
