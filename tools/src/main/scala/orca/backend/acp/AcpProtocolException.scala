package orca.backend.acp

import orca.OrcaFlowException

private[backend] final class AcpProtocolException(message: String)
    extends OrcaFlowException(message)
