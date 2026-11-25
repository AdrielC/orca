package orca.backend

/** Shared live-turn event-channel tuning for subprocess-style backends. */
private[backend] object LiveTurnEvents:

  /** Cap on in-flight unread events. The producer blocks once full, so a slow
    * consumer's backpressure flows back into the agent process pipe.
    */
  val ChannelCapacity: Int = 1024
