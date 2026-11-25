package orca.backend

/** Tracks whether assistant activity is open since the last
  * [[TurnEvent.AssistantMessageEnd]], per the [[TurnEvent]] message grammar.
  */
private[backend] object TurnMessageGrammar:

  def activityAfter(
      activitySinceMessageEnd: Boolean,
      event: TurnEvent
  ): Boolean =
    event match
      case TurnEvent.AssistantMessageEnd => false
      case e if e.opensMessage           => true
      case _                             => activitySinceMessageEnd
