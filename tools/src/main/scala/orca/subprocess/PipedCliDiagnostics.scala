package orca.subprocess

/** Shared limits for subprocess diagnostic retention across piped backends. */
object PipedCliDiagnostics:
  val StderrTailCapacity: Int = 50
