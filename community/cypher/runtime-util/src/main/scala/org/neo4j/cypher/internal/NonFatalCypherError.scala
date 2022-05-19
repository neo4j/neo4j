package org.neo4j.cypher.internal

import scala.util.control.NonFatal

/**
 * Extractor of non-fatal Throwables.
 *
 * This class delegates to [[scala.util.control.NonFatal]], which does not match fatal errors like [[VirtualMachineError]] (e.g., [[OutOfMemoryError]]).
 * See [[scala.util.control.NonFatal]] for more info.
 */
object NonFatalCypherError {

  /**
   * Returns true if the provided `Throwable` is to be considered non-fatal, or false if it is to be considered fatal
   */
  def apply(t: Throwable): Boolean = isNonFatal(t)

  /**
   * Returns Some(t) if NonFatalToRuntime(t) == true, otherwise None
   */
  def unapply(t: Throwable): Option[Throwable] = Some(t).filter(apply)

  // for use from Java code
  def isNonFatal(t: Throwable): Boolean = t match {
    case NonFatal(_) =>
      true
    case _ =>
      false
  }
}
