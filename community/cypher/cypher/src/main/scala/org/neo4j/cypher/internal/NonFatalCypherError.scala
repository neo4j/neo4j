/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal

import scala.util.control.NonFatal

/**
 * Extractor of non-fatal Throwables.
 *
 * For the most part this class delegates to [[scala.util.control.NonFatal]],
 * which not match fatal errors like [[VirtualMachineError]] (e.g., [[OutOfMemoryError]]).
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

  def isNonFatal(t: Throwable): Boolean = t match {
    // NonFatal considers StackOverflowError as fatal, but if we catch it we can at least inform the user via QuerySubscriber.onError()
    case NonFatal(_) =>
      true
    case _:StackOverflowError =>
      true
    case _ =>
      false
  }
}
