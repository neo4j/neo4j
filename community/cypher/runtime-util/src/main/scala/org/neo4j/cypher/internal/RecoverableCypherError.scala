/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.NonFatalCypherError.isNonFatal
import org.neo4j.kernel.api.exceptions.Status.Classification
import org.neo4j.kernel.api.exceptions.Status.HasStatus

/**
 * Used for deciding which errors we should try to recover from during query
 * execution if the query has error handling semantics.
 * E.g. errors that occur while executing a subquery of CALL IN TRANSACTION
 * with error handling.
 *
 * Internal errors and fatal errors that may have compromised the state of the
 * runtime query execution are to be considered non-recoverable, since
 * we may not always be able to guarantee correctness if we continue.
 */
object RecoverableCypherError {

  def apply(t: Throwable): Boolean = isRecoverable(t)

  def unapply(t: Throwable): Option[Throwable] = Some(t).filter(isRecoverable)

  // We use a whitelist approach here:
  // Only non-fatal classified errors are considered recoverable,
  // except DatabaseError which is used for more serious and/or internal problems of the database.
  // NOTE: If you believe a specific error is incorrectly classified here, first consider changing the classification
  //       of its error code before complicating this logic with special cases,
  //       so we can keep this reasonably principled and easily explainable to users.
  def isRecoverable(t: Throwable): Boolean = t match {
    case e: HasStatus if isNonFatal(e) =>
      val classification = e.status().code().classification()
      classification match {
        case Classification.ClientError |
          Classification.TransientError =>
          true
        case _ =>
          false
      }

    // Unfortunately there are still some public API exceptions that do not implement HasStatus that we still
    // want to consider recoverable.
    // We should probably add a HasStatus in the next major release where we can make API changes to get rid
    // of this special case.
    // (These should also be matching the above classifications after passing through
    //  org.neo4j.cypher.internal.macros.TranslateExceptionMacros)
    case _: org.neo4j.graphdb.ConstraintViolationException =>
      true

    case _ =>
      false
  }
}
