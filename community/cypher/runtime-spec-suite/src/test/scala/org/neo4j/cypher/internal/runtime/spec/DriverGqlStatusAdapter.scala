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
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.GqlExceptionMatcher
import org.neo4j.driver.exceptions.{Neo4jException => DriverNeo4jException}
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.scalatest.matchers.BeMatcher

import java.util.Optional
import java.util.{Map => JMap}

/**
 * Adapts driver-side `Neo4jException` onto `ErrorGqlStatusObject` and walks the full Java
 * `getCause` chain so existing `gqlStatus(...).withCause(...)` matchers see the real cause
 * when bolt (under `-Ptest-spd`) wraps it — e.g. pipelined SPD nests a `22G03` inside an
 * `IOException` carrying a generic `50N20`. The supplied matcher picks the first candidate
 * it matches.
 */
object DriverGqlStatusAdapter {

  def asGqlException(matcher: GqlExceptionMatcher): BeMatcher[Throwable] =
    BeMatcher { (t: Throwable) =>
      val candidates = collectCandidates(t)
      if (candidates.isEmpty) {
        throw new AssertionError(
          s"No ErrorGqlStatusObject or driver Neo4jException found in cause chain of $t"
        )
      }
      candidates.view.map(c => (c, matcher(c))).find(_._2.matches) match {
        case Some((_, result)) => result
        // Fall back to the deepest candidate — most-specific code, clearest failure message.
        case None => matcher(candidates.last)
      }
    }

  private def collectCandidates(t: Throwable): List[ErrorGqlStatusObject] = {
    val buf = List.newBuilder[ErrorGqlStatusObject]
    var current: Throwable = t
    while (current != null) {
      current match {
        case e: ErrorGqlStatusObject => buf += e
        case e: DriverNeo4jException => buf += adapt(e)
        case _                       =>
      }
      current = current.getCause
    }
    buf.result()
  }

  private def adapt(ex: DriverNeo4jException): ErrorGqlStatusObject = new ErrorGqlStatusObject {
    override def gqlStatusObject(): ErrorGqlStatusObject = null
    override def getMessage: String = ex.getMessage
    override def legacyMessage: String = ex.getMessage
    override def gqlStatus(): String = ex.gqlStatus()
    override def statusDescription(): String = ex.statusDescription()
    override def diagnosticRecord(): JMap[String, AnyRef] = JMap.of()

    override def cause(): Optional[ErrorGqlStatusObject] =
      ex.gqlCause().map(adapt)
  }
}
