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

import org.apache.commons.lang3.exception.ExceptionUtils
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.scalatest.Assertion

import java.io.PrintStream

import scala.collection.mutable.ArrayBuffer

/**
 * Runtime test utility methods that may have implementations that dependent on the edition (community or enterprise).
 */
trait RuntimeTestUtils {

  /**
   * Extracts the query statistics from the given query state, independent of runtime implementation.
   */
  def queryStatistics(queryState: AnyRef): org.neo4j.cypher.internal.runtime.QueryStatistics
}

object CommunityRuntimeTestUtils extends RuntimeTestUtils {

  override def queryStatistics(state: AnyRef): org.neo4j.cypher.internal.runtime.QueryStatistics = {
    state.asInstanceOf[QueryState].getStatistics
  }
}

/**
 * A test utility for reducing flakiness of non-deterministic tests.
 *
 * <p>
 * On failure, it will retry the test up to [[retries]] number of times,
 * and if it fails more than [[toleratedFailures]] number of times, it will fail the test.
 *
 * <p>
 * Side-notes:
 * If the test succeeds during a retry attempt, it will still run the remaining retry attempts.
 * A test run attempt is considered to have failed if it throws an exception.
 *
 * <p>
 * You can also configure if the first attempt should be accepted as an instant success ([[acceptInstantSuccess]]),
 * how long to sleep between attempts ([[sleepMs]]),
 * and an optional [[PrintStream]] to log tolerated failures to (e.g. Some(System.out)).
 */
case class ScalaTestDeflaker(
  acceptInstantSuccess: Boolean = true,
  retries: Int = 2,
  toleratedFailures: Int = 1,
  sleepMs: Int = 100,
  printToleratedFailuresTo: Option[PrintStream] = None
) {
  require(toleratedFailures < retries)

  def apply(
    test: () => Assertion,
    beforeEachAttempt: () => Unit = () => {},
    afterEachAttempt: () => Unit = () => {},
    afterEachFailedAttempt: Int => Unit = _ => {}
  ): Unit = {

    var failures = 0
    val exceptions = new ArrayBuffer[Exception]()
    var attempt = 0
    while (attempt <= retries) {
      try {
        beforeEachAttempt()
        test()
        afterEachAttempt()
        if (acceptInstantSuccess && attempt == 0) {
          // Instant success!
          return
        }
      } catch {
        case e: Exception =>
          failures += 1
          try {
            afterEachFailedAttempt(failures)
          } catch {
            case ee: Exception =>
              e.addSuppressed(ee)
          }
          try {
            afterEachAttempt()
          } catch {
            case ee: Exception =>
              e.addSuppressed(ee)
          }
          if (failures > toleratedFailures) {
            exceptions.foreach(e.addSuppressed)
            throw e
          }
          exceptions += e
          Thread.sleep(sleepMs)
      }
      attempt += 1
    }
    printToleratedFailures(failures, attempt, exceptions)
  }

  private def printToleratedFailures(failures: Int, attempts: Int, exceptions: ArrayBuffer[Exception]): Unit = {
    printToleratedFailuresTo match {
      case Some(printStream) =>
        val nl = System.lineSeparator()
        val sb = new StringBuilder()
        sb ++= "Test failures tolerated by ScalaTestDeflaker "
        sb ++= String.format("(%d of %d attempts): ", failures, attempts)
        sb ++= nl
        exceptions.foreach { t =>
          sb ++= ExceptionUtils.getStackTrace(t)
          sb ++= nl
        }
        printStream.print(sb.result())
      case _ =>
      // Do nothing
    }
  }
}
