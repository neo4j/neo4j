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
package org.neo4j.cypher.internal.result

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.BeforeAndAfter

class TaskCloserTest extends CypherFunSuite with BeforeAndAfter {
  var taskCloser: TaskCloser = _
  var ran = false
  var outcome: CloseReason = Failure

  before {
    taskCloser = new TaskCloser
    ran = false
  }

  test("cleanUp call methods") {
    taskCloser.addTask(closingTask)
    taskCloser.close(Success)

    ran should equal(true)
    outcome should equal(Success)
  }

  test("cleanUp call methods and pass on the success") {
    outcome = Success

    taskCloser.addTask(closingTask)
    taskCloser.close(Failure)

    ran should equal(true)
    outcome should equal(Failure)
  }

  test("cleanUp calls all cleanUp methods even if some fail") {
    taskCloser.addTask(_ => throw new Exception("oh noes"))
    taskCloser.addTask(closingTask)

    intercept[Exception](taskCloser.close(Success))

    ran should equal(true)
    outcome should equal(Success)
  }

  test("cleanUp calls all cleanUp and if there are failures the first exception is thrown") {
    val expected = new Exception("oh noes")
    taskCloser.addTask(_ => throw new Exception)
    taskCloser.addTask(_ => throw expected)

    val ex = intercept[Exception](taskCloser.close(Success))

    ex should equal(expected)
  }

  test("does not close twice") {
    taskCloser.addTask(closingTask)

    taskCloser.close(Success)
    ran = false
    taskCloser.close(Success)

    // If we close the closer twice, it should only run this once
    ran should not equal true
  }

  test("cleanup without any cleanups does not fail") {
    taskCloser.close(Success)

    ran should equal(false)
  }

  private def closingTask(reason: CloseReason): Unit = {
    ran = true
    outcome = reason
  }

}
