/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util.v3_4

import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.scalatest.BeforeAndAfter

class TaskCloserTest extends CypherFunSuite with BeforeAndAfter {
  var taskCloser: TaskCloser = _
  var ran = false
  var outcome = false

  before {
    taskCloser = new TaskCloser
    ran = false
  }

  test("cleanUp call methods") {
    taskCloser.addTask(closingTask)
    taskCloser.close(success = true)

    ran should equal(true)
    outcome should equal(true)
  }

  test("cleanUp call methods and pass on the success") {
    outcome = true

    taskCloser.addTask(closingTask)
    taskCloser.close(success = false)

    ran should equal(true)
    outcome should equal(false)
  }

  test("cleanUp calls all cleanUp methods even if some fail") {
    taskCloser.addTask(_ => throw new Exception("oh noes"))
    taskCloser.addTask(closingTask)

    intercept[Exception](taskCloser.close(success = true))

    ran should equal(true)
    outcome should equal(true)
  }

  test("cleanUp calls all cleanUp and if there are failures the first exception is thrown") {
    val expected = new Exception("oh noes")
    taskCloser.addTask(_ => throw new Exception)
    taskCloser.addTask(_ => throw expected)

    val ex = intercept[Exception](taskCloser.close(success = true))

    ex should equal(expected)
  }

  test("does not close twice") {
    val expected = new Exception("oh noes")
    taskCloser.addTask(closingTask)

    taskCloser.close(success = true)
    ran = false
    taskCloser.close(success = true)

    // If we close the closer twice, it should only run this once
    ran should not equal(true)
  }

  test("cleanup without any cleanups does not fail") {
    taskCloser.close(success = true)

    ran should equal(false)
  }

  private def closingTask(success:Boolean) = {
    ran = true
    outcome = success
  }

}
