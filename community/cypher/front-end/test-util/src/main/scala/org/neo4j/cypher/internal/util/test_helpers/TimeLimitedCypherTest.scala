/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.util.test_helpers

import org.neo4j.cypher.internal.test.util.DumpUtils
import org.scalatest.Failed
import org.scalatest.Outcome
import org.scalatest.concurrent.Signaler
import org.scalatest.concurrent.ThreadSignaler
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.Minutes
import org.scalatest.time.Span

/**
 * Limits tests in a class to 5 minutes.
 *
 * Broken due to compilation error introduced in Scala 2.13 https://github.com/scala/bug/issues/12520
 */
trait TimeLimitedCypherTest extends TimeLimitedTests {
  self: CypherFunSuite =>

  override val timeLimit: Span = Span(5, Minutes)
  override val defaultTestSignaler: Signaler = ThreadSignaler

  abstract override def withFixture(test: NoArgTest): Outcome = {
    super.withFixture(test) match {
      case Failed(e: org.scalatest.exceptions.TestFailedDueToTimeoutException) =>
        Failed(e.modifyMessage(opts => Some(opts.fold("")(_ + System.lineSeparator()) + DumpUtils.threadDump())))
      case Failed(e: org.scalatest.exceptions.TestCanceledException) =>
        Failed(e.modifyMessage(opts => Some(opts.fold("")(_ + System.lineSeparator()) + DumpUtils.threadDump())))
      case outcome => outcome
    }
  }
}
