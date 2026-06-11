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

import org.scalatest.BeforeAndAfterEach
import org.scalatest.Suite

import scala.collection.mutable

/**
 * A stackable [[BeforeAndAfterEach]] that lets traits register any number of before/after handlers via
 * [[registerBeforeEach]] / [[registerAfterEach]] instead of overriding `beforeEach` / `afterEach`
 * themselves.
 *
 * Handlers nest like stacked `beforeEach`/`afterEach` overrides with super-calls: registration happens in
 * initialization order (supertypes before subtypes), before-handlers run in that order, and after-handlers
 * run in reverse, so the first-registered handler sets up first and tears down last. Every after-handler
 * runs even if another one throws, as with chained `try`/`finally` teardown.
 *
 * Why this exists: a Scala 3 test trait that overrides `BeforeAndAfterEach.afterEach` and calls
 * `super.afterEach()` generates a super-accessor the Scala 2.13 TASTy reader cannot reconstruct, making
 * the trait unreadable from Scala 2.13 consumers. Switching to `BeforeAndAfter` does not work either:
 * its `after { }` may only be called once per suite instance (`NotAllowedException`), so two traits that
 * each register cleanup break every suite mixing both (e.g. the acceptance suites combining
 * `CypherComparisonSupport` with `CreateTempFileTestSupport`); it also trips Scala 3's accidental-override
 * rule against `AnyFunSuiteLike` (workable via delegating `run`/`runTest` overrides as in
 * `CommunityCypherTestSuite`, but those super-calls cannot live in a Scala-3-compiled test-jar trait for
 * the same super-accessor reason), and its registration methods need the Scala-2 `source.Position` macro.
 * This trait resolves all of that: it is compiled by Scala 2.13 (so its single `override` + `super` is
 * fine), it is `BeforeAndAfterEach`-based (no accidental-override, no `Position` implicits), it accepts any
 * number of handlers, and Scala 3 traits extend it and only call `registerAfterEach` /
 * `registerBeforeEach` (plain method calls, no override/super), keeping them readable from Scala 2.13.
 */
trait StackableBeforeAndAfterEach extends BeforeAndAfterEach {
  self: Suite =>

  private val beforeHandlers = mutable.ListBuffer.empty[() => Unit]
  private val afterHandlers = mutable.ListBuffer.empty[() => Unit]

  protected def registerBeforeEach(handler: => Unit): Unit = beforeHandlers += (() => handler)
  protected def registerAfterEach(handler: => Unit): Unit = afterHandlers += (() => handler)

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    beforeHandlers.foreach(_.apply())
  }

  override protected def afterEach(): Unit = {
    def runAll(handlers: List[() => Unit]): Unit = handlers match {
      case Nil => ()
      case head :: tail =>
        try head()
        finally runAll(tail)
    }
    try runAll(afterHandlers.reverseIterator.toList)
    finally super.afterEach()
  }
}
