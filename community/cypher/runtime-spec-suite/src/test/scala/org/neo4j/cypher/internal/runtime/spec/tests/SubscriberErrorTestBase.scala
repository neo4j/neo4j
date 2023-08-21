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
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue

abstract class SubscriberErrorTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT]
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should fail correctly on onResult exception") {
    // when
    val subscriber = ExplodingSubscriber(explodeOnResult = true)

    // then
    a[Kaboom] shouldBe thrownBy {
      execute(someGraphAndQuery, runtime, NO_INPUT.stream(), subscriber).consumeAll()
    }
  }

  test("should fail correctly on onRecord exception") {
    // when
    val subscriber = ExplodingSubscriber(explodeOnRecord = true)

    // then
    a[Kaboom] shouldBe thrownBy {
      execute(someGraphAndQuery, runtime, NO_INPUT.stream(), subscriber).consumeAll()
    }
  }

  test("should fail correctly on onField exception") {
    // when
    val subscriber = ExplodingSubscriber(explodeOnField = true)

    // then
    a[Kaboom] shouldBe thrownBy {
      execute(someGraphAndQuery, runtime, NO_INPUT.stream(), subscriber).consumeAll()
    }
  }

  test("should fail correctly on onRecordCompleted exception") {
    // when
    val subscriber = ExplodingSubscriber(explodeOnRecordCompleted = true)

    // then
    a[Kaboom] shouldBe thrownBy {
      execute(someGraphAndQuery, runtime, NO_INPUT.stream(), subscriber).consumeAll()
    }
  }

  test("should fail correctly on onError exception") {
    // when
    val subscriber = ExplodingSubscriber(explodeOnError = true)

    // then
    val exception =
      intercept[org.neo4j.exceptions.ArithmeticException] {
        execute(someArithmeticErrorQuery, runtime, NO_INPUT.stream(), subscriber).consumeAll()
      }

    exception.getSuppressed.head shouldBe a[Kaboom]
  }

  test("should fail correctly on onResultCompleted exception") {
    // when
    val subscriber = ExplodingSubscriber(explodeOnResultCompleted = true)

    // then
    a[Kaboom] shouldBe thrownBy {
      execute(someGraphAndQuery, runtime, NO_INPUT.stream(), subscriber).consumeAll()
    }
  }

  private def someGraphAndQuery: LogicalQuery = {
    given { nodeGraph(3) }
    new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()
  }

  private def someArithmeticErrorQuery: LogicalQuery = {
    new LogicalQueryBuilder(this)
      .produceResults("x")
      .projection("1/0 AS x")
      .argument()
      .build()
  }

  class Kaboom extends Exception("kaboom")

  case class ExplodingSubscriber(
    explodeOnResult: Boolean = false,
    explodeOnRecord: Boolean = false,
    explodeOnField: Boolean = false,
    explodeOnRecordCompleted: Boolean = false,
    explodeOnError: Boolean = false,
    explodeOnResultCompleted: Boolean = false
  ) extends QuerySubscriber {

    override def onResult(numberOfFields: Int): Unit =
      if (explodeOnResult) throw new Kaboom

    override def onRecord(): Unit =
      if (explodeOnRecord) throw new Kaboom

    override def onField(offset: Int, value: AnyValue): Unit =
      if (explodeOnField) throw new Kaboom

    override def onRecordCompleted(): Unit =
      if (explodeOnRecordCompleted) throw new Kaboom

    override def onError(throwable: Throwable): Unit =
      if (explodeOnError) throw new Kaboom

    override def onResultCompleted(statistics: QueryStatistics): Unit =
      if (explodeOnResultCompleted) throw new Kaboom
  }
}
