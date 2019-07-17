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
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.v4_0.util.TransactionOutOfMemoryException
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}

object MemoryManagementTestBase {
  val maxMemory = 1000
  val failTestThreshold = 1010
}

trait InputStreams[CONTEXT <: RuntimeContext] {
  self: RuntimeTestSuite[CONTEXT] =>

  protected def infiniteInput(data: Any*): InputDataStream = {
    val eightInfinities = Seq.fill(8)(iterate(data.toArray, None, nodeInput = false))
    iteratorInput(eightInfinities: _*)
  }

  protected def infiniteNodeInput(data: Any*): InputDataStream = {
    val eightInfinities = Seq.fill(8)(iterate(data.toArray, None, nodeInput = true))
    iteratorInput(eightInfinities: _*)
  }

  protected def finiteInput(limit: Int, data: Any*): InputDataStream = {
    val eightInfinities = Seq.fill(8)(iterate(data.toArray, Some(limit), nodeInput = false))
    iteratorInput(eightInfinities: _*)
  }


  protected def iterate(data: Array[Any], limit: Option[Int], nodeInput: Boolean): Iterator[Array[Any]] = new Iterator[Array[Any]] {
    private var i = 0
    override def hasNext: Boolean = limit.fold(true)(i < _)

    override def next(): Array[Any] = {
      i += 1
      if (limit.isEmpty && i > MemoryManagementTestBase.failTestThreshold) {
        fail("The query was not killed even though it consumed too much memory.")
      }
      if (data.isEmpty) {
        // Make sure that if you ever call this in parallel, you cannot just create nodes here and need to redesign the test.
        val value = if (nodeInput) graphDb.createNode() else i
        Array(value)
      } else {
        data
      }
    }
  }
}

abstract class MemoryManagementDisabledTestBase[CONTEXT <: RuntimeContext](
                                                                            edition: Edition[CONTEXT],
                                                                            runtime: CypherRuntime[CONTEXT]
                                                                          )
  extends RuntimeTestSuite[CONTEXT](edition.copyWith(
    GraphDatabaseSettings.transaction_max_memory -> "0"), runtime) with InputStreams[CONTEXT] {
  test("should not kill memory eating query") {
    // given
    val input = finiteInput(10000)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort(Seq(Ascending("x")))
      .input(variables = Seq("x"))
      .build()

    // then no exception
    consume(execute(logicalQuery, runtime, input))
  }
}

abstract class MemoryManagementTestBase[CONTEXT <: RuntimeContext](
                                                                    edition: Edition[CONTEXT],
                                                                    runtime: CypherRuntime[CONTEXT]
                                                                  )
  extends RuntimeTestSuite[CONTEXT](edition.copyWith(
    GraphDatabaseSettings.transaction_max_memory -> MemoryManagementTestBase.maxMemory.toString), runtime) with InputStreams[CONTEXT] {

  test("should kill sort query before it runs out of memory") {
    // given
    val input = infiniteInput()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort(Seq(Ascending("x")))
      .input(variables = Seq("x"))
      .build()

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill distinct query before it runs out of memory") {
    // given
    val input = infiniteInput()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .distinct("x AS x")
      .input(variables = Seq("x"))
      .build()

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should not kill count aggregation query") {
    // given
    val input = finiteInput(100000)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(*) AS c"))
      .input(variables = Seq("x"))
      .build()

    // then
    consume(execute(logicalQuery, runtime, input))
  }

  test("should kill collect aggregation query before it runs out of memory") {
    // given
    val input = infiniteInput()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("collect(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill grouping aggregation query before it runs out of memory - one large group") {
    // given
    val input = infiniteInput(0)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill grouping aggregation query before it runs out of memory - many groups") {
    // given
    val input = infiniteInput()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill node hash join query before it runs out of memory") {
    // given
    val nodes = nodeGraph(1)
    val input = infiniteNodeInput(nodes.head)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeHashJoin("x")
      .|.allNodeScan("x")
      .input(nodes = Seq("x"))
      .build()

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill multi-column node hash join query before it runs out of memory") {
    // given
    val (nodes, _) = circleGraph(1)
    val input = infiniteNodeInput(nodes.head, nodes.head)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeHashJoin("x", "y")
      .|.expand("(x)--(y)")
      .|.allNodeScan("x")
      .input(nodes = Seq("x", "y"))
      .build()

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }
}

/**
  * Tests for runtime with full language support
  */
trait FullSupportMemoryManagementTestBase [CONTEXT <: RuntimeContext] {
  self: MemoryManagementTestBase[CONTEXT] =>

  test("should kill eager query before it runs out of memory") {
    // given
    val input = infiniteNodeInput()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .eager()
      .input(nodes = Seq("x"))
      .build()

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill stdDev aggregation query before it runs out of memory") {
    // given
    val input = infiniteInput(5)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("stdev(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill percentileDisc aggregation query before it runs out of memory") {
    // given
    val input = infiniteInput(5)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("percentileDisc(x, 0.1) AS c"))
      .input(variables = Seq("x"))
      .build()

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill percentileCont aggregation query before it runs out of memory") {
    // given
    val input = infiniteInput(5)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("percentileCont(x, 0.1) AS c"))
      .input(variables = Seq("x"))
      .build()

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill distinct aggregation query before it runs out of memory") {
    // given
    val input = infiniteNodeInput()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(DISTINCT x) AS c"))
      .input(nodes = Seq("x"))
      .build()

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }
}
