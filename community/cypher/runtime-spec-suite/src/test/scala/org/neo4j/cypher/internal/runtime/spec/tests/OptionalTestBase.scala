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

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.InputValues
import org.neo4j.cypher.internal.runtime.TestSubscriber
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.exceptions.ArithmeticException
import org.neo4j.graphdb.Node
import org.neo4j.values.storable.NoValue.NO_VALUE
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue

abstract class OptionalTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should optional expand") {
    // given
    val n = sizeHint

    val nodeConnections = givenGraph {
      val nodes = nodeGraph(n)
      randomlyConnect(nodes, Connectivity(0, 5, "OTHER"), Connectivity(0, 5, "NEXT"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .apply()
      .|.optional("x")
      .|.expandAll("(x)-[:NEXT]->(z)")
      .|.expandAll("(x)-[:OTHER]->(y)")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      NodeConnections(x, connections) <- nodeConnections
      y <- if (connections.size == 2) connections("OTHER") else Seq(null)
      z <- if (connections.size == 2) connections("NEXT") else Seq(null)
    } yield Array(x, y, z)

    runtimeResult should beColumns("x", "y", "z").withRows(expected)
  }

  test("should optional expand - all nulls") {
    // given
    val n = sizeHint
    val nodes = givenGraph { nodeGraph(n) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.optional("x")
      .|.expandAll("(x)-[:OTHER]->(y)")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- nodes
    } yield Array(x, null)
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should support chained optionals") {
    // given
    val n = sizeHint

    val nodeConnections = givenGraph {
      val nodes = nodeGraph(n)
      randomlyConnect(nodes, Connectivity(0, 5, "OTHER"), Connectivity(0, 5, "NEXT"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .apply()
      .|.optional("x")
      .|.optional("x")
      .|.expandAll("(x)-[:NEXT]->(z)")
      .|.expandAll("(x)-[:OTHER]->(y)")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      NodeConnections(x, connections) <- nodeConnections
      y <- if (connections.size == 2) connections("OTHER") else Seq(null)
      z <- if (connections.size == 2) connections("NEXT") else Seq(null)
    } yield Array(x, y, z)

    runtimeResult should beColumns("x", "y", "z").withRows(expected)
  }

  test("should support optional under nested apply") {
    // given
    val n = Math.sqrt(sizeHint).toInt

    val nodeConnections = givenGraph {
      val nodes = nodeGraph(n, "Honey")
      randomlyConnect(nodes, Connectivity(0, 5, "NEXT"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z1", "z2")
      .apply()
      .|.apply()
      .|.|.optional("x", "y")
      .|.|.expandAll("(y)-[:NEXT]->(z2)")
      .|.|.optional("x", "y")
      .|.|.expandAll("(x)-[:NEXT]->(z1)")
      .|.|.argument("x", "y")
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      NodeConnections(x, xConnections) <- nodeConnections
      NodeConnections(y, yConnections) <- nodeConnections
      z1 <- if (xConnections.nonEmpty && yConnections.nonEmpty) xConnections("NEXT") else Seq(null)
      z2 <- if (yConnections.nonEmpty) yConnections("NEXT") else Seq(null)
    } yield Array(x, y, z1, z2)

    runtimeResult should beColumns("x", "y", "z1", "z2").withRows(expected)
  }

  test("should support optional with hash join") {
    // given
    val n = Math.sqrt(sizeHint).toInt

    val nodeConnections = givenGraph {
      val nodes = nodeGraph(n, "Honey")
      randomlyConnect(nodes, Connectivity(0, 5, "OTHER"), Connectivity(0, 5, "NEXT"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "z1", "z2")
      .apply()
      .|.nodeHashJoin("x")
      .|.|.optional("x")
      .|.|.expandAll("(x)-[:NEXT]->(z2)")
      .|.|.argument("x")
      .|.optional("x")
      .|.expandAll("(x)-[:OTHER]->(z1)")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      NodeConnections(x, connections) <- nodeConnections
      z1 <- if (connections.contains("OTHER")) connections("OTHER") else Seq(null)
      z2 <- if (connections.contains("NEXT")) connections("NEXT") else Seq(null)
    } yield Array(x, z1, z2)

    runtimeResult should beColumns("x", "z1", "z2").withRows(expected)
  }

  // This test failed because of expand+limit
  test("should support optional with limit") {
    // given
    val n = sizeHint

    val nodeConnections = givenGraph {
      val nodes = nodeGraph(n, "Honey")
      randomlyConnect(nodes, Connectivity(0, 5, "OTHER")).map {
        case NodeConnections(node, connections) => (node.getId, connections)
      }.toMap
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.limit(1)
      .|.optional("x")
      .|.expandAll("(x)-->(y)")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y").withRows(matching {
      case rows: Seq[_] if rows.forall {
          case Array(x, y) =>
            val xid = x.asInstanceOf[VirtualNodeValue].id()
            val connections = nodeConnections(xid)
            if (connections.isEmpty) {
              y == Values.NO_VALUE
            } else {
              withClue(s"x id: $xid --") {
                val yid = y match {
                  case node: VirtualNodeValue => node.id()
                  case _                      => y shouldBe a[VirtualNodeValue]
                }
                connections.values.flatten.exists(_.getId == yid)
              }
            }
        } && {
          val xs = rows.map(_.asInstanceOf[Array[_]](0))
          xs.distinct.size == xs.size // Check that there is at most one row per x
        } =>
    })
  }

  test("should stream") {
    // given
    val stream = createBatchedInputValues().stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .optional()
      .input(variables = Seq("x"))
      .build()

    val subscriber = TestSubscriber.concurrent
    val runtimeResult = execute(logicalQuery, runtime, stream, subscriber)

    runtimeResult.request(1)
    runtimeResult.await()

    // then
    subscriber.allSeen should have size 1
    // NOTE: createBatchedInputValues() creates larger input for parallel runtime so this should not be flaky
    stream.hasMore should be(true)
  }

  test("should support optional under nested apply with sort after apply") {
    // given
    val n = sizeHint

    val nodeConnections = givenGraph {
      val nodes = nodeGraph(n)
      randomlyConnect(nodes, Connectivity(0, 5, "OTHER"), Connectivity(0, 5, "NEXT"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .sort("z ASC", "x DESC", "y ASC")
      .apply()
      .|.optional("x")
      .|.expandAll("(x)-[:NEXT]->(z)")
      .|.expandAll("(x)-[:OTHER]->(y)")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    def id(node: Node): Long = if (node == null) Long.MaxValue else node.getId

    // then
    val expected = for {
      NodeConnections(x, connections) <- nodeConnections
      y <- if (connections.size == 2) connections("OTHER") else Seq(null)
      z <- if (connections.size == 2) connections("NEXT") else Seq(null)
    } yield Array(x, y, z)
    val expectedInOrder = expected.sortBy(row => (id(row(2)), -id(row(0)), id(row(1))))

    runtimeResult should beColumns("x", "y", "z").withRows(inOrder(expectedInOrder))
  }

  test("should work on top of distinct") {
    // given
    val stream = createBatchedInputValues().stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .optional()
      .distinct("x AS x")
      .input(variables = Seq("x"))
      .build()

    val subscriber = TestSubscriber.concurrent
    val runtimeResult = execute(logicalQuery, runtime, stream, subscriber)

    runtimeResult.request(1)
    runtimeResult.await()

    // then
    subscriber.allSeen should have size 1
    // NOTE: createBatchedInputValues() creates larger input for parallel runtime so this should not be flaky
    stream.hasMore should be(true)
  }

  test("should work when nullable variable is aliased on RHS of SemiApply") {
    val nodes = givenGraph {
      nodeGraph(1)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("n0")
      .semiApply()
      .|.optional("n0")
      .|.filter("false")
      .|.projection("n0 AS n2")
      .|.allNodeScan("n1")
      .allNodeScan("n0")
      .build()

    execute(query, runtime) should beColumns("n0").withRows(singleColumn(nodes))
  }

  test("should work when nullable variable is aliased on RHS of Apply") {
    val nodes = givenGraph {
      nodeGraph(1)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("n0")
      .apply()
      .|.optional("n0")
      .|.filter("false")
      .|.projection("n0 AS n2")
      .|.allNodeScan("n1")
      .allNodeScan("n0")
      .build()

    execute(query, runtime) should beColumns("n0").withRows(singleColumn(nodes))
  }

  // https://trello.com/c/Tj00vVvE/
  test("should treat projected argument as non-argument variable when empty") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .apply()
      .|.optional("a")
      .|.filter("false")
      .|.projection("a AS b")
      .|.argument("a")
      .unwind("['input'] as a")
      .argument()
      .build()

    execute(query, runtime) should beColumns("a", "b")
      .withRows(Seq(Array("input", NO_VALUE)))
  }

  // https://trello.com/c/nN53ne8o/
  test("should permit cartesian product of argument under an optional") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("a")
      .optional()
      .apply()
      .|.cartesianProduct()
      .|.|.projection("a AS b")
      .|.|.argument()
      .|.argument()
      .unwind("[1, 2] as a")
      .argument()
      .build()

    execute(query, runtime) should beColumns("a")
      .withRows(inAnyOrder(Seq(Array(1), Array(2))))
  }

  def createBatchedInputValues(): InputValues = {
    val (batchSize, numberOfWorkers) = runtime.name.toLowerCase match {
      case "pipelined" =>
        val morselSize = getConfig.get(GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big)
        (Math.max(10, morselSize + 1), 1)
      case "parallel" =>
        val morselSize = getConfig.get(GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big)
        val numberOfWorkers = getConfig.get(GraphDatabaseSettings.cypher_worker_limit).toInt match {
          case n if n <= 0 => math.max(Runtime.getRuntime.availableProcessors + n, 0)
          case n           => n
        }
        (Math.max(10, morselSize + 1), numberOfWorkers)
      case _ =>
        (10, 1)
    }
    // NOTE: Parallel runtime will exhaust input until intermediate buffers are full
    val inputSize = Math.max(sizeHint, (batchSize + 1) * numberOfWorkers * numberOfWorkers)

    batchedInputValues(batchSize, (0 until inputSize).map(Array[Any](_)): _*)
  }

}

// Supported by interpreted, slotted, pipelined
trait OptionalFailureTestBase[CONTEXT <: RuntimeContext] {
  self: OptionalTestBase[CONTEXT] =>

  test("should cancel outstanding work") {
    // given
    val stream = createBatchedInputValues().stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("x / 0 = 0")
      .optional()
      .input(variables = Seq("x"))
      .build()

    intercept[ArithmeticException] {
      consume(execute(logicalQuery, runtime, stream))
    }
  }

}
