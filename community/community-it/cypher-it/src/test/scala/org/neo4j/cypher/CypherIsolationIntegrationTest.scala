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
package org.neo4j.cypher

import org.neo4j.graphdb.Node
import org.neo4j.kernel.DeadlockDetectedException

import java.util.concurrent.Callable
import java.util.concurrent.Executors

class CypherIsolationIntegrationTest extends ExecutionEngineFunSuite {

  val THREADS = 50
  val UPDATES = 100

  test("Should work around read isolation limitation for simple incremental query") {
    // Given
    val n = createNode("x" -> 0L)

    // When
    race("MATCH (n) SET n.x = n.x + 1")

    // Then
    nodeGetProperty(n, "x") should equal(THREADS * UPDATES)
  }

  test("Should work around read isolation limitation using explicit lock") {
    // Given
    val n = createLabeledNode(Map("x" -> 0L), "L")

    val query =
      """MATCH (n:L) WHERE n.x IS NOT NULL
        |SET n._LOCK_ = true
        |WITH n, n.x AS x
        |SET n.x = x + 1
        |REMOVE n._LOCK_""".stripMargin

    // When
    race(query)

    // Then
    nodeGetProperty(n, "x") should equal(THREADS * UPDATES)
  }

  test("Should work around read isolation limitations using explicit lock for cached node properties") {
    // Given
    val n = createLabeledNode(Map("x" -> 0L), "L")
    graph.createNodeIndex("L", "x")

    val query =
      """MATCH (n:L) WHERE n.x IS NOT NULL
        |SET n._LOCK_ = true
        |WITH n, n.x AS x
        |SET n.x = x + 1
        |REMOVE n._LOCK_""".stripMargin

    // When
    race(query)

    // Then
    nodeGetProperty(n, "x") should equal(THREADS * UPDATES)
  }

  test("Should work around read isolation limitations using explicit lock for cached node properties with map +=") {
    // Given
    val n = createLabeledNode(Map("x" -> 0L), "L")
    graph.createNodeIndex("L", "x")

    val query =
      """MATCH (n:L) WHERE n.x IS NOT NULL
        |SET n += {_LOCK_: true}
        |WITH n, n.x AS x
        |SET n.x = x + 1
        |REMOVE n._LOCK_""".stripMargin

    // When
    race(query)

    // Then
    nodeGetProperty(n, "x") should equal(THREADS * UPDATES)
  }

  test("Should order correctly using an index despite concurrent updates") {
    // Given
    execute(
      """
        |UNWIND range(0,1000) AS i
        |CREATE (:L {x: i})
        |""".stripMargin
    )
    graph.createNodeIndex("L", "x")

    // This query will not plan a Sort.
    // We test whether concurrent updates to the properties can
    // lead to results in the wrong order.
    val query =
      """CYPHER
        |MATCH (n:L) WHERE n.x IS NOT NULL
        |RETURN n.x AS x ORDER BY n.x
        |""".stripMargin

    // This query assigns new property values to each node.
    val scrambler =
      """
        |MATCH (n:L)
        |WITH n ORDER BY rand()
        |WITH collect(n) AS items
        |UNWIND range(0, 1000) AS index
        |WITH index, items[index] as n
        |WITH *, n.x as prev
        |SET n.x = index
        |RETURN prev, index
        |""".stripMargin

    def testToRun(res: List[Long]): Unit = {
      res.sliding(2).foreach {
        case List(a, b) =>
          a should be <= b
        case _ =>
      }
    }
    runTestConcurrently(query, scrambler)(testToRun)
  }

  test("Should return correctly deduplicated results despite concurrent updates") {
    // Given
    execute(
      """
        |UNWIND range(0,1000) AS i
        |CREATE (:L {x: i})
        |""".stripMargin
    )
    graph.createNodeIndex("L", "x")

    // This query used to not plan a Distinct. But it needs to.
    // We test whether concurrent updates to the properties can
    // lead to results that are not distinct
    val query =
      """CYPHER
        |MATCH (n:L) WHERE n.x IS NOT NULL
        |WITH DISTINCT n
        |RETURN id(n) AS x
        |""".stripMargin

    // This query assigns new property values to each node.
    val scrambler =
      """
        |MATCH (n:L)
        |WITH n ORDER BY rand()
        |WITH collect(n) AS items
        |UNWIND range(0, 1000) AS index
        |WITH index, items[index] as n
        |WITH *, n.x as prev
        |SET n.x = index
        |RETURN prev, index
        |""".stripMargin
    def testToRun(res: List[Long]): Unit = res.distinct should equal(res)
    runTestConcurrently(query, scrambler)(testToRun)
  }

  private def runTestConcurrently(query: String, scrambler: String)(testToRun: List[Long] => Unit): Unit = {
    // We need fewer Threads to reproduce the problem
    val THREADS = 10
    // When
    val executor = Executors.newFixedThreadPool(THREADS)
    // Run the scrambler concurrently
    val futures = (1 to THREADS) map { _ =>
      executor.submit(new Callable[Unit] {
        override def call(): Unit = {
          // Expensive, so let's have fewer runs
          for (_ <- 1 to 3) {
            var retry = true
            while (retry) {
              try {
                execute(scrambler)
                retry = false
              } catch {
                case _: DeadlockDetectedException =>
                case t: Throwable                 => throw new RuntimeException(t)
              }
            }
          }
        }
      })
    }

    // And while waiting for all scrambler futures threads to be done,
    // execute the read query and assert that results are distinct
    try {
      while (futures.exists(!_.isDone)) {
        var retry = true
        while (retry) {
          try {
            val res = execute(query).columnAs[Long]("x").toList
            testToRun(res)
            retry = false
          } catch {
            case _: DeadlockDetectedException =>
            case t: Throwable                 => throw new RuntimeException(t)
          }
        }
      }
    } finally {
      executor.shutdown()
    }
  }

  private def race(query: String): Unit = {
    val executor = Executors.newFixedThreadPool(THREADS)

    val futures = (1 to THREADS) map { x =>
      executor.submit(new Callable[Unit] {
        override def call(): Unit = {
          for (x <- 1 to UPDATES) {
            var retry = true
            while (retry) {
              try {
                execute(query)
                retry = false
              } catch {
                case e: DeadlockDetectedException => e
                case t: Throwable                 => throw new RuntimeException(t)
              }
            }
          }
        }
      })
    }

    try {
      futures.foreach(_.get())
    } finally executor.shutdown()
  }

  private def nodeGetProperty(node: Node, property: String): Long = {
    graph.withTx(tx => {
      tx.getNodeById(node.getId).getProperty(property).asInstanceOf[Long]
    })
  }

}
