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

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.TransientTransactionFailureException
import org.neo4j.kernel.DeadlockDetectedException

import java.util.concurrent.Callable
import java.util.concurrent.Executors

class CypherIsolationIntegrationTest extends ExecutionEngineFunSuite {

  val THREADS = 50
  val UPDATES = 100

  test("should not observe missing/double reads on expand with concurrently turning node dense") {
    val res1 = execute("CREATE (c:Container {Id: randomUUID()}) RETURN c.Id as Id")
    val containerId = res1.single("Id")

    // This needs to be < dense threshold.
    val constituent1Count = graph.config().get(GraphDatabaseSettings.dense_node_threshold) - 10
    val constituent1Ids = 0 until constituent1Count

    def createConstituentQuery(labelSuffix: Int): String =
      s"""
         |MATCH(container:Container { Id: $$container_id })
         |MERGE (container)-[:Owns]->(constituent:Constituent${labelSuffix}{Id:$$id})
         |FINISH
         |""".stripMargin

    for (id <- constituent1Ids) {
      execute(createConstituentQuery(1), Map("container_id" -> containerId, "id" -> id))
    }

    val query =
      """
        |OPTIONAL MATCH(container:Container {Id: $container_id})
        |UNWIND $entities AS entity
        |OPTIONAL MATCH(container)-[:Owns]->(c{Id:entity.id})
        |WHERE
        |  (c:Constituent1 AND entity.label = 'Constituent1')
        |WITH container, {id: entity.id, found: c IS NOT NULL} AS constituentFound
        |RETURN container IS NOT NULL AS containerFound, collect(constituentFound) as found
        |""".stripMargin
    val queryParams = Map(
      "container_id" -> containerId,
      "entities" -> constituent1Ids.map(id => Map[String, Any]("id" -> id, "label" -> "Constituent1"))
    )

    runTestConcurrently(
      query,
      createConstituentQuery(2),
      queryParams,
      (threadId, runId) => Map("container_id" -> containerId, "id" -> (100 * threadId + runId)),
      threads = 1,
      runsPerThread = 50
    ) { res2 =>
      val singleRow = res2.single
      singleRow("containerFound") should equal(true)
      val constituents = res2.single("found").asInstanceOf[Seq[Map[String, Any]]]
      val missing = constituents.collect {
        case c if !c("found").asInstanceOf[Boolean] => c("id")
      }
      val duplicates = constituents.map(_("id"))
        .groupBy(identity)
        .values
        .filter(_.size > 1)
        .flatten

      withClue(s"Duplicates $duplicates, missing ids: $missing\n") {
        missing shouldBe empty
        duplicates shouldBe empty
      }
    }
  }

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

    def testToRun(res: RewindableExecutionResult): Unit = {
      res.columnAs[Long]("x").toList.sliding(2).foreach {
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
    def testToRun(res: RewindableExecutionResult): Unit = {
      val list = res.columnAs[Long]("x").toList
      list.distinct should equal(list)
    }
    runTestConcurrently(query, scrambler)(testToRun)
  }

  private def runTestConcurrently(
    query: String,
    scrambler: String,
    queryParams: Map[String, Any] = Map(),
    // (ThreadId, RunId) => params for the scrambler query.
    scramblerParams: (Int, Int) => Map[String, Any] = (_, _) => Map(),
    // We need fewer Threads to reproduce the problem
    threads: Int = 10,
    // Expensive, so let's have fewer runs
    runsPerThread: Int = 3
  )(testToRun: RewindableExecutionResult => Unit): Unit = {
    // When
    val executor = Executors.newFixedThreadPool(threads)
    // Run the scrambler concurrently
    val futures = (1 to threads) map { threadId =>
      executor.submit(new Callable[Unit] {
        override def call(): Unit = {
          for (runId <- 1 to runsPerThread) {
            var retry = true
            while (retry) {
              try {
                execute(scrambler, scramblerParams(threadId, runId))
                retry = false
              } catch {
                case _: TransientTransactionFailureException =>
                case t: Throwable                            => throw new RuntimeException(t)
              }
            }
          }
        }
      })
    }

    // And while waiting for all scrambler futures threads to be done,
    // execute the read query and assert on the results
    try {
      var i = 0
      while (futures.exists(!_.isDone)) {
        var retry = true
        while (retry) {
          try {
            val res = execute(query, queryParams)
            testToRun(res)
            retry = false
          } catch {
            case _: TransientTransactionFailureException =>
            case t: Throwable                            => throw new RuntimeException(t)
          }
        }
        i += 1
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
          for (_ <- 1 to UPDATES) {
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
