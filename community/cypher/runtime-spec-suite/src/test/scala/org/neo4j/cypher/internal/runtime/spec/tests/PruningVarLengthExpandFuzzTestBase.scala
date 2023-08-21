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
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.tests.PruningVarLengthExpandFuzzTestBase.REL
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType

import scala.collection.immutable.IndexedSeq
import scala.util.Random

abstract class PruningVarLengthExpandFuzzTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT]
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {
  private val population: Int = 1000
  private val seed = System.currentTimeMillis()
  private val random = new Random(seed)

  test("random and compare") {
    val nodes = setUpGraph(15 + random.nextInt(10))
    val millisToRun = 10000

    withClue("seed used: " + seed) {
      runForMillis(millisToRun)(() => testPruningVarExpand(nodes(random.nextInt(population)), random))
      runForMillis(millisToRun)(() => testBFSPruningVarExpand(nodes(random.nextInt(population)), BOTH, random))
      runForMillis(millisToRun)(() => testBFSPruningVarExpand(nodes(random.nextInt(population)), OUTGOING, random))
      runForMillis(millisToRun)(() => testBFSPruningVarExpand(nodes(random.nextInt(population)), INCOMING, random))
    }
  }

  private def runForMillis(millisToRun: Long)(f: () => Unit): Unit = {
    val start = System.currentTimeMillis()
    while (System.currentTimeMillis() - start < millisToRun) {
      f()
    }
  }

  private def setUpGraph(friendCount: Int): IndexedSeq[Node] = {

    var count = 0

    def checkAndSwitch(): Unit = {
      count += 1
      if (count == 1000) {
        runtimeTestSupport.restartTx()
        count = 0
      }
    }

    val nodes = (0 to population) map { _ =>
      checkAndSwitch()
      tx.createNode()
    }

    for {
      n1 <- nodes
      friends = random.nextInt(friendCount)
      _ <- 0 to friends
      n2 = nodes(random.nextInt(population))
    } {
      checkAndSwitch()
      tx.getNodeById(n1.getId).createRelationshipTo(n2, REL)
    }

    runtimeTestSupport.restartTx()

    nodes
  }

  private def testPruningVarExpand(startNode: Node, r: Random): Unit = {
    val min = r.nextInt(3)
    val max = min + 1 + r.nextInt(3)

    val pruningQuery = new LogicalQueryBuilder(this)
      .produceResults("from", "to")
      .pruningVarExpand(s"(from)-[*$min..$max]-(to)")
      .input(nodes = Seq("from"))
      .build()

    val distinctQuery = new LogicalQueryBuilder(this)
      .produceResults("from", "to")
      .distinct("from AS from", "to AS to")
      .expand(s"(from)-[*$min..$max]-(to)")
      .input(nodes = Seq("from"))
      .build()

    val distinctVarExpandResult =
      consume(execute(distinctQuery, runtime, inputValues(Array(startNode)))).map(_.toList).toSet
    val pruningVarExpandResult =
      consume(execute(pruningQuery, runtime, inputValues(Array(startNode)))).map(_.toList).toSet

    if (distinctVarExpandResult != pruningVarExpandResult) {
      val missingFromNew = distinctVarExpandResult -- pruningVarExpandResult
      val shouldNotBeInNew = pruningVarExpandResult -- distinctVarExpandResult
      var message =
        s"""Failed to do PruningVarExpand
           |startNode: $startNode
           |size of old result ${distinctVarExpandResult.size}
           |size of new result ${pruningVarExpandResult.size}
           |min $min
           |max $max
           |""".stripMargin
      if (missingFromNew.nonEmpty) {
        message += s"missing from new result: ${missingFromNew.mkString(",")}\n"
      }
      if (shouldNotBeInNew.nonEmpty) {
        message += s"should not be in new result: ${shouldNotBeInNew.mkString(",")}\n"
      }

      fail(message)
    }
  }

  private def testBFSPruningVarExpand(startNode: Node, direction: SemanticDirection, r: Random): Unit = {
    val min = r.nextInt(2)
    val max = min + 1 + r.nextInt(3)

    val pattern = direction match {
      case OUTGOING                   => s"(from)-[*$min..$max]->(to)"
      case SemanticDirection.INCOMING => s"(from)<-[*$min..$max]-(to)"
      case SemanticDirection.BOTH     => s"(from)-[*$min..$max]-(to)"
    }
    val pruningQuery = new LogicalQueryBuilder(this)
      .produceResults("from", "to")
      .bfsPruningVarExpand(pattern)
      .input(nodes = Seq("from"))
      .build()

    val distinctQuery = new LogicalQueryBuilder(this)
      .produceResults("from", "to")
      .distinct("from AS from", "to AS to")
      .expand(pattern)
      .input(nodes = Seq("from"))
      .build()

    val distinctVarExpandResult =
      consume(execute(distinctQuery, runtime, inputValues(Array(startNode)))).map(_.toList).toSet
    val pruningVarExpandResult =
      consume(execute(pruningQuery, runtime, inputValues(Array(startNode)))).map(_.toList).toSet

    if (distinctVarExpandResult != pruningVarExpandResult) {
      val missingFromNew = distinctVarExpandResult -- pruningVarExpandResult
      val shouldNotBeInNew = pruningVarExpandResult -- distinctVarExpandResult
      var message =
        s"""Failed to do $direction BFSPruningVarExpand
           |startNode: $startNode
           |size of old result ${distinctVarExpandResult.size}
           |size of new result ${pruningVarExpandResult.size}
           |min $min
           |max $max
           |""".stripMargin
      if (missingFromNew.nonEmpty) {
        message += s"missing from new result: ${missingFromNew.mkString(",")}\n"
      }
      if (shouldNotBeInNew.nonEmpty) {
        message += s"should not be in new result: ${shouldNotBeInNew.mkString(",")}\n"
      }
      fail(message)
    }
  }
}

object PruningVarLengthExpandFuzzTestBase {
  val REL: RelationshipType = RelationshipType.withName("REL")
}
