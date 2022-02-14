/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
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
  runtime: CypherRuntime[CONTEXT],
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("random and compare") {
    val POPULATION: Int = 1 * 1000
    val seed = System.currentTimeMillis()
    val nodes = setUpGraph(seed, POPULATION, 20)
    val r = new Random

    val start = System.currentTimeMillis()

    while(System.currentTimeMillis() - start < 10000) {
      withClue("seed used: " + seed) {
        testNode(nodes(r.nextInt(POPULATION)), r)
      }
    }

  }

  private def setUpGraph(seed: Long, POPULATION: Int, friendCount: Int): IndexedSeq[Node] = {
    val r = new Random(seed)

    var count = 0

    def checkAndSwitch(): Unit = {
      count += 1
      if (count == 1000) {
        runtimeTestSupport.restartTx()
        count = 0
      }
    }

    val nodes = (0 to POPULATION) map { _ =>
      checkAndSwitch()
      tx.createNode()
    }

    for {
      n1 <- nodes
      friends = r.nextInt(friendCount)
      _ <- 0 to friends
      n2 = nodes(r.nextInt(POPULATION))
    } {
      checkAndSwitch()
      tx.getNodeById(n1.getId).createRelationshipTo(n2, REL)
    }

    runtimeTestSupport.restartTx()

    nodes
  }

  private def testNode(startNode: Node, r: Random): Unit = {
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

    val distinctVarExpandResult = consume(execute(distinctQuery, runtime, inputValues(Array(startNode)))).map(_.toList).toSet
    val pruningVarExpandResult = consume(execute(pruningQuery, runtime, inputValues(Array(startNode)))).map(_.toList).toSet

    if (distinctVarExpandResult != pruningVarExpandResult) {
      val missingFromNew = distinctVarExpandResult -- pruningVarExpandResult
      val shouldNotBeInNew = pruningVarExpandResult -- distinctVarExpandResult
      var message =
        s"""startNode: $startNode
           |size of old result ${distinctVarExpandResult.size}
           |size of new result ${pruningVarExpandResult.size}
           |min $min
           |max $max
           |""".stripMargin
      if(missingFromNew.nonEmpty) {
        message += s"missing from new result: ${missingFromNew.mkString(",")}\n"
      }
      if(shouldNotBeInNew.nonEmpty) {
        message += s"should not be in new result: ${shouldNotBeInNew.mkString(",")}\n"
      }

      fail(message)
    }
  }
}

object PruningVarLengthExpandFuzzTestBase {
  val REL: RelationshipType = RelationshipType.withName("REL")
}
