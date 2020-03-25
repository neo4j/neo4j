/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.GraphDatabaseFunSuite
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.MapCypherRow
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper.withQueryState
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Variable
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PruningVarLengthExpandPipeTest.createVarLengthPredicate
import org.neo4j.graphdb.Node
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.query.QuerySubscriberAdapter
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.RelationshipValue

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class PruningVarLengthExpandPipeTest extends GraphDatabaseFunSuite {
  val types = RelationshipTypes(Array.empty[String])

  test("random and compare") {
    // runs DistinctVarExpand and VarExpand side-by-side and checks that the reachable nodes are the same
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

  private def setUpGraph(seed: Long, POPULATION: Int, friendCount: Int = 50): IndexedSeq[Node] = {
    val r = new Random(seed)

    var tx = graph.beginTransaction(Type.IMPLICIT, LoginContext.AUTH_DISABLED)
    var count = 0

    def checkAndSwitch(): Unit = {
      count += 1
      if (count == 1000) {
        tx.commit()
        tx = graph.beginTransaction(Type.IMPLICIT, LoginContext.AUTH_DISABLED)
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
      friendIdx <- 0 to friends
      n2 = nodes(r.nextInt(POPULATION))
    } {
      checkAndSwitch()
      tx.getNodeById(n1.getId).createRelationshipTo(n2, REL)
    }

    tx.commit()

    nodes
  }

  private def testNode(startNode: Node, r: Random): Unit = {
    val min = r.nextInt(3)
    val max = min + 1 + r.nextInt(3)
    val sourcePipe = new FakePipe(Iterator(Map("from" -> startNode)))
    val sourcePipe2 = new FakePipe(Iterator(Map("from" -> startNode)))
    val pipeUnderTest = createPipe(sourcePipe, min, max, SemanticDirection.BOTH)
    val pipe = VarLengthExpandPipe(sourcePipe2, "from", "r", "to", SemanticDirection.BOTH, SemanticDirection.BOTH, types, min, Some(max), nodeInScope = false)()
    val columns = Array("from", "to")
    val comparison = ProduceResultsPipe(DistinctPipe(pipe, Array(DistinctPipe.GroupingCol("from", Variable("from")), DistinctPipe.GroupingCol("to", Variable("to"))))(), columns)()

    val distinctExpand = graph.withTx { tx =>
      withQueryState(graph, tx, Array.empty, { queryState =>
        pipeUnderTest.createResults(queryState).toList
      })
    }

    val records = ArrayBuffer.empty[CypherRow]
    val subscriber: QuerySubscriber = new QuerySubscriberAdapter {
      private var record: mutable.Map[String, AnyValue] = mutable.Map.empty

      override def onField(offset: Int, value: AnyValue): Unit = {
          record.put(columns(offset), value)
      }
      override def onRecordCompleted(): Unit = {
        records.append(new MapCypherRow(record))
        record =  mutable.Map.empty
      }
    }
    graph.withTx { tx =>
      withQueryState(graph, tx, Array.empty, { queryState =>
        comparison.createResults(queryState).toList
      }, subscriber)
    }

    val oldThing = records.toSet
    val newThing = distinctExpand.toSet
    if (oldThing != newThing) {
      val missingFromNew = oldThing -- newThing
      val shouldNotBeInNew = newThing -- oldThing
      var message =
        s"""startNode: $startNode
            |size of old result ${oldThing.size}
            |size of new result ${newThing.size}
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

  private def createPipe(src: FakePipe, min: Int, max: Int, outgoing: SemanticDirection) = {
    PruningVarLengthExpandPipe(src, "from", "to", types, outgoing, min, max)()
  }

  private def createPipe(src: FakePipe,
                         min: Int,
                         max: Int,
                         outgoing: SemanticDirection,
                         relationshipPredicate: Predicate,
                         nodePredicate: Predicate) = {
    val filteringStep = createVarLengthPredicate(nodePredicate, relationshipPredicate)
    PruningVarLengthExpandPipe(src, "from", "to", types, outgoing, min, max, filteringStep)()
  }
}

object PruningVarLengthExpandPipeTest {
  def createVarLengthPredicate(nodePredicate: Predicate, relationshipPredicate: Predicate): VarLengthPredicate = {
    new VarLengthPredicate {
      override def filterNode(row: CypherRow, state: QueryState)(node: NodeValue): Boolean = {
        val cp = row.copyWith("to", node)
        val result = nodePredicate.isTrue(cp, state)
        result
      }

      override def filterRelationship(row: CypherRow, state: QueryState)(rel: RelationshipValue): Boolean = {
        val cp = row.copyWith("r", rel)
        val result = relationshipPredicate.isTrue(cp, state)
        result
      }

      override def predicateExpressions: Seq[Predicate] = Seq(nodePredicate, relationshipPredicate)
    }
  }
}
