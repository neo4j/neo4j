/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.internal.cypher.acceptance

import java.util.stream.Collectors

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._
import org.neo4j.values.storable.Value

import scala.collection.JavaConversions._

trait IndexingTestSupport extends ExecutionEngineFunSuite with CypherComparisonSupport {

  protected val LABEL: String = "Label"
  protected val PROPERTY: String = "prop"
  protected val NONINDEXED: String = "nonindexed"

  def cypherComparisonSupport: Boolean

  protected def createIndex(): Unit = {
    graph.createIndex(LABEL, PROPERTY)
  }

  protected def dropIndex(): Unit = {
    graph.execute(s"DROP INDEX ON :$LABEL($PROPERTY)")
  }

  protected def createIndexedNode(value: Value): Node = {
    createLabeledNode(Map(PROPERTY -> value.asObject()), LABEL)
  }

  protected def setIndexedValue(node: Node, value: Value): Unit = {
    graph.inTx {
      node.setProperty(PROPERTY, value.asObject())
    }
  }

  protected def setNonIndexedValue(node: Node, value: Value): Unit = {
    graph.inTx {
      node.setProperty(NONINDEXED, value.asObject())
    }
  }

  protected def assertSeekMatchFor(value: Value, nodes: Node*): Unit = {
    val query = s"MATCH (n:$LABEL) WHERE n.$PROPERTY = $$param RETURN n"
    testRead(query, Map("param" -> value.asObject()), "NodeIndexSeek", nodes, config = Configs.All - Configs.OldAndRule)
  }

  protected def assertScanMatch(nodes: Node*): Unit = {
    val query = s"MATCH (n:$LABEL) WHERE EXISTS(n.$PROPERTY) RETURN n"
    testRead(query, Map(), "NodeIndexScan", nodes)
  }

  protected def assertRangeScanFor(op: String, bound: Value, nodes: Node*): Unit = {
    val predicate = s"n.$PROPERTY $op $$param"
    val query = s"MATCH (n:$LABEL) WHERE $predicate RETURN n"
    testRead(query, Map("param" -> bound.asObject()), "NodeIndexSeekByRange", nodes)
  }

  protected def assertLabelRangeScanFor(op: String, bound: Value, nodes: Node*): Unit = {
    val predicate = s"n.$NONINDEXED $op $$param"
    val query = s"MATCH (n:$LABEL) WHERE $predicate RETURN n"
    testRead(query, Map("param" -> bound.asObject()), "NodeByLabelScan", nodes)
  }

  protected def assertRangeScanFor(op1: String, bound1: Value, op2: String, bound2: Value, nodes: Node*): Unit = {
    val predicate1 = s"n.$PROPERTY $op1 $$param1"
    val predicate2 = s"n.$PROPERTY $op2 $$param2"
    val query = s"MATCH (n:$LABEL) WHERE $predicate1 AND $predicate2 RETURN n"
    testRead(query, Map("param1" -> bound1.asObject(), "param2" -> bound2.asObject()), "NodeIndexSeekByRange", nodes)
  }

  private def testRead(query: String, params: Map[String, AnyRef], wantedOperator: String, expected: Seq[Node],
                       config: TestConfiguration = Configs.Interpreted - Configs.OldAndRule): Unit = {
    if (cypherComparisonSupport) {
      val result =
        executeWith(
          config,
          query,
          params = params,
          planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators(wantedOperator))
        )
      val nodes = result.columnAs("n").toSet
      expected.foreach(p => assert(nodes.contains(p)))
      nodes.size() should be(expected.size)
    } else {
      val result = graph.execute("CYPHER runtime=slotted "+query, params)
      val nodes = result.columnAs("n").stream().collect(Collectors.toSet)
      expected.foreach(p => assert(nodes.contains(p)))
      nodes.size() should be(expected.size)

      val plan = result.getExecutionPlanDescription.toString
      plan should include(wantedOperator)
    }
  }
}
