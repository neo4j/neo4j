/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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

  protected def assertSeekMatchFor(value: Value, nodes: Node*): Unit = {
    val query = "MATCH (n:%s) WHERE n.%s = $%s RETURN n".format(LABEL, PROPERTY, PROPERTY)
    testIndexedRead(query, Map(PROPERTY -> value.asObject()), "NodeIndexSeek", nodes)
  }

  protected def assertScanMatch(nodes: Node*): Unit = {
    val query = "MATCH (n:%s) WHERE EXISTS(n.%s) RETURN n".format(LABEL, PROPERTY)
    testIndexedRead(query, Map(), "NodeIndexScan", nodes)
  }

  protected def assertRangeScanFor(op1: String, bound1: Value, op2: String, bound2: Value, nodes: Node*): Unit = {
    val predicate1 = s"n.$PROPERTY $op1 $$param1"
    val predicate2 = s"n.$PROPERTY $op2 $$param2"
    val query = s"MATCH (n:$LABEL) WHERE $predicate1 AND $predicate2 RETURN n"
    testIndexedRead(query, Map("param1" -> bound1.asObject(), "param2" -> bound2.asObject()), "NodeIndexSeekByRange", nodes)
  }

  private def testIndexedRead(query: String, params: Map[String, AnyRef], wantedOperator: String, expected: Seq[Node]): Unit = {
    if (cypherComparisonSupport) {
      val result =
        executeWith(
          TestConfiguration(
            Versions(Versions.V3_4, Versions.V3_3, Versions.Default),
            Planners(Planners.Cost, Planners.Default),
            Runtimes(Runtimes.Interpreted, Runtimes.Slotted, Runtimes.Default)
          ),
          query,
          params = params,
          planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators(wantedOperator),
                                                             expectPlansToFail = Configs.AllRulePlanners)
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
      plan should include (s":$LABEL($PROPERTY)")
    }


  }
}
