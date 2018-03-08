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

import scala.collection.JavaConversions._

trait IndexingTestSupport extends ExecutionEngineFunSuite with CypherComparisonSupport {

  protected val LABEL: String = "Label"
  protected val PROPERTY: String = "prop"

  protected def createIndex(): Unit = {
    graph.createIndex(LABEL, PROPERTY)
  }

  protected def dropIndex(): Unit = {
    graph.execute(s"DROP INDEX ON :$LABEL($PROPERTY)")
  }

  protected def createIndexedNode(value: AnyRef): Node = {
    createLabeledNode(Map(PROPERTY -> value), LABEL)
  }

  protected def setIndexedValue(node: Node, value: AnyRef): Unit = {
    graph.inTx {
      node.setProperty(PROPERTY, value)
    }
  }

  protected def assertSeekMatchFor(value: AnyRef, node: Node*): Unit = {
    val query = "CYPHER runtime=slotted MATCH (n:%s) WHERE n.%s = $%s RETURN n".format(LABEL, PROPERTY, PROPERTY)
    testIndexedRead(query, Map(PROPERTY -> value), "NodeIndexSeek", node)
  }

  protected def assertScanMatch(node: Node*): Unit = {
    val query = "CYPHER runtime=slotted MATCH (n:%s) WHERE EXISTS(n.%s) RETURN n".format(LABEL, PROPERTY)
    testIndexedRead(query, Map(), "NodeIndexScan", node)
  }

  private def testIndexedRead(query: String, params: Map[String, AnyRef], wantedOperator: String, expected: Seq[Node]): Unit = {
    val result = graph.execute(query, params)

    val nodes = result.columnAs("n").stream().collect(Collectors.toSet)
    expected.foreach(p => assert(nodes.contains(p)))
    nodes.size() should be(expected.size)

    val plan = result.getExecutionPlanDescription.toString
    plan should include(wantedOperator)
    plan should include (s":$LABEL($PROPERTY)")
  }
}
