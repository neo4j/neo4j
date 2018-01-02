/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compiler.v2_3.helpers.CollectionSupport
import org.neo4j.cypher.{ExecutionEngineFunSuite, QueryStatisticsTestSupport}
import org.neo4j.graphdb.Node
import org.neo4j.kernel.impl.store.NeoStores
import org.scalatest.Assertions
import org.scalautils.LegacyTripleEquals

class LabelsAcceptanceTest extends ExecutionEngineFunSuite
  with QueryStatisticsTestSupport with Assertions with CollectionSupport with LegacyTripleEquals {

  test("Adding_single_literal_label") {
    assertThat("create (n {}) set n:FOO", List("FOO"))
    assertThat("create (n {}) set n :FOO", List("FOO"))
  }

  test("Adding_multiple_literal_labels") {
    assertThat("create (n {}) set n:FOO:BAR", List("FOO", "BAR"))
    assertThat("create (n {}) set n :FOO :BAR", List("FOO", "BAR"))
    assertThat("create (n {}) set n :FOO:BAR", List("FOO", "BAR"))
  }

  test("Creating_nodes_with_literal_labels") {
    assertDoesNotWork("CREATE node :FOO:BAR {name: 'Stefan'}")
    assertDoesNotWork("CREATE node :FOO:BAR")
    assertThat("CREATE node", List())
    assertThat("CREATE (node :FOO:BAR)", List("FOO", "BAR"))
    assertThat("CREATE (node:FOO:BAR {name: 'Mattias'})", List("FOO", "BAR"))
    assertThat("CREATE (n:Person)-[:OWNS]->(x:Dog) RETURN n AS node", List("Person"))
  }

  test("Recreating_and_labelling_the_same_node_twice_differently_is_forbidden") {
    assertDoesNotWork("CREATE (n: FOO)-[:test]->b, (n: BAR)-[:test2]->c")
    assertDoesNotWork("CREATE (c)<-[:test2]-(n: FOO), (n: BAR)<-[:test]-(b)")
    assertDoesNotWork("CREATE n :Foo CREATE (n :Bar)-[:OWNS]->(x:Dog)")
    assertDoesNotWork("CREATE n {} CREATE (n :Bar)-[:OWNS]->(x:Dog)")
    assertDoesNotWork("CREATE n :Foo CREATE (n {})-[:OWNS]->(x:Dog)")
  }

  test("Add_labels_to_nodes_in_a_foreach") {
    assertThat("CREATE a,b,c WITH [a,b,c] as nodes FOREACH(n in nodes | SET n :FOO:BAR)", List("FOO", "BAR"))
  }

  test("Using_labels_in_RETURN_clauses") {
    createNode()
    assertThat("match (n) where id(n) = 0 RETURN labels(n)", List())

    createNode()
    assertThat("match (n) where id(n) = 0 SET n :FOO RETURN labels(n)", List("FOO"))
  }


  test("Removing_labels") {
    usingLabels("FOO", "BAR").
      assertThat("START n=node({node}) REMOVE n:FOO RETURN n").
      returnsLabels("BAR")

    usingLabels("FOO").
      assertThat("START n=node({node}) REMOVE n:BAR RETURN n").
      returnsLabels("FOO")
  }

  test("should not create labels id when trying to delete non-existing lables") {
    createNode()
    val result = execute("MATCH n REMOVE n:BAR RETURN id(n) as id").toList
    result should equal(List(Map("id" -> 0)))

    graph.inTx {
      graph.getDependencyResolver.resolveDependency(classOf[NeoStores]).getLabelTokenStore.getHighId should equal(0)
    }
  }

  private class AssertThat(labels: Seq[String], query:String) {
    def returnsLabels(expected:String*):AssertThat = {
      val node = createLabeledNode(labels:_*)
      val result = executeScalar[Node](query, "node"->node)


      assertInTx(result.labels === expected.toList)
      this
    }
  }

  private class UsingLabels(labels:Seq[String]) {
    def assertThat(q:String):AssertThat = new AssertThat(labels, q)
  }

  private def usingLabels(labels:String*):UsingLabels = new UsingLabels(labels)

  private def assertThat(q: String, expectedLabels: List[String]) {
    val result = execute(q).toList

    graph.inTx {
      if (result.isEmpty) {
        val n = graph.getNodeById(0)
        assert(n.labels === expectedLabels)
      } else {
        result.foreach {
          map => map.get("node") match {
            case None =>
              assert(makeTraversable(map.head._2).toList === expectedLabels)

            case Some(n: Node) =>
              assert(n.labels === expectedLabels)

            case _ =>
              throw new AssertionError("assertThat used with result that is not a node")
          }
        }
      }
    }

    insertNewCleanDatabase()
  }


  private def insertNewCleanDatabase() {
    stopTest()
    initTest()
  }

  def assertDoesNotWork(s: String) {
    intercept[Exception](assertThat(s, List.empty))
  }
}
