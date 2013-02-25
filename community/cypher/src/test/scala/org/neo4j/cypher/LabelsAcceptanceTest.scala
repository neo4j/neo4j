/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher

import internal.helpers.CollectionSupport
import org.junit.Test
import org.scalatest.Assertions
import org.neo4j.test.ImpermanentGraphDatabase
import org.neo4j.graphdb.Node

class LabelsAcceptanceTest extends ExecutionEngineHelper with StatisticsChecker with Assertions with CollectionSupport {

  @Test def Adding_single_literal_label() {
    assertThat("create n = {} set n:FOO", List("FOO"))
    assertThat("create n = {} set n :FOO", List("FOO"))
  }

  @Test def Adding_multiple_literal_labels() {
    assertThat("create n = {} set n:FOO:BAR", List("FOO", "BAR"))
    assertThat("create n = {} set n :FOO :BAR", List("FOO", "BAR"))
    assertThat("create n = {} set n :FOO:BAR", List("FOO", "BAR"))
  }

  @Test def Creating_nodes_with_literal_labels() {
    assertThat("CREATE node :FOO:BAR {name: 'Stefan'}", List("FOO", "BAR"))
    assertThat("CREATE node:FOO:BAR VALUES {name: 'Mattias'}", List("FOO", "BAR"))
    assertThat("CREATE (n:Person)-[:OWNS]->(x:Dog) RETURN n AS node", List("Person"))
  }

  @Test def Recreating_and_labelling_the_same_node_twice_is_forbidden() {
    assertDoesNotWork("CREATE (n: FOO)-[:test]->b, (n: BAR)-[:test2]->c")
    assertDoesNotWork("CREATE n :Foo CREATE (n :Bar)-[:OWNS]->(x:Dog)")
    assertDoesNotWork("CREATE n {} CREATE (n :Bar)-[:OWNS]->(x:Dog)")
    assertDoesNotWork("CREATE n :Foo CREATE (n {})-[:OWNS]->(x:Dog)")
  }

  @Test def Add_labels_to_nodes_in_a_foreach() {
    assertThat("CREATE a,b,c WITH [a,b,c] as nodes FOREACH(n in nodes : SET n :FOO:BAR)", List("FOO", "BAR"))
  }

  @Test def Using_labels_in_RETURN_clauses() {
    assertThat("START n=node(0) RETURN labels(n)", List())
    assertThat("START n=node(0) SET n :FOO RETURN labels(n)", List("FOO"))
  }


  @Test def Removing_labels() {
    usingLabels("FOO", "BAR").
      assertThat("START n=node({node}) REMOVE n :FOO RETURN n").
      returnsLabels("BAR")

    usingLabels("FOO", "BAR").
      assertThat("START n=node({node}) REMOVE n:FOO RETURN n").
      returnsLabels("BAR")

    usingLabels("FOO").
      assertThat("START n=node({node}) REMOVE n:BAR RETURN n").
      returnsLabels("FOO")
  }


  private class AssertThat(labels: Seq[String], query:String) {
    def returnsLabels(expected:String*):AssertThat = {
      val node = createLabeledNode(labels:_*)
      val result = executeScalar[Node](query, "node"->node)

      assert(result.labels === expected.toList)
      this
    }
  }

  private class UsingLabels(labels:Seq[String]) {
    def assertThat(q:String):AssertThat = new AssertThat(labels, q)
  }

  private def usingLabels(labels:String*):UsingLabels = new UsingLabels(labels)
 
 /*
 Removing multiple literal labels
 REMOVE n :BAR:BAZ
 REMOVE n:BAR:BAZ

 Removing multiple literal labels using an expression
 REMOVE n LABEL <expr>

   */

  /* STILL TO DO
   Setting labels literally

 Removing a single literal label
 REMOVE n LABEL :FOO
 REMOVE n:FOO

 Removing multiple literal labels
 REMOVE n :BAR:BAZ
 REMOVE n:BAR:BAZ

 Removing multiple literal labels using an expression
 REMOVE n LABEL <expr>


 Matching
 Expressing label requirements in match without negation
 MATCH n:Person-->x:Person
 MATCH (n:Person:Husband|:BAR)-[:FOO|:BAR]->x:Person
 MATCH n:Person|:Husband-[:FOO|:BAR]->x:Person

 Note:
 Concatenation (logical and) binds tighter than |, so a b|c is (a and b) or c

 Write predicate that tests for the existence of a single label
 MATCH n-->x:Person
 WHERE :Person IN labels(n) // arbitrary label expression

 Write predicate that tests for the existence of one of multiple labels
 WHERE n:Male OR n:Female
 WHERE n:Male|:Female

 Write predicate that tests for existence of labels from collection
 MATCH n:Foo-->x<--m:Bar
 WHERE ALL(l in labels(x): l in labels(n) and l in labels(m))

 Reusing the label predicate sublanguage from match (simple case)
 MATCH n-->x<--m:Bar
 WHERE n:Animal or (n:Person and n.income > 0)

 Reusing the label predicate sublanguage from match (complex case)
 MATCH n-->x<--m:Bar
 WHERE n:Animal or (n:Person:Chef and n.income > 0)


 Returning label membership of a node
 START n=node(0) WHERE n:FOO RETURN n:BAR, n:BAR|:baz
*/

  private def assertThat(q: String, expectedLabels: List[String]) {
    val result = parseAndExecute(q)

    if (result.isEmpty) {
      val n = graph.getNodeById(1)
      assert(n.labels === expectedLabels)
    } else {
      result.foreach {
        map => map.get("node") match {
                  case None =>
                    assert(makeTraversable(map.head._2).toList === expectedLabels)

                  case Some(n:Node) =>
                    assert(n.labels === expectedLabels)

                  case _ =>
                    throw new AssertionError("assertThat used with result that is not a node")
                }
      }
    }

    insertNewCleanDatabase()
  }


  private def insertNewCleanDatabase() {
    graph.shutdown()

    graph = new ImpermanentGraphDatabase() with Snitch
    refNode = graph.getReferenceNode
    executionEngineHelperInit()
  }

  def assertDoesNotWork(s: String) {
    intercept[Exception](assertThat(s, List.empty))
  }
}