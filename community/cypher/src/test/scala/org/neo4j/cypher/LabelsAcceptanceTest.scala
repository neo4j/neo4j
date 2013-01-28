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

  @Test def `Adding single literal label`() {
    assertThat("create n = {} add n:FOO", List("FOO"))
    assertThat("create n = {} add n label :FOO", List("FOO"))
    assertThat("create n = {} add n :FOO", List("FOO"))
  }

  @Test def `Adding multiple literal labels`() {
    assertThat("create n = {} add n label :FOO:BAR", List("FOO", "BAR"))
    assertThat("create n = {} add n:FOO:BAR", List("FOO", "BAR"))
    assertThat("create n = {} add n :FOO :BAR", List("FOO", "BAR"))
    assertThat("create n = {} add n :FOO:BAR", List("FOO", "BAR"))
  }

  @Test def `Adding labels using an expression`() {
    createLabeledNode("FOO", "BAR")
    assertThat("start x=node(1) create n = {} add n label labels(x)", List("FOO", "BAR"))
    assertThat("create n = {} add n label [:FOO, :BAR]", List("FOO", "BAR"))
  }

  @Test def `Creating nodes with literal labels`() {
    assertThat("CREATE node LABEL [:BAR, :FOO] VALUES {name: 'Jacob'}", List("BAR", "FOO"))
    assertThat("CREATE node :FOO:BAR {name: 'Stefan'}", List("FOO", "BAR"))
    assertThat("CREATE node:FOO:BAR VALUES {name: 'Mattias'}", List("FOO", "BAR"))
    assertThat("CREATE (n:Person)-[:OWNS]->(x:Dog) RETURN n AS node", List("Person"))
  }

  @Test def `Recreating and labelling the same node twice is forbidden`() {
    assertDoesNotWork("CREATE (n: FOO)-[:test]->b, (n: BAR)-[:test2]->c")
    assertDoesNotWork("CREATE (n LABEL :Bar)-[:OWNS]->(n LABEL :Car)")
    assertDoesNotWork("CREATE n :Foo CREATE (n :Bar)-[:OWNS]->(x:Dog)")
    assertDoesNotWork("CREATE n {} CREATE (n :Bar)-[:OWNS]->(x:Dog)")
    assertDoesNotWork("CREATE n :Foo CREATE (n {})-[:OWNS]->(x:Dog)")
  }

  @Test def `Creating nodes with labels from expressions`() {
    assertThat("START n=node(0) WITH [:FOO,:BAR] as lbls CREATE node LABEL lbls", List("FOO", "BAR"))
    assertThat("CREATE (n LABEL [:FOO, :BAR] VALUES {name:'Mattias'})-[:FOO]->x:Person RETURN n AS node", List("FOO", "BAR"))
  }

  @Test def `Add labels to nodes in a foreach`() {
    assertThat("CREATE a,b,c WITH [a,b,c] as nodes FOREACH(n in nodes : ADD n label :FOO:BAR)", List("FOO", "BAR"))
  }

  @Test def Using_labels_in_RETURN_clauses() {
    assertThat("START n=node(0) RETURN labels(n)", List())
    assertThat("START n=node(0) ADD n LABEL :FOO RETURN labels(n)", List("FOO"))
    assertThat("START n = node(0) RETURN :FOO", List("FOO"))
    assertThat("START n = node(0) RETURN [:FOO, :BAR]", List("FOO", "BAR"))
  }

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
                }
      }
    }

    graph.shutdown()

    graph = new ImpermanentGraphDatabase() with Snitch
    refNode = graph.getReferenceNode
    executionEngineHelperInit()
  }

  def assertDoesNotWork(s: String) {
    intercept[Exception](assertThat(s, List.empty))
  }
}