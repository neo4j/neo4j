/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v3_2.executionplan.InternalExecutionResult
import org.neo4j.cypher.{NewPlannerTestSupport, ExecutionEngineFunSuite}
import org.neo4j.graphdb.{RelationshipType, Node, Label}

class QueryPlanCompatibilityTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("should produce compatible plans for simple MATCH node query") {
    checkCompatibility("MATCH (n:Person) RETURN n")
  }

  test("should produce compatible plans for simple MATCH relationship query") {
    checkCompatibility("MATCH (n:Person)-[r:KNOWS]->(m) RETURN r")
  }

  test("should produce compatible plans with predicates") {
    checkCompatibility(
      """
        |MATCH (n:Person) WHERE n.name STARTS WITH 'Joe' AND n.age >= 42
        |RETURN count(n)
      """.stripMargin)
  }

  test("should produce compatible plans with unwind") {
    checkCompatibility(
      """
        |WITH 'Joe' as name
        |UNWIND [42,43,44] as age
        |MATCH (n:Person) WHERE n.name STARTS WITH name AND n.age >= age
        |RETURN count(n)
      """.stripMargin)
  }

  // Too much has changed since 2.3, but this text might make sense against a more recent version
  ignore("should produce compatible plans for complex query") {
    checkCompatibility(
      """
        |WITH 'Joe' as name
        |UNWIND [42,43,44] as age
        |MATCH (n:Person) WHERE n.name STARTS WITH name AND n.age >= age
        |OPTIONAL MATCH (n)-[r:KNOWS]->(m) WHERE exists(r.since)
        |RETURN count(r)
      """.stripMargin)
  }

  private def makeData(): Unit = {
    var prev: Node = null
    Range(0, 1000).foreach { i =>
      val node = graph.createNode(Label.label("Person"))
      node.setProperty("name", s"Joe_$i")
      node.setProperty("age", 1 + i)
      if (prev != null) {
        val r = prev.createRelationshipTo(node, RelationshipType.withName("KNOWS"))
        r.setProperty("since", 1970 + i)
      }
      prev = node
    }
  }

  private def checkCompatibility(queryText: String, params: (String, Any)*) = {
    val compatibility = "2.3"
    val compatibilityResult = innerExecute(s"CYPHER $compatibility $queryText", params: _*)
    val interpretedResult = innerExecute(s"CYPHER runtime=interpreted $queryText", params: _*)
    assertResultsAreSame(compatibilityResult, interpretedResult, queryText, s"Diverging results between $compatibility and current")
    compatibilityResult.close()
    interpretedResult.close()
    assertPlansAreSame(interpretedResult, compatibilityResult, queryText, s"Diverging query plan between $compatibility and current")
  }

  protected def assertPlansAreSame(current: InternalExecutionResult, other: InternalExecutionResult, queryText: String, errorMsg: String, replaceNaNs: Boolean = false) {
    withClue(errorMsg) {
      val currentText = current.executionPlanDescription().toString
      val otherText = other.executionPlanDescription().toString
      val currentOps = current.executionPlanDescription().flatten.map(_.name.toLowerCase)
      val otherOps = other.executionPlanDescription().flatten.map(_.name.toLowerCase)
      withClue(s"$errorMsg: $currentOps != $otherOps\n$currentText\n$otherText") {
        currentOps should be(otherOps)
      }
    }
  }
}
