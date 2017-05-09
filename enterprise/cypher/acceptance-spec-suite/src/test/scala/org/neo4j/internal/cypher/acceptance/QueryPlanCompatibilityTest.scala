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

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}
import org.neo4j.graphdb.{Label, Node, RelationshipType}

class QueryPlanCompatibilityTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("should produce compatible plans for simple MATCH node query") {
    executeWithCompatibilityAndAssertSimilarPlans("MATCH (n:Person) RETURN n")
  }

  test("should produce compatible plans for simple MATCH relationship query") {
    executeWithCompatibilityAndAssertSimilarPlans("MATCH (n:Person)-[r:KNOWS]->(m) RETURN r")
  }

  test("should produce compatible plans with predicates") {
    executeWithCompatibilityAndAssertSimilarPlans(
      """
        |MATCH (n:Person) WHERE n.name STARTS WITH 'Joe' AND n.age >= 42
        |RETURN count(n)
      """.stripMargin)
  }

  test("should produce compatible plans with unwind") {
    executeWithCompatibilityAndAssertSimilarPlans(
      """
        |WITH 'Joe' as name
        |UNWIND [42,43,44] as age
        |MATCH (n:Person) WHERE n.name STARTS WITH name AND n.age >= age
        |RETURN count(n)
      """.stripMargin)
  }

  // Too much has changed since 2.3, but this test might make sense against a more recent version
  ignore("should produce compatible plans for complex query") {
    executeWithCompatibilityAndAssertSimilarPlans(
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
}
