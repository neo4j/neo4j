/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compiler.v3_2.executionplan.InternalExecutionResult
import org.neo4j.cypher.{NewPlannerTestSupport, ExecutionEngineFunSuite}
import org.neo4j.graphdb.{RelationshipType, Node, Label}

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
