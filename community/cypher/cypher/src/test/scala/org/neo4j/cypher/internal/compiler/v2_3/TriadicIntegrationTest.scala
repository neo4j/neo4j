/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription
import org.scalatest.matchers.{MatchResult, Matcher}

class TriadicIntegrationTest extends ExecutionEngineFunSuite {
  private val QUERY: String = """MATCH (p1:Person)-[:FRIEND]-()-[:FRIEND]-(p2:Person)
                                |WHERE NOT (p1)-[:FRIEND]-(p2)
                                |RETURN p1.name AS l, p2.name AS r""".stripMargin
  test("find friends of others") {
    // given
    execute( """CREATE (a:Person{name:"a"}), (b:Person{name:"b"}), (c:Person{name:"c"}), (d:Person{name:"d"})
               |CREATE (a)-[:FRIEND]->(c), (b)-[:FRIEND]->(c), (c)-[:FRIEND]->(d)""".stripMargin)

    // when
    val result = profile(QUERY)

    // then
    result.toSet should equal(Set(
      Map("l" -> "a", "r" -> "b"),
      Map("l" -> "a", "r" -> "d"),
      Map("l" -> "b", "r" -> "a"),
      Map("l" -> "b", "r" -> "d"),
      Map("l" -> "d", "r" -> "a"),
      Map("l" -> "d", "r" -> "b")))
    result should use("TriadicSelection")
  }

  test("find friendly people") {
    // given
    execute( """CREATE (a:Person{name:"a"}), (b:Person{name:"b"}), (c:Person{name:"c"}), (d:Person{name:"d"}), (e:Person{name:"e"})
               |CREATE (a)-[:FRIEND]->(c), (b)-[:FRIEND]->(d), (c)-[:FRIEND]->(e), (d)-[:FRIEND]->(e)""".stripMargin)

    // when
    val result = profile(QUERY)

    // then
    result.toSet should equal(Set(
      Map("l" -> "a", "r" -> "e"),
      Map("l" -> "b", "r" -> "e"),
      Map("l" -> "d", "r" -> "c"),
      Map("l" -> "c", "r" -> "d"),
      Map("l" -> "e", "r" -> "a"),
      Map("l" -> "e", "r" -> "b")))
    result should use("TriadicSelection")
  }

  test("should not find my friends") {
    // given
    execute(
      """CREATE (a:Person{name:"a"}), (b:Person{name:"b"}), (c:Person{name:"c"})
        |CREATE (a)-[:FRIEND]->(b), (b)-[:FRIEND]->(c), (c)-[:FRIEND]->(a)""".stripMargin)

    // when
    val result: InternalExecutionResult = profile(QUERY)

    // then
    result should (use("TriadicSelection") and be(empty))
  }

  def use(operator: String): Matcher[InternalExecutionResult] = new Matcher[InternalExecutionResult] {
    override def apply(result: InternalExecutionResult): MatchResult = {
      val plan: InternalPlanDescription = result.executionPlanDescription()
      MatchResult(
        matches = plan.find(operator).nonEmpty,
        rawFailureMessage = s"Plan should use $operator:\n$plan",
        rawNegatedFailureMessage = s"Plan should not use $operator:\n$plan")
    }
  }
}
