/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.graphdb.Node

class OptionalBehaviourAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {
  test("optional nodes with labels in match clause should return null when there is no match") {
    createNode()
    val result = executeWithNewPlanner("match n optional match n-[r]-(m:Person) return r")
    assert(result.toList === List(Map("r" -> null)))
  }

  test("optional nodes with labels in match clause should not return if where is no match") {
    createNode()
    val result = executeWithNewPlanner("match n optional match (n)-[r]-(m) where m:Person return r")
    assert(result.toList === List(Map("r" -> null)))
  }

  test("should allow match following optional match if there is an intervening with when there are results") {
    val a = createLabeledNode("A")
    val c = createLabeledNode("C")
    relate(a, c)
    val d = createNode()
    relate(c, d)
    val result = executeScalarWithNewPlanner[Node]("MATCH (a:A) OPTIONAL MATCH (a)-->(b:B) OPTIONAL MATCH (a)-->(c:C) WITH coalesce(b, c) as x MATCH (x)-->(d) RETURN d")
    assert(result === d)
  }

  test("should allow match following optional match if there is an intervening with when there are no results") {
    createLabeledNode("A")
    val result = executeWithNewPlanner("MATCH (a:A) OPTIONAL MATCH (a)-->(b:B) OPTIONAL MATCH (a)-->(c:C) WITH coalesce(b, c) as x MATCH (x)-->(d) RETURN d")
    assert(result.toList === List())
  }

  test("should support optional match without any external dependencies in WITH") {
    val nodeA = createLabeledNode("A")
    val nodeB = createLabeledNode("B")
    val result = executeWithNewPlanner("OPTIONAL MATCH (a:A) WITH a AS a MATCH (b:B) RETURN a, b")

    assert(result.toList === List(Map("a" -> nodeA, "b" -> nodeB)))
  }
}
