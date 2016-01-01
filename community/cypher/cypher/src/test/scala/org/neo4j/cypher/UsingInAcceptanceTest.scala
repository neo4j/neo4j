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

class UsingInAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("fail if using index with start clause") {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN & THEN
    intercept[SyntaxException](
      execute("start n=node(*) using index n:Person(name) where n:Person and n.name IN ['kabam'] return n"))
  }

  test("fail if using an identifier with label not used in match") {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN
    intercept[SyntaxException](
      executeWithNewPlanner("match n-->() using index n:Person(name) where n.name IN ['kabam'] return n"))
  }

  test("fail if using an hint for a non existing index") {
    // GIVEN: NO INDEX

    // WHEN
    intercept[IndexHintException](
      executeWithNewPlanner("match (n:Person)-->() using index n:Person(name) where n.name IN ['kabam'] return n"))
  }

  test("fail if using hints with unusable equality predicate") {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN
    intercept[SyntaxException](
      executeWithNewPlanner("match (n:Person)-->() using index n:Person(name) where NOT (n.name IN ['kabam']) return n"))
  }

  test("fail if joining index hints in equality predicates") {
    // GIVEN
    graph.createIndex("Person", "name")
    graph.createIndex("Food", "name")

    // WHEN
    intercept[SyntaxException](
      executeWithNewPlanner("match (n:Person)-->(m:Food) using index n:Person(name) using index m:Food(name) where n.name IN [m.name] return n"))
  }

  test("fail when equality checks are done with OR") {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN
    intercept[SyntaxException](
      executeWithNewPlanner("match n-->() using index n:Person(name) where n.name IN ['kabam'] OR n.name = 'kaboom' return n"))
  }
}
