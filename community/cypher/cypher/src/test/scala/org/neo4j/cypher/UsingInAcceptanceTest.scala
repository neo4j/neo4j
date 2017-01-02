/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
      executeWithAllPlanners("start n=node(*) using index n:Person(name) where n:Person and n.name IN ['kabam'] return n"))
  }

  test("fail if using an identifier with label not used in match") {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN
    intercept[SyntaxException](
      executeWithAllPlanners("match n-->() using index n:Person(name) where n.name IN ['kabam'] return n"))
  }

  test("fail if using an hint for a non existing index") {
    // GIVEN: NO INDEX

    // WHEN
    intercept[IndexHintException](
      executeWithAllPlanners("match (n:Person)-->() using index n:Person(name) where n.name IN ['kabam'] return n"))
  }

  test("fail if using hints with unusable equality predicate") {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN
    intercept[SyntaxException](
      executeWithAllPlanners("match (n:Person)-->() using index n:Person(name) where NOT (n.name IN ['kabam']) return n"))
  }

  test("fail if joining index hints in equality predicates") {
    // GIVEN
    graph.createIndex("Person", "name")
    graph.createIndex("Food", "name")

    // WHEN
    intercept[SyntaxException](
      executeWithAllPlanners("match (n:Person)-->(m:Food) using index n:Person(name) using index m:Food(name) where n.name IN [m.name] return n"))
  }

  test("fail when equality checks are done with OR") {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN
    intercept[SyntaxException](
      executeWithAllPlanners("match n-->() using index n:Person(name) where n.name IN ['kabam'] OR n.name = 'kaboom' return n"))
  }

  test("when failing to support all hints we should provide an understandable error message") {
    // GIVEN
    graph.createIndex("LocTag", "id")

    // WHEN
    val query = """MATCH (t1:LocTag {id:1642})-[:Child*0..]->(:LocTag)
                  |     <-[:Tagged]-(s1:Startup)<-[r1:Role]-(u:User)
                  |     -[r2:Role]->(s2:Startup)-[:Tagged]->(:LocTag)
                  |     <-[:Child*0..]-(t2:LocTag {id:1642})
                  |USING INDEX t1:LocTag(id)
                  |USING INDEX t2:LocTag(id)
                  |RETURN count(u)""".stripMargin


    val error = intercept[HintException](executeWithAllPlanners(query))

    error.getMessage should equal("The current planner cannot satisfy all hints in the query, please try removing hints or try with another planner")
  }
}
