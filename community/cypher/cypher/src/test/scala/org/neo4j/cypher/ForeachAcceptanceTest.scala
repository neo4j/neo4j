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

class ForeachAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport with QueryStatisticsTestSupport {

  test("should understand symbols introduced by FOREACH") {
    createLabeledNode("Label")
    createLabeledNode("Label")
    createLabeledNode("Label2")
    val query =
      """MATCH (a:Label)
        |WITH collect(a) as nodes
        |MATCH (b:Label2)
        |FOREACH(n in nodes |
        |  CREATE UNIQUE (n)-[:SELF]->(b))""".stripMargin

    // should work
    eengine.execute(query, Map.empty[String, Any], graph.session())
  }

  test("nested foreach") {
    // given
    createLabeledNode("Root")

    // when
    val query = """MATCH (r:Root)
                  |FOREACH (i IN range(1, 10) |
                  | CREATE (r)-[:PARENT]->(c:Child { id:i })
                  | FOREACH (j IN range(1, 10) |
                  |   CREATE (c)-[:PARENT]->(:Child { id: c.id * 10 + j })
                  | )
                  |)""".stripMargin

    val result = updateWithBothPlanners(query)

    // then
    assertStats(result, nodesCreated = 110, relationshipsCreated = 110, propertiesWritten = 110, labelsAdded = 110)
    val rows = executeScalar[Number]("MATCH (:Root)-[:PARENT]->(:Child) RETURN count(*)")
    rows should equal(10)
    val ids = updateWithBothPlanners("MATCH (:Root)-[:PARENT*]->(c:Child) RETURN c.id AS id ORDER BY c.id").toList
    ids should equal((1 to 110).map(i => Map("id" -> i)))
  }

  test("foreach should return no results") {
    // given
    val query = "FOREACH( n in range( 0, 1 ) | CREATE (p:Person) )"

    // when
    val result = updateWithBothPlanners(query)

    // then
    assertStats(result, nodesCreated = 2, labelsAdded = 2)
    result should use("Foreach", "CreateNode")
    result shouldBe empty
  }

  test("foreach should not expose inner variables") {
    val query = """MATCH (n)
                  |FOREACH (i IN [0, 1, 2]
                  |  CREATE (m)
                  |)
                  |SET m.prop = 0
                """.stripMargin

    a [SyntaxException] should be thrownBy updateWithBothPlanners(query)
  }

  test("foreach should let you use inner variables from create relationship patterns") {
    // given
    val query = """FOREACH (x in [1] |
                  |CREATE (e:Event)-[i:IN]->(p:Place)
                  |SET e.foo='e_bar'
                  |SET i.foo='i_bar'
                  |SET p.foo='p_bar')
                  |WITH 0 as dummy
                  |MATCH (e:Event)-[i:IN]->(p:Place)
                  |RETURN e.foo, i.foo, p.foo""".stripMargin

    // when
    val result = updateWithBothPlanners(query)

    // then
    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, labelsAdded = 2, propertiesWritten = 3)
    val resultList = result.toList
    resultList.head.get("e.foo") should equal(Some("e_bar"))
    resultList.head.get("i.foo") should equal(Some("i_bar"))
    resultList.head.get("p.foo") should equal(Some("p_bar"))
  }

  test("Foreach and delete should work together without breaking on unknown identifier types") {
    // given
    val node = createLabeledNode("Label")
    relate(node, createNode())

    val query =
      """MATCH (n: Label)
        |OPTIONAL MATCH (n)-[rel]->()
        |FOREACH (r IN CASE WHEN rel IS NOT NULL THEN [rel] ELSE [] END | DELETE r )""".stripMargin

    // when
    val result = updateWithBothPlanners(query)

    // then
    assertStats(result, relationshipsDeleted = 1)
  }

  test("merge inside foreach in compatibility mode should work nicely") {
    // given

    val query =
      """|FOREACH(v IN [1] |
         |  CREATE (a), (b)
         |  MERGE (a)-[:FOO]->(b))""".stripMargin

    // when
    val result = updateWithBothPlannersAndCompatibilityMode(query)

    // then
    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)
  }

  test("foreach with non-trivially typed collection and create pattern should not create bound node") {
    // given
    val query =
      """CREATE (a),(b)
        |WITH a, collect(b) as nodes, true as condition
        |FOREACH (x IN CASE WHEN condition THEN nodes ELSE [] END | CREATE (a)-[:X]->(x) );""".stripMargin

    // when
    val result = updateWithBothPlannersAndCompatibilityMode(query)

    // then
    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)
  }

  test("foreach with non-trivially typed collection and merge pattern should not create bound node") {
    // given
    createLabeledNode("Foo")
    createLabeledNode("Bar")

    val query =
      """MATCH (n:Foo),(m:Bar)
        |FOREACH (x IN CASE WHEN true THEN [n] ELSE [] END |
        |   MERGE (x)-[:FOOBAR]->(m) );""".stripMargin

    // when
    val result = updateWithBothPlannersAndCompatibilityMode(query)

    // then
    assertStats(result, relationshipsCreated = 1)
  }

  test("foreach with mixed type collection should not plan create of bound node and fail at runtime") {
    // given
    createLabeledNode("Foo")
    createLabeledNode("Bar")

    val query =
      """MATCH (n:Foo),(m:Bar)
        |WITH n, [m, 42] as mixedTypeCollection
        |FOREACH (x IN mixedTypeCollection | CREATE (n)-[:FOOBAR]->(x) );""".stripMargin

    // when
    val explain = executeWithCostPlannerOnly(s"EXPLAIN $query")

    // then
    explain.executionPlanDescription().toString shouldNot include("CreateNode")

    // when
    try {
      val result = executeWithCostPlannerOnly(query)
    }
    catch {
      case e: Exception => e.getMessage should startWith("Expected to find a node at x but")
    }
  }
}
