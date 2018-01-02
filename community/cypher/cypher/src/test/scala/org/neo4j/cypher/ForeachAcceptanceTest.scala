/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

class ForeachAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport {

  test("nested foreach") {
    // given
    createLabeledNode("root")

    // when
    val query = """MATCH (r:root)
      |FOREACH (i IN range(1,10)|
      |CREATE (r)-[:PARENT]->(c:child { id:i })
      |FOREACH (j IN range(1,10)|
      |CREATE (c)-[:PARENT]->(:child { id:c.id*10+j })))""".stripMargin
    execute(query)


    // then
    val rows = executeScalar[Number]("MATCH (:root)-[:PARENT]->(:child) RETURN count(*)")
    rows should equal(10)
    val ids = execute("MATCH (:root)-[:PARENT*]->(c:child) RETURN c.id AS id ORDER BY c.id").toList
    ids should equal((1 to 110).map(i => Map("id" -> i)))
  }

  test("foreach should return no results") {
    // given
    val query = "FOREACH( n in range( 0, 1 ) | CREATE (p:Person) )"

    // when
    val result = execute(query).toList

    // then
    result shouldBe empty
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
    val result = execute(query).toList

    // then
    result.head.get("e.foo") should equal(Some("e_bar"))
    result.head.get("i.foo") should equal(Some("i_bar"))
    result.head.get("p.foo") should equal(Some("p_bar"))
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
    val result = execute(query)

    // then
    assertStats(result, relationshipsDeleted = 1)
  }
}
