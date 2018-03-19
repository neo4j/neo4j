/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.cypher.ExecutionEngineFunSuite

import scala.collection.Map

class HintAcceptanceTest
  extends ExecutionEngineFunSuite {

  test("should use a simple hint") {
    val query = "MATCH (a)--(b)--(c) USING JOIN ON b RETURN a,b,c"
    val result = execute(query)
    result should use("NodeHashJoin")
  }

  test("should not plan multiple joins for one hint") {
    val a = createLabeledNode(Map[String, Any]("name" -> "a"), "A")
    for(i <- 0 until 10) {
      val b = createLabeledNode(Map[String, Any]("name" -> s"${i}b"), "B")
      relate(a, b)
    }

    val query = """MATCH (a:A)
                  |OPTIONAL MATCH (a)-->(b:B)
                  |USING JOIN ON a
                  |RETURN a.name, b.name""".stripMargin

    val result = execute(query)
    result should use("NodeOuterHashJoin")
    result shouldNot use("NodeHashJoin")
  }

  test("solve join hint after ") {
    val query =
      """MATCH path=allShortestPaths((p1:Person {id: 17592186045594})-[:KNOWS*]-(p2:Person {id: 2199023256530}))
        |UNWIND relationships(path) AS k
        |WITH path, startNode(k) AS pA, endNode(k) AS pB
        |
        |OPTIONAL MATCH
        |  (pA)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(post:Post)-[:HAS_CREATOR]->(pB),
        |  (post)<-[:CONTAINER_OF]-(forum:Forum)
        |USING JOIN ON pB
        |WHERE forum.creationDate >= 20110201000000000 AND forum.creationDate <= 20110301000000000
        |RETURN *""".stripMargin
    val result = execute(query)
    result should use("NodeHashJoin")
  }
}
