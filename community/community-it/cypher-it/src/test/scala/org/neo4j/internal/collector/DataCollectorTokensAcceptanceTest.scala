/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.collector

import org.neo4j.cypher._
import org.neo4j.internal.collector.DataCollectorMatchers.beMapContaining

class DataCollectorTokensAcceptanceTest extends ExecutionEngineFunSuite {

  test("should retrieve TOKENS") {
    // given
    execute(
      """CREATE (u:User {name:'Herbert', age:42, weight: 67.1})
         CREATE (m:Manager {firstName:'Joe', lastName:'Lee'})
         CREATE (o:Office)
         CREATE (u)-[:KNOWS]->(m)
         CREATE (m)-[:BOSSES {since:"forever"}]->(u)
         CREATE (m)-[:AT]->(o)""".stripMargin)

    // when
    val res = execute("CALL db.stats.retrieve('TOKENS')").single

    // then
    res("section") should be("TOKENS")
    list(res("data"), "labels") should contain only ("User", "Office", "Manager")
    list(res("data"), "relationshipTypes") should contain only ("KNOWS", "BOSSES", "AT")
    list(res("data"), "propertyKeys") should contain only ("name", "age", "weight", "firstName", "lastName", "since")
  }

  test("should put token counts into META section on retrieveAllAnonymized") {
    // given
    execute(
      """CREATE (u:User {name:'Herbert', age:42, weight: 67.1})
         CREATE (m:Manager {firstName:'Joe', lastName:'Lee'})
         CREATE (o:Office)
         CREATE (m)-[:BOSSES {since:"forever"}]->(u)
         CREATE (m)-[:AT]->(o)""".stripMargin)

    // when
    val res = execute("CALL db.stats.retrieveAllAnonymized('myGraphToken')")

    // then
    res.toList.head should beMapContaining(
        "section" -> "META",
        "data" -> beMapContaining(
          "graphToken" -> "myGraphToken",
          "labelCount" -> 3,
          "relationshipTypeCount" -> 2,
          "propertyKeyCount" -> 6
        )
      )

  }

  private def list(map: AnyRef, key: String) =
    map.asInstanceOf[Map[String, AnyRef]](key).asInstanceOf[IndexedSeq[AnyRef]]
}
