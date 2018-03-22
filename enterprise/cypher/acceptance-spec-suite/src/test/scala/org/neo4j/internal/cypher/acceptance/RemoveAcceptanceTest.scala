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

import org.neo4j.cypher._
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.Configs

class RemoveAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  test("remove with case expression should work gh #10831") {
    // given
    graph.execute("CREATE (:Person {name: 'Alice', age: 23})-[:KNOWS]->(:Person {name:'Bob', age: 24})")

    // when
    val query =
      """MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})
        |REMOVE CASE WHEN a.age>b.age THEN a ELSE b END.age
        |RETURN a.age, b.age""".stripMargin

    val result = executeWith(Configs.UpdateConf, query)

    // then
    result.toList should equal(List(Map("a.age" -> 23, "b.age" -> null)))
  }

  test("remove property from null literal") {
    executeWith(Configs.Interpreted - Configs.Cost2_3, "REMOVE null.p") should have size 0
  }

}
