/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
