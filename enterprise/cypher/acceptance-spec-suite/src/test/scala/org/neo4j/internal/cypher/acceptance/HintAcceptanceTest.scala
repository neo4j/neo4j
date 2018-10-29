/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import java.time.LocalDate

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.ComparePlansWithAssertion
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.values.storable.CoordinateReferenceSystem
import org.neo4j.values.storable.Values

import scala.collection.Map

class HintAcceptanceTest
    extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should use a simple hint") {
    val query = "MATCH (a)--(b)--(c) USING JOIN ON b RETURN a,b,c"
    executeWith(Configs.All, query, planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("NodeHashJoin"), expectPlansToFail = Configs.RulePlanner))
  }

  test("should not plan multiple joins for one hint - left outer join") {
    val a = createLabeledNode(Map[String, Any]("name" -> "a"), "A")
    for(i <- 0 until 10) {
      val b = createLabeledNode(Map[String, Any]("name" -> s"${i}b"), "B")
      relate(a, b)
    }

    val query = """MATCH (a:A)
                  |OPTIONAL MATCH (a)-->(b:B)
                  |USING JOIN ON a
                  |RETURN a.name, b.name""".stripMargin

    executeWith(Configs.InterpretedAndSlotted - Configs.Cost2_3 - Configs.Cost3_1, query,
      planComparisonStrategy = ComparePlansWithAssertion((p) => {
      p should includeSomewhere.aPlan("NodeLeftOuterHashJoin")
      p should not(includeSomewhere.aPlan("NodeHashJoin"))
    }, expectPlansToFail = Configs.Version2_3 + Configs.Version3_1))
  }

  test("should not plan multiple joins for one hint - right outer join") {
    val b = createLabeledNode(Map[String, Any]("name" -> "b"), "B")
    for(i <- 0 until 10) {
      val a = createLabeledNode(Map[String, Any]("name" -> s"${i}a"), "A")
      if(i == 0) relate(a, b)
    }

    val query = """MATCH (a:A)
                  |OPTIONAL MATCH (a)-->(b:B)
                  |USING JOIN ON a
                  |RETURN a.name, b.name""".stripMargin

    executeWith(Configs.InterpretedAndSlotted - Configs.Cost2_3 - Configs.Cost3_1, query, planComparisonStrategy = ComparePlansWithAssertion((p) => {
      p should includeSomewhere.aPlan("NodeRightOuterHashJoin")
      p should not(includeSomewhere.aPlan("NodeHashJoin"))
    }, expectPlansToFail = Configs.Version2_3 + Configs.Version3_1))
  }

  test("should solve join hint on 1 variable with join on more, if possible") {
    val query =
      """MATCH (pA:Person),(pB:Person) WITH pA, pB
        |
        |OPTIONAL MATCH
        |  (pA)<-[:HAS_CREATOR]-(pB)
        |USING JOIN ON pB
        |RETURN *""".stripMargin

    executeWith(Configs.InterpretedAndSlotted - Configs.Cost2_3 - Configs.Cost3_1, query,
      planComparisonStrategy = ComparePlansWithAssertion((p) => {
        p should includeSomewhere.aPlan("NodeRightOuterHashJoin")
      }, expectPlansToFail = Configs.Version2_3 + Configs.Version3_1))
  }

  test("should do index seek instead of index scan with explicit index seek hint") {
    graph.createIndex("A", "prop")
    graph.createIndex("B", "prop")

    graph.inTx {
      createLabeledNode(Map("prop" -> 42), "A")
      createLabeledNode(Map("prop" -> 1337), "B")
    }

    // At the time of writing this test fails with generic index hints:
    // USING INDEX a:A(prop)
    // USING INDEX b:B(prop)
    val query = """EXPLAIN
                  |LOAD CSV WITH HEADERS FROM 'file:///dummy.csv' AS row
                  |MATCH (a:A), (b:B)
                  |USING INDEX SEEK a:A(prop)
                  |USING INDEX SEEK b:B(prop)
                  |WHERE a.prop = row.propA AND b.prop = row.propB
                  |RETURN a.prop, b.prop
                """.stripMargin

    executeWith(Configs.InterpretedAndSlotted - Configs.RulePlanner - Configs.Cost2_3 - Configs.Cost3_1, query,
      planComparisonStrategy = ComparePlansWithAssertion((p) => {
        p should includeSomewhere.nTimes(2, aPlan("NodeIndexSeek"))
      }, expectPlansToFail = Configs.Version2_3 + Configs.Version3_1))
  }

  test("should accept hint on spatial index with distance function") {
    // Given
    graph.createIndex("Business", "location")
    graph.createIndex("Review", "date")

    val business = createLabeledNode(Map("location" -> Values.pointValue(CoordinateReferenceSystem.WGS84, -111.977, 33.3288)), "Business")
    val review = createLabeledNode(Map("date" -> LocalDate.parse("2017-03-01")), "Review")
    relate(review, business, "REVIEWS")

    // When
    val query =
      """MATCH (b:Business)<-[:REVIEWS]-(r:Review)
        |USING INDEX b:Business(location)
        |WHERE distance(b.location, point({latitude: 33.3288, longitude: -111.977})) < 6500
        |AND date("2017-01-01") <= r.date <= date("2018-01-01")
        |RETURN COUNT(*)""".stripMargin

    val result = executeWith(Configs.Version3_5 + Configs.Version3_4 - Configs.Compiled, query)

    // Then
    result.toList should be(List(Map("COUNT(*)" -> 1)))

  }
}
