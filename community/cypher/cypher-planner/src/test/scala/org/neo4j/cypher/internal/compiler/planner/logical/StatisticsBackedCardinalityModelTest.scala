/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.CardinalityModelTestHelper
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.AssumeIndependenceQueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.AssumeIndependenceQueryGraphCardinalityModel.MIN_INBOUND_CARDINALITY
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class StatisticsBackedCardinalityModelTest extends CypherFunSuite with LogicalPlanningTestSupport with CardinalityModelTestHelper {

  val allNodes = 733.0
  val personCount = 324.0
  val relCount = 50.0
  val rel2Count = 78.0

  test("query containing a WITH and LIMIT on low/fractional cardinality") {
    val i = .1
    givenPattern("MATCH (a:Person) WITH a LIMIT 10 MATCH (a)-[:REL]->()").
      withGraphNodes(allNodes).
      withLabel('Person -> i).
      withRelationshipCardinality('Person -> 'REL -> 'Person -> relCount).
      shouldHavePlannerQueryCardinality(createCardinalityModel)(
        Math.max(MIN_INBOUND_CARDINALITY.amount, Math.min(i, 10.0)) * relCount / i
      )
  }

  test("query containing a WITH and LIMIT on high cardinality") {
    val i = personCount
    givenPattern("MATCH (a:Person) WITH a LIMIT 10 MATCH (a)-[:REL]->()").
      withGraphNodes(allNodes).
      withLabel('Person -> i).
      withRelationshipCardinality('Person -> 'REL -> 'Person -> relCount).
      shouldHavePlannerQueryCardinality(createCardinalityModel)(
        Math.min(i, 10.0) * relCount / i
      )
  }

  test("query containing a WITH and LIMIT on parameterized cardinality") {
    val i = personCount
    givenPattern("MATCH (a:Person) WITH a LIMIT $limit MATCH (a)-[:REL]->()").
      withGraphNodes(allNodes).
      withLabel('Person -> i).
      withRelationshipCardinality('Person -> 'REL -> 'Person -> relCount).
      shouldHavePlannerQueryCardinality(createCardinalityModel)(
        Math.min(i, DEFAULT_LIMIT_CARDINALITY) * relCount / i
      )
  }

  test("query containing a WITH and aggregation vol. 2") {
    val patternNodeCrossProduct = allNodes * allNodes
    val labelSelectivity = personCount / allNodes
    val maxRelCount = patternNodeCrossProduct * labelSelectivity
    val relSelectivity = rel2Count / maxRelCount

    val firstQG = patternNodeCrossProduct * labelSelectivity * relSelectivity

    val aggregation = Math.sqrt(firstQG)

    givenPattern("MATCH (a:Person)-[:REL2]->(b) WITH a, count(*) as c MATCH (a)-[:REL]->()").
      withGraphNodes(allNodes).
      withLabel('Person -> personCount).
      withRelationshipCardinality('Person -> 'REL -> 'Person -> relCount).
      withRelationshipCardinality('Person -> 'REL2 -> 'Person -> rel2Count).
      shouldHavePlannerQueryCardinality(createCardinalityModel)(aggregation * relCount / personCount)
  }

  test("query containing both SKIP and LIMIT") {
    val i = personCount
    givenPattern( "MATCH (n:Person) WITH n SKIP 5 LIMIT 10").
      withGraphNodes(allNodes).
      withLabel('Person -> i).
      shouldHavePlannerQueryCardinality(createCardinalityModel)(
        Math.min(i, 10.0)
      )
  }

  // We do not improve in this case
  ignore("query containing both SKIP and LIMIT with large skip, so skip + limit exceeds total row count boundary") {
    val i = personCount
    givenPattern( s"MATCH (n:Person) WITH n SKIP ${personCount - 5} LIMIT 10").
      withGraphNodes(allNodes).
      withLabel('Person -> i).
      shouldHavePlannerQueryCardinality(createCardinalityModel)(
        Math.min(i, 5.0)
      )
  }

  test("should reduce cardinality for a WHERE after a WITH") {
    val i = personCount
    givenPattern("MATCH (a:Person) WITH a LIMIT 10 WHERE a.age = 20").
      withGraphNodes(allNodes).
      withLabel('Person -> i).
      shouldHavePlannerQueryCardinality(createCardinalityModel)(
        Math.min(i, 10.0) * DEFAULT_EQUALITY_SELECTIVITY
      )
  }

  test("should reduce cardinality for a WHERE after a WITH, unknown LIMIT") {
    val i = personCount
    givenPattern("MATCH (a:Person) WITH a LIMIT $limit WHERE a.age = 20").
      withGraphNodes(allNodes).
      withLabel('Person -> i).
      shouldHavePlannerQueryCardinality(createCardinalityModel)(
        Math.min(i, DEFAULT_LIMIT_CARDINALITY) * DEFAULT_EQUALITY_SELECTIVITY
      )
  }

  test("should reduce cardinality for a WHERE after a WITH, with ORDER BY") {
    val i = personCount
    givenPattern("MATCH (a:Person) WITH a ORDER BY a.name WHERE a.age = 20").
      withGraphNodes(allNodes).
      withLabel('Person -> i).
      shouldHavePlannerQueryCardinality(createCardinalityModel)(
        i * DEFAULT_EQUALITY_SELECTIVITY
      )
  }

  test("should reduce cardinality for a WHERE after a WITH, with DISTINCT") {
    val i = personCount
    givenPattern("MATCH (a:Person) WITH DISTINCT a WHERE a.age = 20").
      withGraphNodes(allNodes).
      withLabel('Person -> i).
      shouldHavePlannerQueryCardinality(createCardinalityModel)(
        i * DEFAULT_DISTINCT_SELECTIVITY * DEFAULT_EQUALITY_SELECTIVITY
      )
  }

  test("should reduce cardinality for a WHERE after a WITH, with AGGREGATION without grouping") {
    val i = personCount
    givenPattern("MATCH (a:Person) WITH count(a) AS count WHERE count > 20").
      withGraphNodes(allNodes).
      withLabel('Person -> i).
      shouldHavePlannerQueryCardinality(createCardinalityModel)(
        Math.max(DEFAULT_RANGE_SELECTIVITY, MIN_INBOUND_CARDINALITY.amount)
      )
  }

  test("should reduce cardinality for a WHERE after a WITH, with AGGREGATION with grouping") {
    val i = personCount
    givenPattern("MATCH (a:Person) WITH count(a) AS count, a.name AS name WHERE count > 20").
      withGraphNodes(allNodes).
      withLabel('Person -> i).
      shouldHavePlannerQueryCardinality(createCardinalityModel)(
        Math.sqrt(i) * DEFAULT_RANGE_SELECTIVITY
      )
  }

  test("standalone procedure call should have default cardinality") {
    givenPattern("CALL my.proc.foo(42) YIELD x")
      .withGraphNodes(allNodes)
      .shouldHavePlannerQueryCardinality(createCardinalityModel)(DEFAULT_MULTIPLIER)
  }

  test("procedure call with no input should not have 0 cardinality") {
    givenPattern("MATCH (:Foo) CALL my.proc.foo(42) YIELD x")
      .withGraphNodes(allNodes)
      .withLabel('Foo -> 0)
      .shouldHavePlannerQueryCardinality(createCardinalityModel)(1)
  }

  test("procedure call with large input should multiply cardinality") {
    val inputSize = 1000000
    givenPattern("MATCH (:Foo) CALL my.proc.foo(42) YIELD x")
      .withGraphNodes(inputSize)
      .withLabel('Foo -> inputSize)
      .shouldHavePlannerQueryCardinality(createCardinalityModel)(DEFAULT_MULTIPLIER * inputSize)
  }

  test("standalone LOAD CSV should have default cardinality") {
    givenPattern("LOAD CSV FROM 'foo' AS csv")
      .withGraphNodes(allNodes)
      .shouldHavePlannerQueryCardinality(createCardinalityModel)(DEFAULT_MULTIPLIER)
  }

  test("LOAD CSV with no input should not have 0 cardinality") {
    givenPattern("MATCH (:Foo) LOAD CSV FROM 'foo' AS csv")
      .withGraphNodes(allNodes)
      .withLabel('Foo -> 0)
      .shouldHavePlannerQueryCardinality(createCardinalityModel)(1)
  }

  test("LOAD CSV with large input should multiply cardinality") {
    val inputSize = 1000000
    givenPattern("MATCH (:Foo) LOAD CSV FROM 'foo' AS csv")
      .withGraphNodes(inputSize)
      .withLabel('Foo -> inputSize)
      .shouldHavePlannerQueryCardinality(createCardinalityModel)(DEFAULT_MULTIPLIER * inputSize)
  }

  test("UNWIND with no information should have default cardinality") {
    givenPattern("UNWIND $foo AS i")
      .withGraphNodes(allNodes)
      .shouldHavePlannerQueryCardinality(createCardinalityModel)(DEFAULT_MULTIPLIER)
  }

  test("UNWIND with empty list literal should have min inbound cardinality") {
    givenPattern("UNWIND [] AS i")
      .withGraphNodes(allNodes)
      .shouldHavePlannerQueryCardinality(createCardinalityModel)(MIN_INBOUND_CARDINALITY.amount)
  }

  test("UNWIND with non-empty list literal should have list size cardinality") {
    givenPattern("UNWIND [1, 2, 3, 4, 5] AS i")
      .withGraphNodes(allNodes)
      .shouldHavePlannerQueryCardinality(createCardinalityModel)(5)
  }

  test("UNWIND with single element range") {
    givenPattern("UNWIND range(0, 0) AS i")
      .withGraphNodes(allNodes)
      .shouldHavePlannerQueryCardinality(createCardinalityModel)(1)
  }

  test("UNWIND with empty range 1") {
    givenPattern("UNWIND range(0, -1) AS i")
      .withGraphNodes(allNodes)
      .shouldHavePlannerQueryCardinality(createCardinalityModel)(MIN_INBOUND_CARDINALITY.amount)
  }

  test("UNWIND with empty range 2") {
    givenPattern("UNWIND range(10, 0, 1) AS i")
      .withGraphNodes(allNodes)
      .shouldHavePlannerQueryCardinality(createCardinalityModel)(MIN_INBOUND_CARDINALITY.amount)
  }

  test("UNWIND with empty range 3") {
    givenPattern("UNWIND range(0, 10, -1) AS i")
      .withGraphNodes(allNodes)
      .shouldHavePlannerQueryCardinality(createCardinalityModel)(MIN_INBOUND_CARDINALITY.amount)
  }

  test("UNWIND with non-empty range") {
    givenPattern("UNWIND range(1, 10) AS i")
      .withGraphNodes(allNodes)
      .shouldHavePlannerQueryCardinality(createCardinalityModel)(10)
  }

  test("UNWIND with non-empty DESC range") {
    givenPattern("UNWIND range(10, 1, -1) AS i")
      .withGraphNodes(allNodes)
      .shouldHavePlannerQueryCardinality(createCardinalityModel)(10)
  }

  test("UNWIND with non-empty range with aligned step") {
    givenPattern("UNWIND range(1, 9, 2) AS i")
      .withGraphNodes(allNodes)
      .shouldHavePlannerQueryCardinality(createCardinalityModel)(5)
  }

  test("UNWIND with non-empty DESC range with aligned step") {
    givenPattern("UNWIND range(9, 1, -2) AS i")
      .withGraphNodes(allNodes)
      .shouldHavePlannerQueryCardinality(createCardinalityModel)(5)
  }

  test("UNWIND with non-empty range with unaligned step") {
    givenPattern("UNWIND range(1, 9, 3) AS i")
      .withGraphNodes(allNodes)
      .shouldHavePlannerQueryCardinality(createCardinalityModel)(3)
  }

  test("UNWIND with non-empty DESC range with unaligned step") {
    givenPattern("UNWIND range(9, 1, -3) AS i")
      .withGraphNodes(allNodes)
      .shouldHavePlannerQueryCardinality(createCardinalityModel)(3)
  }

  def createCardinalityModel(in: QueryGraphCardinalityModel): Metrics.CardinalityModel =
    new StatisticsBackedCardinalityModel(in, newExpressionEvaluator)

  override def createQueryGraphCardinalityModel(stats: GraphStatistics): QueryGraphCardinalityModel =
    AssumeIndependenceQueryGraphCardinalityModel(stats, combiner)
}
