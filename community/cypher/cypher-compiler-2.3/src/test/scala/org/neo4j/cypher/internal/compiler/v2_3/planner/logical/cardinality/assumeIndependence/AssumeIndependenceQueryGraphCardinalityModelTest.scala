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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality.assumeIndependence

import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality.{ABCDCardinalityData, RandomizedCardinalityModelTestSuite}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_3.spi.GraphStatistics

class AssumeIndependenceQueryGraphCardinalityModelTest extends RandomizedCardinalityModelTestSuite with ABCDCardinalityData {

  import ABCD._
  import org.scalatest.prop.TableDrivenPropertyChecks._

  test("all queries") {
    val queries = Table.apply[String, Double](
      ("query", "expected cardinality"),
      "MATCH (n)"
        -> N,

      "MATCH (n:A)"
        -> A,

      "MATCH (n:A) MATCH (m:B)"
        -> A * B,

      "MATCH a, b" ->
        N * N,

      ""
        -> 1.0,

      "MATCH a, (b:B)"
        -> N * B,

      "MATCH (a:A:B)"
        -> N * Asel * Bsel,

      "MATCH (a:B:A)"
        -> N * Asel * Bsel,

      "MATCH (a:Z)"
        -> 0.0,

      "MATCH (a:A:Z)"
        -> 0.0,

      "MATCH (a:Z:B)"
        -> 0.0,

      "MATCH (a:A) WHERE a.prop = 42"
        -> A * Aprop,

      "MATCH (a:A) WHERE a.prop STARTS WITH 'p'"
        -> {
        val equalitySelectivity = orTimes(DEFAULT_NUMBER_OF_INDEX_LOOKUPS, Aprop)
        A * (equalitySelectivity + ((1d - equalitySelectivity) * DEFAULT_RANGE_SEEK_FACTOR))
      },

      "MATCH (a:B) WHERE a.bar = 42"
        -> B * DEFAULT_EQUALITY_SELECTIVITY,

      "MATCH (a:A) WHERE NOT a.prop = 42"
        -> A * (1 - Aprop),

      "MATCH (a:A) WHERE a.prop = 42 OR a.prop = 43"
        -> A * or(Aprop, Aprop),

      "MATCH (a:A) WHERE a.prop = 42 OR a.bar = 43"
        -> A * or(Aprop, Abar),

      "MATCH (a:B) WHERE a.prop = 42 OR a.bar = 43"
        -> B * or(Bprop, DEFAULT_EQUALITY_SELECTIVITY),

      "MATCH (a) WHERE false"
        -> 0,

      "MATCH (a:A) WHERE a.prop = 42 AND a.bar = 43"
        -> A * Aprop * Abar,

      "MATCH (a:B) WHERE a.prop = 42 AND a.bar = 43"
        -> B * Bprop * DEFAULT_EQUALITY_SELECTIVITY,

      "MATCH (a)-->(b)"
        -> R,

      "MATCH (a:A)-[r:T1]->(b:B)"
        -> A_T1_B,

      "MATCH (b:B)<-[r:T1]-(a:A)"
        -> A_T1_B,

      "MATCH (a:A)-[r:T1]-(b:B)"
        -> A * B * or(A_T1_B_sel, B_T1_A_sel),

      "MATCH (a:A)-[r:T1]->(b)"
        -> A_T1_ANY,

      "MATCH (a:A)-[r:T1]-(b)"
        -> {
        A_T1_ANY + ANY_T1_A
      },

      "MATCH (a:A)-[*0..0]-(b:A)"
        -> N * Asel * Asel // This is a simplification, because *0..0 relationships mean something weird and icky
                           // It's not really right, but should not be fixed by the cardinality model. It should have
                           // been rewritten away before this stage
      ,

      "MATCH (a:A:B)-->()"
        -> {
        val maxRelCount = N * N * Asel * Bsel
        val A_relSelectivity = math.min(A_ANY_ANY / maxRelCount, 1.0)
        val B_relSelectivity = math.min(B_ANY_ANY / maxRelCount, 1.0)
        val relSelectivity = A_relSelectivity * B_relSelectivity
        A * B * relSelectivity
      },

      "MATCH (a:A)-[:T1|:T2]->(:B)"
        -> {
        val patternNodeCrossProduct = N * N
        val labelSelectivity = Asel * Bsel
        val maxRelCount = patternNodeCrossProduct * labelSelectivity
        val relSelectivity = or(A_T1_B / maxRelCount, A_T2_B / maxRelCount)
        patternNodeCrossProduct * labelSelectivity * relSelectivity
      },

      "MATCH (a:A:E)-[:T1|:T2]->()"
        -> {
        val patternNodeCrossProduct = N * N
        val labelSelectivity = Asel * Esel
        val maxRelCount = patternNodeCrossProduct * labelSelectivity
        val relSelectivityT1 = and(A_T1_ANY / maxRelCount, E_T1_ANY / maxRelCount)
        val relSelectivityT2 = and(A_T2_ANY / maxRelCount, E_T2_ANY / maxRelCount)
        val relSelectivity = or(relSelectivityT1, relSelectivityT2)
        patternNodeCrossProduct * labelSelectivity * relSelectivity
      },

      "MATCH (a:A:D)-[:T1|:T2]->()"
        -> {
        val patternNodeCrossProduct = N * N
        val labelSelectivity = Asel * Dsel
        val maxRelCount = patternNodeCrossProduct * labelSelectivity
        val relSelectivityT1 = and(A_T1_ANY / maxRelCount) * (D_T1_ANY / maxRelCount)
        val relSelectivityT2 = and(A_T2_ANY / maxRelCount) * (D_T2_ANY / maxRelCount)
        val relSelectivity = or(relSelectivityT1, relSelectivityT2)
        patternNodeCrossProduct * labelSelectivity * relSelectivity
      },

      "MATCH (a:A:B)-[:T1|:T2]->(c:C:D)"
        -> {
        val A_T2_C = 0
        val A_T2_D = 0
        val B_T2_C = 0
        val B_T2_D = 0
        val patternNodeCrossProduct = N * N
        val labelSelectivity = Asel * Bsel * Csel * Dsel
        val maxRelCount = patternNodeCrossProduct * labelSelectivity
        val relSelT1 = and(A_T1_C / maxRelCount, A_T1_D / maxRelCount, B_T1_C / maxRelCount, B_T1_D / maxRelCount)
        val relSelT2 = and(A_T2_C / maxRelCount, A_T2_D / maxRelCount, B_T2_C / maxRelCount, B_T2_D / maxRelCount)
        val relSelectivity = or(relSelT1, relSelT2)
        patternNodeCrossProduct * labelSelectivity * relSelectivity
      },

      "MATCH (a) OPTIONAL MATCH (a)-[:T1]->(:B)"
        -> (A_T1_B + B_T1_B),

      "MATCH (a:A) OPTIONAL MATCH (a)-[:T1]->(:B)"
        -> A_T1_B,

      "MATCH (a:A) OPTIONAL MATCH (a)-[:MISSING]->()"
        -> A,

      "MATCH (a) OPTIONAL MATCH (b)"
        -> N * N,

      "MATCH (a:A) OPTIONAL MATCH (b:B) OPTIONAL MATCH (c:C)"
        -> A * B * C,

      "MATCH (a:A) WHERE id(a) IN [1,2,3]"
        -> A * (3.0 / N),

      "MATCH (a:A)-[r1:T1]->(b:B)-[r2:T2]->(c:C)"
        -> N * N * N * Asel * A_T1_B_sel * Bsel * B_T2_C_sel * Csel,

      "MATCH (a:A)-[r1:T1]->(b:B)-[r2:T1]->(c:C)"
        -> N * N * N * Asel * A_T1_B_sel * Bsel * B_T1_C_sel * Csel *
        DEFAULT_REL_UNIQUENESS_SELECTIVITY,

      "MATCH (a:A)-[r1:T1]->(b:B)-[r2:T1]->(a)"
        -> N * N * Asel * A_T1_B_sel * Bsel * B_T1_A_sel,

      "MATCH (:A)-[r1:T1]->(:A)-[r2:T1]->(:B)-[r3:T1]->(:B)"
        -> A * A * B * B * A_T1_A_sel * A_T1_B_sel * B_T1_B_sel *
        Math.pow(DEFAULT_REL_UNIQUENESS_SELECTIVITY, 3), // Once per rel-uniqueness predicate

      "MATCH (:A)-[r1:T1]->(:A)-[r2:T1]->(:B)-[r3:T1]->(:B)-[r4:T2]->(c:C)"
        -> A * A * B * B * C * A_T1_A_sel * A_T1_B_sel * B_T1_B_sel * B_T2_C_sel *
        Math.pow(DEFAULT_REL_UNIQUENESS_SELECTIVITY, 4)
    )

    forAll(queries) { (q: String, expected: Double) =>
      forQuery(q).
        shouldHaveQueryGraphCardinality(expected)
    }
  }

  test("empty graph") {
    givenPattern("MATCH a WHERE a.prop = 10").
      withGraphNodes(0).
      withKnownProperty('prop).
      shouldHaveQueryGraphCardinality(0)
  }

  test("honours bound arguments") {
    givenPattern("MATCH (a:FOO)-[:TYPE]->(b:BAR)").
    withQueryGraphArgumentIds(IdName("a")).
    withInboundCardinality(13.0).
    withGraphNodes(500).
    withLabel('FOO -> 100).
    withLabel('BAR -> 400).
    withRelationshipCardinality('FOO -> 'TYPE -> 'BAR -> 1000).
    shouldHaveQueryGraphCardinality(1000.0 / 500.0 * 13.0)
  }

  test("input cardinality of zero on a different identifier should not affect cardinality estimation of the pattern") {
    givenPattern("MATCH (a)").
    withQueryGraphArgumentIds(IdName("e")).
    withInboundCardinality(0.0).
    withGraphNodes(500).
    shouldHaveQueryGraphCardinality(500)
  }


  // TODO: Add a test for a relpatterns where the number of matching nodes is zero


  test("varlength two steps out") {

// The result includes all (:A)-[:T1]->(:B)
// and all (:A)-[:T1]->()-[:T1]->(:B)

    val maxRelCount = A * N * B
    val l1Selectivity = A_T1_B / maxRelCount
    val l2Selectivities = Seq(A_T1_ANY / maxRelCount, ANY_T1_B / maxRelCount)
    val l2Selectivity = l2Selectivities.reduce(_ * _)
    val totalSelectivity = or(l1Selectivity, l2Selectivity)

    forQuery("MATCH (a:A)-[r:T1*1..2]->(b:B)").
    shouldHaveQueryGraphCardinality(maxRelCount * totalSelectivity)
  }

//  test("varlength three steps out") {
//    forQuery("MATCH (a:A)-[r:T1*1..3]->(b:B)").
//      shouldHaveQueryGraphCardinality(
//        A * B * A_T1_A_sel + // The result includes all (:A)-[:T1]->(:B)
//        A * N * B * A_T1_STAR_sel * STAR_T1_B_sel + // and all (:A)-[:T1]->()-[:T1]->(:B)
//        A * N * N * B * A_T1_STAR_sel * STAR_T1_STAR_sel * STAR_T1_B_sel  // and all (:A)-[:T1]->()-[:T1]->()-[:T1]-(:B)
//      )
//  }

  def createCardinalityModel(stats: GraphStatistics): QueryGraphCardinalityModel =
    AssumeIndependenceQueryGraphCardinalityModel(stats, combiner)
}
