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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence

import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.ABCDECardinalityData
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.PatternRelationshipMultiplierCalculator.uniquenessSelectivityForNRels
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks.Table
import org.scalatest.prop.TableDrivenPropertyChecks.forAll

class AssumeIndependenceQueryGraphCardinalityModelTest extends CypherFunSuite with ABCDECardinalityData {

  test("all queries") {
    val queries = Table.apply[String, Double](
      ("query", "expected cardinality"),
      "MATCH (n)"
        -> N,

      "MATCH (n:A)"
        -> A,

      "MATCH (n:A) MATCH (m:B)"
        -> A * B,

      "MATCH (a), (b)" ->
        N * N,

      ""
        -> 1.0,

      "MATCH (a), (b:B)"
        -> N * B,

      "MATCH (a:A:B)"
        -> N * Asel * Bsel,

      "MATCH (a:B:A)"
        -> N * Asel * Bsel,

      "MATCH (a:A) WHERE a.prop = 42"
        -> A * Aprop,

      "MATCH (a:A) WHERE a.prop STARTS WITH 'p'"
        -> A * DEFAULT_RANGE_SEEK_FACTOR,

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

      "MATCH (a:A)-[r:T2]->(b:B)" // This pattern has on avg more than one rel per pair of nodes
        -> A_T2_B,

      "MATCH (b:B)<-[r:T1]-(a:A)"
        -> A_T1_B,

      "MATCH (a:A)-[r:T1]-(b:B)"
        -> A * B * (A_T1_B_sel + B_T1_A_sel),

      "MATCH (a:A)-[r:T1]->(b)"
        -> A_T1_ANY,

      "MATCH (a:A)-[r:T1]-(b)"
        -> (A_T1_ANY + ANY_T1_A),

      "MATCH (a:A)-[*0..0]-(b:A)"
        -> N * Asel * Asel // This is a simplification, because *0..0 relationships mean something weird and icky
                           // It's not really right, but should not be fixed by the cardinality model. It should have
                           // been rewritten away before this stage
      ,

      "MATCH (a:A:B)-->()"
        -> {
        val maxRelCount = N * Asel * Bsel * N
        val A_relMult = A_ANY_ANY / maxRelCount * Bsel
        val B_relMult = B_ANY_ANY / maxRelCount * Asel
        val relMult = Math.min(A_relMult, B_relMult)
        maxRelCount * relMult
      },

      "MATCH (a:A:B)-[:T1]->(:C)"
        -> {
        val maxRelCount = N * Asel * Bsel * N * Csel
        val A_relMult = A_T1_C / maxRelCount * Bsel
        val B_relMult = B_T1_C / maxRelCount * Asel
        val relMult = Math.min(A_relMult, B_relMult)
        maxRelCount * relMult
      },

      "MATCH (a:A)-[:T1|T2]->(:B)"
        -> {
        val maxRelCount = N  * Asel * N * Bsel
        val relMult = A_T1_B / maxRelCount + A_T2_B / maxRelCount
        maxRelCount * relMult
      },

      "MATCH (a:A:E)-[:T1|T2]->()"
        -> {
        val maxRelCount = N * Asel * Esel * N
        val relMultT1 = Math.min(A_T1_ANY / maxRelCount * Esel, E_T1_ANY / maxRelCount * Asel)
        val relMultT2 = Math.min(A_T2_ANY / maxRelCount * Esel, E_T2_ANY / maxRelCount * Asel)
        val relMult = relMultT1 + relMultT2
        maxRelCount * relMult
      },

      "MATCH (a:A:D)-[:T1|T2]->()"
        -> {
        val maxRelCount = N * Asel * Dsel * N
        val relMultT1 = Math.min(A_T1_ANY / maxRelCount * Dsel, D_T1_ANY / maxRelCount * Asel)
        val relMultT2 = Math.min(A_T2_ANY / maxRelCount * Dsel, D_T2_ANY / maxRelCount * Asel)
        val relMult = relMultT1 + relMultT2
        maxRelCount * relMult
      },

      "MATCH (a:A:B)-[:T1]->(c:C:D)"
        -> {
        val maxRelCount = N * Asel * Bsel * N * Csel * Dsel
        val relMult = Seq(
          A_T1_C / maxRelCount * Bsel * Dsel,
          A_T1_D / maxRelCount * Bsel * Csel,
          B_T1_C / maxRelCount * Asel * Dsel,
          B_T1_D / maxRelCount * Asel * Csel,
        ).min
        maxRelCount * relMult
      },

      "MATCH (a:A:B)-[:T1|T2]->(c:C:D)"
        -> {
        val maxRelCount = N * Asel * Bsel * N * Csel * Dsel
        val relMultT1 = Seq(
          A_T1_C / maxRelCount * Bsel * Dsel,
          A_T1_D / maxRelCount * Bsel * Csel,
          B_T1_C / maxRelCount * Asel * Dsel,
          B_T1_D / maxRelCount * Asel * Csel,
        ).min
        val relMultT2 = Seq(
          A_T2_C / maxRelCount * Bsel * Dsel,
          A_T2_D / maxRelCount * Bsel * Csel,
          B_T2_C / maxRelCount * Asel * Dsel,
          B_T2_D / maxRelCount * Asel * Csel,
        ).min
        val relMult = relMultT1 + relMultT2
        maxRelCount * relMult
      },

      "MATCH (a) OPTIONAL MATCH (a)-[:T1]->(:B)"
        -> (A_T1_B + B_T1_B),

      "MATCH (a:A) OPTIONAL MATCH (a)-[:T1]->(:B)"
        -> A_T1_B,

      "MATCH (a:A) OPTIONAL MATCH (a)-[:T2]->(:C)" // Does not exist
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
        -> N * N * N * Asel * A_T1_B_sel * Bsel * B_T1_C_sel * Csel * uniquenessSelectivityForNRels(2).factor,

      "MATCH (a:A)-[r1:T1]->(b:B)-[r2:T1]->(a)"
        -> N * N * Asel * A_T1_B_sel * Bsel * B_T1_A_sel * uniquenessSelectivityForNRels(2).factor,

      "MATCH (:A)-[r1:T1]->(:A)-[r2:T1]->(:B)-[r3:T1]->(:B)"
        -> A * A * B * B * A_T1_A_sel * A_T1_B_sel * B_T1_B_sel *
        uniquenessSelectivityForNRels(3).factor,

      "MATCH (:A)-[r1:T1]->(:A)-[r2:T1]->(:B)-[r3:T1]->(:B)-[r4:T2]->(c:C)"
        -> A * A * B * B * C * A_T1_A_sel * A_T1_B_sel * B_T1_B_sel * B_T2_C_sel *
        uniquenessSelectivityForNRels(3).factor // r4 has a different relType, so it does not need to be checked for rel-uniqueness against the other rels
    )

    forAll(queries) { (q: String, expected: Double) =>
      queryShouldHaveCardinality(q, expected)
    }
  }

  private val varLength0_0 = N * Asel * Bsel
  test("varlength 0..0 should be equal to non-varlength") {
    queryShouldHaveCardinality("MATCH (:A:B)", varLength0_0)
    queryShouldHaveCardinality("MATCH (a:A)-[r:T1*0..0]->(b:B)", varLength0_0)
  }

  private val varLength1_1 = A_T1_B
  test("varlength 1..1 should be equal to non-varlength") {
    queryShouldHaveCardinality("MATCH (:A)-[r1:T1]->(:B)", varLength1_1)
    queryShouldHaveCardinality("MATCH (a:A)-[r:T1*1..1]->(b:B)", varLength1_1)
  }

  test("varlength 0..1 should equal sum of 0..0 and 1..1") {
    queryShouldHaveCardinality("MATCH (a:A)-[r:T1*0..1]->(b:B)", varLength0_0 + varLength1_1)
  }

  private val varLength2_2 = A * N * B * A_T1_ANY_sel * ANY_T1_B_sel * uniquenessSelectivityForNRels(2).factor
  test("varlength 2..2 should be equal to non-varlength") {
    queryShouldHaveCardinality("MATCH (:A)-[r1:T1]->()-[r2:T1]->(:B)", varLength2_2)
    queryShouldHaveCardinality("MATCH (a:A)-[r:T1*2..2]->(b:B)", varLength2_2)
  }

  test("varlength 1..2 should equal sum of 1..1 and 2..2") {
    queryShouldHaveCardinality("MATCH (a:A)-[r:T1*1..2]->(b:B)", varLength1_1 + varLength2_2)
  }

  test("varlength 0..2 should equal sum of 0..0 and 1..1 and 2..2") {
    queryShouldHaveCardinality("MATCH (a:A)-[r:T1*0..2]->(b:B)", varLength0_0 + varLength1_1 + varLength2_2)
  }

  private val varLength3_3 = A * N * N * B * A_T1_ANY_sel * ANY_T1_ANY_sel * ANY_T1_B_sel * uniquenessSelectivityForNRels(3).factor
  test("varlength 3..3 should be equal to non-varlength") {
    // The result includes all (:A)-[:T1]->()-[:T1]->()-[:T1]->(:B)
    queryShouldHaveCardinality("MATCH (:A)-[r1:T1]->()-[r2:T1]->()-[r3:T1]->(:B)", varLength3_3)
    queryShouldHaveCardinality("MATCH (a:A)-[r:T1*3..3]->(b:B)", varLength3_3)
  }

  test("varlength 1..3 should equal sum of 1..1 and 2..2 and 3..3") {
    queryShouldHaveCardinality("MATCH (a:A)-[r:T1*1..3]->(b:B)", varLength1_1 + varLength2_2 + varLength3_3)
  }
}
