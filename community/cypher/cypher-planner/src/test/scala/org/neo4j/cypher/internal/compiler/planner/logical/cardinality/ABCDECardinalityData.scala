/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality

import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.schema.IndexType

import scala.util.Random

trait ABCDECardinalityData extends CardinalityIntegrationTestSupport {
  self: CypherFunSuite =>

  /**
   * Minimum number of nodes in the database.
   * This value is needed for correct cardinality estimations.
   */
  private val MIN_N = 1500

  /**
   * Total number of nodes in the database.
   */
  val N: Double = Random.nextDouble() * 1E6 + MIN_N

  val Asel: Double = .2 // How selective a :A predicate is
  val Bsel: Double = .1 // How selective a :B predicate is
  val Csel: Double = .01 // How selective a :C predicate is
  val Dsel: Double = .001 // How selective a :D predicate is
  val Esel: Double = Bsel // How selective a :E predicate is
  val Tsel: Double = Bsel
  val R1sel: Double = Bsel
  val R2sel: Double = Bsel

  val A: Double = N * Asel // Nodes with label A
  val B: Double = N * Bsel // Nodes with label B
  val C: Double = N * Csel // Nodes with label C
  val D: Double = N * Dsel // Nodes with label D
  val E: Double = N * Esel // Nodes with label E
  val R1: Double = N * R1sel
  val R2: Double = N * R2sel
  val T: Double = N * Tsel // Nodes with label T

  val UnindexedProp = DEFAULT_PROPERTY_SELECTIVITY * DEFAULT_EQUALITY_SELECTIVITY // Selectivity of prop (of node with no label) to exist and be a unique value
  val ApropExists: Double = 1.0   // Exists selectivity of index on :A(prop)
  val ApropUnique: Double = 0.5   // Unique selectivity of index on :A(prop)
  val Aprop: Double = ApropExists * ApropUnique // Selectivity of :A(prop) to exist and be unique
  val Bprop: Double = 0.003       // Unique selectivity of index on :B(prop)
  val Abar: Double = 0.002        // Unique selectivity of index on :A(bar)
  val CpropBarUnique: Double = 0.01 // Unique selectivity of index on :C(prop, bar)
  val CpropBarExists: Double = 0.7 // Exists selectivity of index on :C(prop, bar)
  val DfooBarBazUnique: Double = 0.0006 // Unique selectivity of index on :D(foo, bar, baz)
  val DfooBarBazExists: Double = 0.2 // Exists selectivity of index on :D(foo, bar, baz)
  val EsomeUnique: Double = 0.3   // Unique selectivity of index on :E(some)
  val EsomeExists: Double = 0.5   // Exists selectivity of index on :E(some)
  val TpropExists: Double = 0.7   // Exists selectivity of index on :T(prop)
  val TpropUnique: Double = 0.3   // Unique selectivity of index on :T(prop)

  val R1propExists: Double = 0.8   // Exists selectivity of index on :R1(prop)
  val R1propUnique: Double = 0.2   // Unique selectivity of index on :R1(prop)
  val R2fooBarExists: Double = 0.9   // Exists selectivity of index on :R2(foo, bar)
  val R2fooBarUnique: Double = 0.1   // Unique selectivity of index on :R2(foo, bar)

  val T1prop: Double = 0.003 // Selectivity of index on :T1(prop)
  val T2propFooExists: Double = 0.2   // Exists selectivity of index on :T2(prop, foo)
  val T2propFooUnique: Double = 0.009 // Unique selectivity of index on :T2(prop, foo)

  // Multipliers for patterns

  val A_T1_A_sel: Double = 5.0 / A
  val A_T1_B_sel: Double = 0.5
  val A_T1_C_sel: Double = 0.05
  val A_T1_D_sel: Double = 0.005
  val A_T1_E_sel: Double = 0.0
  val A_T2_A_sel: Double = 0.0
  val A_T2_B_sel: Double = 5.0 // On avg more than 1 T2 rel per (A,B) tuple.
  val A_T2_C_sel: Double = 0.0
  val A_T2_D_sel: Double = 0.0
  val A_T2_E_sel: Double = 0.0

  val B_T1_A_sel: Double = 0.01
  val B_T1_B_sel: Double = 10.0 / B
  val B_T1_C_sel: Double = 0.1
  val B_T1_D_sel: Double = 0.001
  val B_T1_E_sel: Double = 0.0
  val B_T2_A_sel: Double = 0.0
  val B_T2_B_sel: Double = 0.0
  val B_T2_C_sel: Double = 0.0031
  val B_T2_D_sel: Double = 0.0
  val B_T2_E_sel: Double = 0.0

  val C_T1_A_sel: Double = 0.0
  val C_T1_B_sel: Double = 0.0
  val C_T1_C_sel: Double = 0.05
  val C_T1_D_sel: Double = 0.02
  val C_T1_E_sel: Double = 0.0
  val C_T2_A_sel: Double = 0.0
  val C_T2_B_sel: Double = 0.0
  val C_T2_C_sel: Double = 0.0
  val C_T2_D_sel: Double = 0.0
  val C_T2_E_sel: Double = 0.0

  val D_T1_A_sel: Double = 0.0
  val D_T1_B_sel: Double = 0.0
  val D_T1_C_sel: Double = 0.3
  val D_T1_D_sel: Double = 0.0
  val D_T1_E_sel: Double = 0.0
  val D_T2_A_sel: Double = 0.0
  val D_T2_B_sel: Double = 0.0
  val D_T2_C_sel: Double = 0.07
  val D_T2_D_sel: Double = 0.0
  val D_T2_E_sel: Double = 0.0

  val E_T1_A_sel: Double = 0.0
  val E_T1_B_sel: Double = 0.0
  val E_T1_C_sel: Double = 0.0
  val E_T1_D_sel: Double = 0.0
  val E_T1_E_sel: Double = 0.0
  val E_T2_A_sel: Double = 0.0
  val E_T2_B_sel: Double = 0.01
  val E_T2_C_sel: Double = 0.01
  val E_T2_D_sel: Double = 0.001
  val E_T2_E_sel: Double = 0.0

  // Cardinalities for patterns

  val A_T1_A: Double = A * A * A_T1_A_sel
  val A_T1_B: Double = A * B * A_T1_B_sel
  val A_T1_C: Double = A * C * A_T1_C_sel
  val A_T1_D: Double = A * D * A_T1_D_sel
  val A_T1_E: Double = A * E * A_T1_E_sel
  val A_T2_A: Double = A * A * A_T2_A_sel
  val A_T2_B: Double = A * B * A_T2_B_sel
  val A_T2_C: Double = A * C * A_T2_C_sel
  val A_T2_D: Double = A * D * A_T2_D_sel
  val A_T2_E: Double = A * E * A_T2_E_sel

  val B_T1_A: Double = B * A * B_T1_A_sel
  val B_T1_B: Double = B * B * B_T1_B_sel
  val B_T1_C: Double = B * C * B_T1_C_sel
  val B_T1_D: Double = B * D * B_T1_D_sel
  val B_T1_E: Double = B * E * B_T1_E_sel
  val B_T2_A: Double = B * A * B_T2_A_sel
  val B_T2_B: Double = B * B * B_T2_B_sel
  val B_T2_C: Double = B * C * B_T2_C_sel
  val B_T2_D: Double = B * D * B_T2_D_sel
  val B_T2_E: Double = B * E * B_T2_E_sel

  val C_T1_A: Double = C * A * C_T1_A_sel
  val C_T1_B: Double = C * B * C_T1_B_sel
  val C_T1_C: Double = C * C * C_T1_C_sel
  val C_T1_D: Double = C * D * C_T1_D_sel
  val C_T1_E: Double = C * E * C_T1_E_sel
  val C_T2_A: Double = C * A * C_T2_A_sel
  val C_T2_B: Double = C * B * C_T2_B_sel
  val C_T2_C: Double = C * C * C_T2_C_sel
  val C_T2_D: Double = C * D * C_T2_D_sel
  val C_T2_E: Double = C * E * C_T2_E_sel

  val D_T1_A: Double = D * A * D_T1_A_sel
  val D_T1_B: Double = D * B * D_T1_B_sel
  val D_T1_C: Double = D * C * D_T1_C_sel
  val D_T1_D: Double = D * D * D_T1_D_sel
  val D_T1_E: Double = D * E * D_T1_E_sel
  val D_T2_A: Double = D * A * D_T2_A_sel
  val D_T2_B: Double = D * B * D_T2_B_sel
  val D_T2_C: Double = D * C * D_T2_C_sel
  val D_T2_D: Double = D * D * D_T2_D_sel
  val D_T2_E: Double = D * E * D_T2_E_sel

  val E_T1_A: Double = E * A * E_T1_A_sel
  val E_T1_B: Double = E * B * E_T1_B_sel
  val E_T1_C: Double = E * C * E_T1_C_sel
  val E_T1_D: Double = E * D * E_T1_D_sel
  val E_T1_E: Double = E * E * E_T1_E_sel
  val E_T2_A: Double = E * A * E_T2_A_sel
  val E_T2_B: Double = E * B * E_T2_B_sel
  val E_T2_C: Double = E * C * E_T2_C_sel
  val E_T2_D: Double = E * D * E_T2_D_sel
  val E_T2_E: Double = E * E * E_T2_E_sel

  // Sums

  val A_T1_ANY: Double = A_T1_A + A_T1_B + A_T1_C + A_T1_D + A_T1_E
  val A_T1_ANY_sel: Double = A_T1_ANY / (N * A)
  val A_T2_ANY: Double = A_T2_A + A_T2_B + A_T2_C + A_T2_D + A_T2_E
  val A_ANY_ANY: Double = A_T1_ANY + A_T2_ANY
  val A_ANY_B: Double = A_T1_B + A_T2_B
  val ANY_T1_A: Double = A_T1_A + B_T1_A + C_T1_A + D_T1_A + E_T1_A
  val ANY_T1_A_sel: Double = ANY_T1_A / (N * A)
  val ANY_T2_A: Double = A_T2_A + B_T2_A + C_T2_A + D_T2_A + E_T2_A
  val ANY_ANY_A: Double = ANY_T1_A + ANY_T2_A

  val B_T1_ANY: Double = B_T1_A + B_T1_B + B_T1_C + B_T1_D + B_T1_E
  val B_T2_ANY: Double = B_T2_A + B_T2_B + B_T2_C + B_T2_D + B_T2_E
  val B_ANY_ANY: Double = B_T1_ANY + B_T2_ANY
  val ANY_T1_B: Double = A_T1_B + B_T1_B + C_T1_B + D_T1_B + E_T1_B
  val ANY_T1_B_sel: Double = ANY_T1_B / (N * B)
  val ANY_T2_B: Double = A_T2_B + B_T2_B + C_T2_B + D_T2_B + E_T2_B
  val ANY_ANY_B: Double = ANY_T1_B + ANY_T2_B

  val C_T1_ANY: Double = C_T1_A + C_T1_B + C_T1_C + C_T1_D + C_T1_E
  val C_T2_ANY: Double = C_T2_A + C_T2_B + C_T2_C + C_T2_D + C_T2_E
  val C_ANY_ANY: Double = C_T1_ANY + C_T2_ANY
  val ANY_T1_C: Double = A_T1_C + B_T1_C + C_T1_C + D_T1_C + E_T1_C
  val ANY_T2_C: Double = A_T2_C + B_T2_C + C_T2_C + D_T2_C + E_T2_C
  val ANY_T2_C_sel: Double = ANY_T2_C / (N * C)
  val ANY_ANY_C: Double = ANY_T1_C + ANY_T2_C

  val D_T1_ANY: Double = D_T1_A + D_T1_B + D_T1_C + D_T1_D + D_T1_E
  val D_T2_ANY: Double = D_T2_A + D_T2_B + D_T2_C + D_T2_D + D_T2_E
  val D_ANY_ANY: Double = D_T1_ANY + D_T2_ANY
  val ANY_T1_D: Double = A_T1_D + B_T1_D + C_T1_D + D_T1_D + E_T1_D
  val ANY_T2_D: Double = A_T2_D + B_T2_D + C_T2_D + D_T2_D + E_T2_D
  val ANY_ANY_D: Double = ANY_T1_D + ANY_T2_D

  val E_T1_ANY: Double = E_T1_A + E_T1_B + E_T1_C + E_T1_D + E_T1_E
  val E_T2_ANY: Double = E_T2_A + E_T2_B + E_T2_C + E_T2_D + E_T2_E
  val E_ANY_ANY: Double = E_T1_ANY + E_T2_ANY
  val ANY_T1_E: Double = A_T1_E + B_T1_E + C_T1_E + D_T1_E + E_T1_E
  val ANY_T2_E: Double = A_T2_E + B_T2_E + C_T2_E + D_T2_E + E_T2_E
  val ANY_ANY_E: Double = ANY_T1_E + ANY_T2_E

  val ANY_T1_ANY: Double = A_T1_ANY + B_T1_ANY + C_T1_ANY + D_T1_ANY + E_T1_ANY
  val ANY_T1_ANY_sel: Double = ANY_T1_ANY / (N * N)

  val ANY_T2_ANY: Double = A_T2_ANY + B_T2_ANY + C_T2_ANY + D_T2_ANY + E_T2_ANY
  val ANY_T2_ANY_sel: Double = ANY_T2_ANY / (N * N)
  val ANY_T2_B_sel: Double = ANY_T2_B / (N * B)

  // Relationship count: the total number of relationships in the system
  val R: Double = A_ANY_ANY + B_ANY_ANY + C_ANY_ANY + D_ANY_ANY + E_ANY_ANY
  val R_sel: Double = R / (N * N)

  def getIndexType: IndexType = IndexType.RANGE

  override protected def plannerBuilder(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    super.plannerBuilder()
      .setAllNodesCardinality(N)
      .setAllRelationshipsCardinality(R)
      .setLabelCardinality("A", A)
      .setLabelCardinality("B", B)
      .setLabelCardinality("C", C)
      .setLabelCardinality("D", D)
      .setLabelCardinality("E", E)
      .setLabelCardinality("R1", R1)
      .setLabelCardinality("R2", R2)
      .setLabelCardinality("T", T)
      .setLabelCardinality("EMPTY", 0.0)

      .addNodeIndex("A", Seq("prop"), 1.0, Aprop, indexType = getIndexType)
      .addNodeIndex("B", Seq("prop"), 1.0, Bprop, indexType = getIndexType)
      .addNodeIndex("A", Seq("bar"), 1.0, Abar, indexType = getIndexType)
      .addNodeIndex("C", Seq("prop", "bar"), CpropBarExists, CpropBarUnique, indexType = getIndexType)
      .addNodeIndex("D", Seq("foo", "bar", "baz"), DfooBarBazExists, DfooBarBazUnique, indexType = getIndexType)
      .addNodeIndex("E", Seq("some"), EsomeExists, EsomeUnique, indexType = getIndexType)

      .addNodeIndex("T", Seq("prop"), TpropExists, TpropUnique, indexType = IndexType.TEXT)

      // make sure that we can cope with duplicate indexes
      .addNodeIndex("R1", Seq("prop"), R1propExists, R1propUnique, indexType = IndexType.RANGE)
      .addNodeIndex("R2", Seq("foo", "bar"), R2fooBarExists, R2fooBarUnique, indexType = IndexType.RANGE)
      .addNodeIndex("R1", Seq("prop"), R1propExists, R1propUnique, indexType = IndexType.TEXT)
      .addNodeIndex("R2", Seq("foo", "bar"), R2fooBarExists, R2fooBarUnique, indexType = IndexType.TEXT)

      .addNodeExistenceConstraint("C", "bar")

      .setRelationshipCardinality("()-[:T1]->()", ANY_T1_ANY)
      .setRelationshipCardinality("()-[:T2]->()", ANY_T2_ANY)
      .setRelationshipCardinality("()-[]->(:A)", ANY_ANY_A)
      .setRelationshipCardinality("()-[:T1]->(:A)", ANY_T1_A)
      .setRelationshipCardinality("()-[:T2]->(:A)", ANY_T2_A)
      .setRelationshipCardinality("()-[]->(:B)", ANY_ANY_B)
      .setRelationshipCardinality("()-[:T1]->(:B)", ANY_T1_B)
      .setRelationshipCardinality("()-[:T2]->(:B)", ANY_T2_B)
      .setRelationshipCardinality("()-[]->(:C)", ANY_ANY_C)
      .setRelationshipCardinality("()-[:T1]->(:C)", ANY_T1_C)
      .setRelationshipCardinality("()-[:T2]->(:C)", ANY_T2_C)
      .setRelationshipCardinality("()-[]->(:D)", ANY_ANY_D)
      .setRelationshipCardinality("()-[:T1]->(:D)", ANY_T1_D)
      .setRelationshipCardinality("()-[:T2]->(:D)", ANY_T2_D)
      .setRelationshipCardinality("()-[]->(:E)", ANY_ANY_E)
      .setRelationshipCardinality("()-[:T1]->(:E)", ANY_T1_E)
      .setRelationshipCardinality("()-[:T2]->(:E)", ANY_T2_E)

      .setRelationshipCardinality("(:A)-[]->()", A_ANY_ANY)
      .setRelationshipCardinality("(:A)-[]->(:B)", A_ANY_B)
      .setRelationshipCardinality("(:A)-[:T1]->()", A_T1_ANY)
      .setRelationshipCardinality("(:A)-[:T2]->()", A_T2_ANY)
      .setRelationshipCardinality("(:A)-[:T1]->(:A)", A_T1_A)
      .setRelationshipCardinality("(:A)-[:T2]->(:A)", A_T2_A)
      .setRelationshipCardinality("(:A)-[:T1]->(:B)", A_T1_B)
      .setRelationshipCardinality("(:A)-[:T2]->(:B)", A_T2_B)
      .setRelationshipCardinality("(:A)-[:T1]->(:C)", A_T1_C)
      .setRelationshipCardinality("(:A)-[:T2]->(:C)", A_T2_C)
      .setRelationshipCardinality("(:A)-[:T1]->(:D)", A_T1_D)
      .setRelationshipCardinality("(:A)-[:T2]->(:D)", A_T2_D)
      .setRelationshipCardinality("(:A)-[:T1]->(:E)", A_T1_E)
      .setRelationshipCardinality("(:A)-[:T2]->(:E)", A_T2_E)

      .setRelationshipCardinality("(:B)-[]->()", B_ANY_ANY)
      .setRelationshipCardinality("(:B)-[:T1]->()", B_T1_ANY)
      .setRelationshipCardinality("(:B)-[:T2]->()", B_T2_ANY)
      .setRelationshipCardinality("(:B)-[:T1]->(:A)", B_T1_A)
      .setRelationshipCardinality("(:B)-[:T2]->(:A)", B_T2_A)
      .setRelationshipCardinality("(:B)-[:T1]->(:B)", B_T1_B)
      .setRelationshipCardinality("(:B)-[:T2]->(:B)", B_T2_B)
      .setRelationshipCardinality("(:B)-[:T1]->(:C)", B_T1_C)
      .setRelationshipCardinality("(:B)-[:T2]->(:C)", B_T2_C)
      .setRelationshipCardinality("(:B)-[:T1]->(:D)", B_T1_D)
      .setRelationshipCardinality("(:B)-[:T2]->(:D)", B_T2_D)
      .setRelationshipCardinality("(:B)-[:T1]->(:E)", B_T1_E)
      .setRelationshipCardinality("(:B)-[:T2]->(:E)", B_T2_E)

      .setRelationshipCardinality("(:C)-[]->()", C_ANY_ANY)
      .setRelationshipCardinality("(:C)-[:T1]->()", C_T1_ANY)
      .setRelationshipCardinality("(:C)-[:T2]->()", C_T2_ANY)
      .setRelationshipCardinality("(:C)-[:T1]->(:A)", C_T1_A)
      .setRelationshipCardinality("(:C)-[:T2]->(:A)", C_T2_A)
      .setRelationshipCardinality("(:C)-[:T1]->(:B)", C_T1_B)
      .setRelationshipCardinality("(:C)-[:T2]->(:B)", C_T2_B)
      .setRelationshipCardinality("(:C)-[:T1]->(:C)", C_T1_C)
      .setRelationshipCardinality("(:C)-[:T2]->(:C)", C_T2_C)
      .setRelationshipCardinality("(:C)-[:T1]->(:D)", C_T1_D)
      .setRelationshipCardinality("(:C)-[:T2]->(:D)", C_T2_D)
      .setRelationshipCardinality("(:C)-[:T1]->(:E)", C_T1_E)
      .setRelationshipCardinality("(:C)-[:T2]->(:E)", C_T2_E)

      .setRelationshipCardinality("(:D)-[]->()", D_ANY_ANY)
      .setRelationshipCardinality("(:D)-[:T1]->()", D_T1_ANY)
      .setRelationshipCardinality("(:D)-[:T2]->()", D_T2_ANY)
      .setRelationshipCardinality("(:D)-[:T1]->(:A)", D_T1_A)
      .setRelationshipCardinality("(:D)-[:T2]->(:A)", D_T2_A)
      .setRelationshipCardinality("(:D)-[:T1]->(:B)", D_T1_B)
      .setRelationshipCardinality("(:D)-[:T2]->(:B)", D_T2_B)
      .setRelationshipCardinality("(:D)-[:T1]->(:C)", D_T1_C)
      .setRelationshipCardinality("(:D)-[:T2]->(:C)", D_T2_C)
      .setRelationshipCardinality("(:D)-[:T1]->(:D)", D_T1_D)
      .setRelationshipCardinality("(:D)-[:T2]->(:D)", D_T2_D)
      .setRelationshipCardinality("(:D)-[:T1]->(:E)", D_T1_E)
      .setRelationshipCardinality("(:D)-[:T2]->(:E)", D_T2_E)

      .setRelationshipCardinality("(:E)-[]->()", E_ANY_ANY)
      .setRelationshipCardinality("(:E)-[:T1]->()", E_T1_ANY)
      .setRelationshipCardinality("(:E)-[:T2]->()", E_T2_ANY)
      .setRelationshipCardinality("(:E)-[:T1]->(:A)", E_T1_A)
      .setRelationshipCardinality("(:E)-[:T2]->(:A)", E_T2_A)
      .setRelationshipCardinality("(:E)-[:T1]->(:B)", E_T1_B)
      .setRelationshipCardinality("(:E)-[:T2]->(:B)", E_T2_B)
      .setRelationshipCardinality("(:E)-[:T1]->(:C)", E_T1_C)
      .setRelationshipCardinality("(:E)-[:T2]->(:C)", E_T2_C)
      .setRelationshipCardinality("(:E)-[:T1]->(:D)", E_T1_D)
      .setRelationshipCardinality("(:E)-[:T2]->(:D)", E_T2_D)
      .setRelationshipCardinality("(:E)-[:T1]->(:E)", E_T1_E)
      .setRelationshipCardinality("(:E)-[:T2]->(:E)", E_T2_E)

      .addRelationshipIndex("T1", Seq("prop"), 1.0, T1prop, indexType = getIndexType)
      .addRelationshipIndex("T2", Seq("prop", "foo"), T2propFooExists, T2propFooUnique, indexType = getIndexType)
}
