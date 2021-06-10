/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality

import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.util.Random

trait ABCDECardinalityData extends CardinalityModelIntegrationTest {
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

  val Asel = .2 // How selective a :A predicate is
  val Bsel = .1 // How selective a :B predicate is
  val Csel = .01 // How selective a :C predicate is
  val Dsel = .001 // How selective a :D predicate is
  val Esel = Bsel // How selective a :E predicate is

  val A = N * Asel // Nodes with label A
  val B = N * Bsel // Nodes with label B
  val C = N * Csel // Nodes with label C
  val D = N * Dsel // Nodes with label D
  val E = N * Esel // Nodes with label E

  val Aprop = 0.5         // Unique selectivity of index on :A(prop)
  val Bprop = 0.003       // Unique selectivity of index on :B(prop)
  val Abar = 0.002        // Unique selectivity of index on :A(bar)
  val CpropBarUnique = 0.01 // Unique selectivity of index on :C(prop, bar)
  val CpropBarExists = 0.7 // Exists selectivity of index on :C(prop, bar)
  val DfooBarBazUnique = 0.0006 // Unique selectivity of index on :D(foo, bar, baz)
  val DfooBarBazExists = 0.2 // Exists selectivity of index on :D(foo, bar, baz)
  val EsomeUnique = 0.3   // Unique selectivity of index on :E(some)
  val EsomeExists = 0.5   // Exists selectivity of index on :E(some)

  val T1prop = 0.003 // Selectivity of index on :T1(prop)
  val T2propFooExists = 0.2   // Exists selectivity of index on :T2(prop, foo)
  val T2propFooUnique = 0.009 // Unique selectivity of index on :T2(prop, foo)

  // Multipliers for patterns

  val A_T1_A_sel = 5.0 / A
  val A_T1_B_sel = 0.5
  val A_T1_C_sel = 0.05
  val A_T1_D_sel = 0.005
  val A_T2_A_sel = 0
  val A_T2_B_sel = 5 // On avg more than 1 T2 rel per (A,B) tuple.

  val B_T1_B_sel = 10.0 / B
  val B_T1_C_sel = 0.1
  val B_T1_A_sel = 0.01
  val B_T1_D_sel = 0.001
  val B_T2_C_sel = 0.0031

  val C_T1_D_sel = 0.02

  val D_T1_C_sel = 0.3
  val D_T2_C_sel = 0.07

  val E_T2_B_sel = 0.01
  val E_T2_C_sel = 0.01
  val E_T2_D_sel = 0.001

  // Cardinalities for patterns

  val A_T1_A = A * A * A_T1_A_sel
  val A_T1_B = A * B * A_T1_B_sel
  val A_T1_C = A * C * A_T1_C_sel
  val A_T1_D = A * D * A_T1_D_sel
  val A_T1_E = 0
  val A_T2_A = A * A * A_T2_A_sel
  val A_T2_B = A * B * A_T2_B_sel
  val A_T2_C = 0
  val A_T2_D = 0
  val A_T2_E = 0

  val B_T1_A = B * A * B_T1_A_sel
  val B_T1_B = B * B * B_T1_B_sel
  val B_T1_C = B * C * B_T1_C_sel
  val B_T1_D = B * D * B_T1_D_sel
  val B_T1_E = 0
  val B_T2_A = 0
  val B_T2_B = 0
  val B_T2_C = B * C * B_T2_C_sel
  val B_T2_D = 0
  val B_T2_E = 0

  val C_T1_A = 0
  val C_T1_B = 0
  val C_T1_C = 0
  val C_T1_D = C * D * C_T1_D_sel
  val C_T1_E = 0
  val C_T2_A = 0
  val C_T2_B = 0
  val C_T2_C = 0
  val C_T2_D = 0
  val C_T2_E = 0

  val D_T1_A = 0
  val D_T1_B = 0
  val D_T1_C = D * C * D_T1_C_sel
  val D_T1_D = 0
  val D_T1_E = 0
  val D_T2_A = 0
  val D_T2_B = 0
  val D_T2_C = D * C * D_T2_C_sel
  val D_T2_D = 0
  val D_T2_E = 0

  val E_T1_A = 0
  val E_T1_B = 0
  val E_T1_C = 0
  val E_T1_D = 0
  val E_T1_E = 0
  val E_T2_A = 0
  val E_T2_B = E * B * E_T2_B_sel
  val E_T2_C = E * C * E_T2_C_sel
  val E_T2_D = E * D * E_T2_D_sel
  val E_T2_E = 0

  // Sums

  val A_T1_ANY = A_T1_A + A_T1_B + A_T1_C + A_T1_D + A_T1_E
  val A_T1_ANY_sel = A_T1_ANY / (N * A)
  val A_T2_ANY = A_T2_A + A_T2_B + A_T2_C + A_T2_D + A_T2_E
  val A_ANY_ANY = A_T1_ANY + A_T2_ANY
  val ANY_T1_A = A_T1_A + B_T1_A + C_T1_A + D_T1_A + E_T1_A
  val ANY_T1_A_sel = ANY_T1_A / (N * A)
  val ANY_T2_A = A_T2_A + B_T2_A + C_T2_A + D_T2_A + E_T2_A
  val ANY_ANY_A = ANY_T1_A + ANY_T2_A

  val B_T1_ANY = B_T1_A + B_T1_B + B_T1_C + B_T1_D + B_T1_E
  val B_T2_ANY = B_T2_A + B_T2_B + B_T2_C + B_T2_D + B_T2_E
  val B_ANY_ANY = B_T1_ANY + B_T2_ANY
  val ANY_T1_B = A_T1_B + B_T1_B + C_T1_B + D_T1_B + E_T1_B
  val ANY_T1_B_sel = ANY_T1_B / (N * B)
  val ANY_T2_B = A_T2_B + B_T2_B + C_T2_B + D_T2_B + E_T2_B
  val ANY_ANY_B = ANY_T1_B + ANY_T2_B

  val C_T1_ANY = C_T1_A + C_T1_B + C_T1_C + C_T1_D + C_T1_E
  val C_T2_ANY = C_T2_A + C_T2_B + C_T2_C + C_T2_D + C_T2_E
  val C_ANY_ANY = C_T1_ANY + C_T2_ANY
  val ANY_T1_C = A_T1_C + B_T1_C + C_T1_C + D_T1_C + E_T1_C
  val ANY_T2_C = A_T2_C + B_T2_C + C_T2_C + D_T2_C + E_T2_C
  val ANY_T2_C_sel = ANY_T2_C / (N * C)
  val ANY_ANY_C = ANY_T1_C + ANY_T2_C

  val D_T1_ANY = D_T1_A + D_T1_B + D_T1_C + D_T1_D + D_T1_E
  val D_T2_ANY = D_T2_A + D_T2_B + D_T2_C + D_T2_D + D_T2_E
  val D_ANY_ANY = D_T1_ANY + D_T2_ANY
  val ANY_T1_D = A_T1_D + B_T1_D + C_T1_D + D_T1_D + E_T1_D
  val ANY_T2_D = A_T2_D + B_T2_D + C_T2_D + D_T2_D + E_T2_D
  val ANY_ANY_D = ANY_T1_D + ANY_T2_D

  val E_T1_ANY = E_T1_A + E_T1_B + E_T1_C + E_T1_D + E_T1_E
  val E_T2_ANY = E_T2_A + E_T2_B + E_T2_C + E_T2_D + E_T2_E
  val E_ANY_ANY = E_T1_ANY + E_T2_ANY
  val ANY_T1_E = A_T1_E + B_T1_E + C_T1_E + D_T1_E + E_T1_E
  val ANY_T2_E = A_T2_E + B_T2_E + C_T2_E + D_T2_E + E_T2_E
  val ANY_ANY_E = ANY_T1_E + ANY_T2_E

  val ANY_T1_ANY = A_T1_ANY + B_T1_ANY + C_T1_ANY + D_T1_ANY + E_T1_ANY
  val ANY_T1_ANY_sel = ANY_T1_ANY / (N * N)

  val ANY_T2_ANY = A_T2_ANY + B_T2_ANY + C_T2_ANY + D_T2_ANY + E_T2_ANY
  val ANY_T2_ANY_sel = ANY_T2_ANY / (N * N)

  // Relationship count: the total number of relationships in the system
  val R = A_ANY_ANY + B_ANY_ANY + C_ANY_ANY + D_ANY_ANY + E_ANY_ANY
  val R_sel = R / (N * N)

  override protected def plannerBuilder(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    super.plannerBuilder()
      .setAllNodesCardinality(N)
      .setAllRelationshipsCardinality(R)
      .setLabelCardinality("A", A)
      .setLabelCardinality("B", B)
      .setLabelCardinality("C", C)
      .setLabelCardinality("D", D)
      .setLabelCardinality("E", E)
      .setLabelCardinality("EMPTY", 0)

      .addNodeIndex("A", Seq("prop"), 1.0, Aprop)
      .addNodeIndex("B", Seq("prop"), 1.0, Bprop)
      .addNodeIndex("A", Seq("bar"), 1.0, Abar)
      .addNodeIndex("C", Seq("prop", "bar"), CpropBarExists, CpropBarUnique)
      .addNodeIndex("D", Seq("foo", "bar", "baz"), DfooBarBazExists, DfooBarBazUnique)
      .addNodeIndex("E", Seq("some"), EsomeExists, EsomeUnique)

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

      .addRelationshipIndex("T1", Seq("prop"), 1.0, T1prop)
      .addRelationshipIndex("T2", Seq("prop", "foo"), T2propFooExists, T2propFooUnique)
}
