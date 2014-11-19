/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.CardinalityTestHelper
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.assumeIndependence.{AssumeIndependenceQueryGraphCardinalityModel, IndependenceCombiner}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_2.planner.{LogicalPlanningTestSupport, SemanticTable}
import org.neo4j.cypher.internal.compiler.v2_2.planner.{SemanticTable, LogicalPlanningTestSupport}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{Cardinality, CardinalityTestHelper}
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics
import org.neo4j.cypher.internal.helpers.testRandomizer

class AssumeIndependenceQueryGraphCardinalityModelTest extends CypherFunSuite with LogicalPlanningTestSupport with CardinalityTestHelper {


  // Glossary:
  val N: Double  = testRandomizer.nextDouble() * 1E6 // Graph node count - the god number.
  println("N: " + N)
  val Asel = .2          // How selective a :A predicate is
  val Bsel = .1          // How selective a :B predicate is
  val Csel = .01         // How selective a :C predicate is
  val Dsel = .001        // How selective a :D predicate is
  val A: Double = N * Asel // Nodes with label A
  val B: Double = N * Bsel // Nodes with label B
  val C: Double = N * Csel // Nodes with label C
  val D: Double = N * Dsel // Nodes with label D

  val Aprop = 0.5        // Selectivity of index on :A(prop)
  val Bprop = 0.003      // Selectivity of index on :B(prop)
  val Abar = 0.002       // Selectivity of index on :A(bar)

  val A_T1_A_sel = 5.0 / A // Numbers of relationships of type T1 between A and B respectively labeled nodes
  val A_T1_B_sel = 0.5
  val A_T1_C_sel = 0.05
  val A_T1_D_sel = 0.005

  val A_T1_A    = A * A * A_T1_A_sel
  val A_T1_B    = A * B * A_T1_B_sel
  val A_T1_C    = A * C * A_T1_C_sel
  val A_T1_D    = A * D * A_T1_D_sel
  val A_T1_STAR = A_T1_A + A_T1_B + A_T1_C + A_T1_D

  val B_T1_B_sel = 10.0 / B
  val B_T1_C_sel = 0.1
  val B_T1_A_sel = 0.01
  val B_T1_D_sel = 0.001

  val B_T1_B    = B * B * B_T1_B_sel
  val B_T1_C    = B * C * B_T1_C_sel
  val B_T1_A    = B * A * B_T1_A_sel
  val B_T1_D    = B * D * B_T1_D_sel
  val B_T1_STAR = B_T1_A + B_T1_B + B_T1_C + B_T1_D
  val STAR_T1_B = B_T1_B + A_T1_B

  val D_T1_C_sel = 0.3
  val D_T1_C     = D * C * D_T1_C_sel

  val A_T2_A_sel = 0
  val A_T2_B_sel = 5

  val A_T2_A    = A * A * A_T2_A_sel
  val A_T2_B    = A * B * A_T2_B_sel
  val A_T2_STAR = A_T2_A + A_T2_B
  val STAR_T2_B = A_T2_B + 0 // B_T2_B

  val D_T2_C_sel = 0.07
  val D_T2_C     = D * C * D_T2_C_sel

  // Relationship count: the total number of relationships in the system
  val R = A_T1_STAR + B_T1_STAR + A_T2_STAR + D_T1_C + D_T2_C

  test("all nodes is gotten from stats") {
    forQuery("MATCH (n)").
    shouldHaveQueryGraphCardinality(N)
  }

  test("all nodes of given label") {
    forQuery("MATCH (n:A)").
    shouldHaveQueryGraphCardinality(A)
  }

  test("cross product of all nodes of two labels") {
    forQuery("MATCH (n:A) MATCH (m:B)").
    shouldHaveQueryGraphCardinality(A * B)
  }

  test("cross product of all nodes") {
    forQuery("MATCH a, b").
    shouldHaveQueryGraphCardinality(N * N)
  }

  test("empty pattern yields single result") {
    forQuery("").
    shouldHaveQueryGraphCardinality(1)
  }

  test("cross product of all nodes and a label scan") {
    forQuery("MATCH a, (b:B)").
    shouldHaveQueryGraphCardinality(N * B)
  }

  test("node cardinality given multiple labels") {
    forQuery("MATCH (a:A:B)").
    shouldHaveQueryGraphCardinality(N * Asel * Bsel)
  }

  test("node cardinality given multiple labels 2") {
    forQuery("MATCH (a:B:A)").
    shouldHaveQueryGraphCardinality(N * Asel * Bsel)
  }

  test("node cardinality when label is missing from store") {
    forQuery("MATCH (a:Z)").
    shouldHaveQueryGraphCardinality(0)
  }

  test("node cardinality when label is missing from store 2") {
    forQuery("MATCH (a:A:Z)").
    shouldHaveQueryGraphCardinality(0)
  }

  test("node cardinality when one label is missing") {
    forQuery("MATCH (a:Z:B)").
    shouldHaveQueryGraphCardinality(0)
  }

  test("cardinality for label and property equality when index is present") {
    forQuery("MATCH (a:A) WHERE a.prop = 42").
    shouldHaveQueryGraphCardinality(A * Aprop)
  }

  test("cardinality for label and property equality when index is not present") {
    forQuery("MATCH (a:B) WHERE a.bar = 42").
    shouldHaveQueryGraphCardinality(B * DEFAULT_EQUALITY_SELECTIVITY)
  }

  test("cardinality for label and property NOT-equality when index is present") {
    forQuery("MATCH (a:A) WHERE NOT a.prop = 42").
    shouldHaveQueryGraphCardinality(A * (1 - Aprop))
  }

  test("cardinality for multiple OR:ed equality predicates on a single index") {
    forQuery("MATCH (a:A) WHERE a.prop = 42 OR a.prop = 43").
    shouldHaveQueryGraphCardinality(A * or(Aprop, Aprop))
  }

  test("cardinality for multiple OR:ed equality predicates on two indexes") {
    forQuery("MATCH (a:A) WHERE a.prop = 42 OR a.bar = 43").
    shouldHaveQueryGraphCardinality(A * or(Aprop, Abar))
  }

  test("cardinality for multiple OR:ed equality predicates where one is backed by index and one is not") {
    forQuery("MATCH (a:B) WHERE a.prop = 42 OR a.bar = 43").
    shouldHaveQueryGraphCardinality(B * or(Bprop, DEFAULT_EQUALITY_SELECTIVITY))
  }

  ignore("cardinality for property equality predicate when property name is unknown (does not exist in the store)") {
    // This should work
    forQuery("MATCH (a) WHERE a.unknownProp = 42").
    shouldHaveQueryGraphCardinality(0)
  }

  test("cardinality for literal false") {
    forQuery("MATCH (a) WHERE false").
    shouldHaveQueryGraphCardinality(0)
  }

  test("cardinality for multiple AND:ed equality predicates on two indexes") {
    forQuery("MATCH (a:A) WHERE a.prop = 42 AND a.bar = 43").
    shouldHaveQueryGraphCardinality(A * Aprop * Abar)
  }

  test("cardinality for multiple AND:ed equality predicates where one is backed by index and one is not") {
    forQuery("MATCH (a:B) WHERE a.prop = 42 AND a.bar = 43").
    shouldHaveQueryGraphCardinality(B * Bprop * DEFAULT_EQUALITY_SELECTIVITY)
  }

  test("relationship cardinality given no labels or types") {
    forQuery("MATCH (a)-->(b)").
    shouldHaveQueryGraphCardinality(R)
  }

  test("relationship cardinality given labels on both sides") {
    forQuery("MATCH (a:A)-[r:T1]->(b:B)").
    shouldHaveQueryGraphCardinality(A_T1_B)
  }

  test("relationship cardinality given labels on both sides for the reverse direction") {
    forQuery("MATCH (b:B)<-[r:T1]-(a:A)").
    shouldHaveQueryGraphCardinality(A_T1_B)
  }

  test("relationship cardinality given labels on both sides bidirectional") {
    forQuery("MATCH (a:A)-[r:T1]-(b:B)").
    shouldHaveQueryGraphCardinality(A * B * or(A_T1_B_sel, B_T1_A_sel))
  }

  test("relationship cardinality given a label on one side") {
    forQuery("MATCH (a:A)-[r:T1]->(b)").
    shouldHaveQueryGraphCardinality(A_T1_STAR)
  }

  test("relationship cardinality given a label on one side bidirectional") {
    val STAR_T1_A = B_T1_A
    forQuery("MATCH (a:A)-[r:T1]-(b)").
    shouldHaveQueryGraphCardinality(A_T1_STAR + STAR_T1_A)
  }

  test("cardinality for rel-patterns with multiple labels on one end") {
    val maxRelCount = N * N * Asel * Bsel
    val B_T2_STAR = 0
    val A_relSelectivity = (A_T1_STAR + A_T2_STAR) / maxRelCount
    val B_relSelectivity = (B_T1_STAR + B_T2_STAR) / maxRelCount
    val relSelectivity = A_relSelectivity * B_relSelectivity
    forQuery("MATCH (a:A:B)-->()").
    shouldHaveQueryGraphCardinality(A * B * relSelectivity)
  }

  test("cardinality for rel-patterns with multiple rel types") {
    val patternNodeCrossProduct = N * N
    val labelSelectivity = Asel * Bsel
    val maxRelCount = patternNodeCrossProduct * labelSelectivity
    val relSelectivity = (A_T1_B + A_T2_B) / maxRelCount - (A_T1_B / maxRelCount) * (A_T2_B / maxRelCount)

    forQuery("MATCH (a:A)-[:T1|:T2]->(:B)").
    shouldHaveQueryGraphCardinality(patternNodeCrossProduct * labelSelectivity * relSelectivity)
  }

  test("cardinality for rel-patterns with multiple rel types and multiple labels on one side - test 1") {
    val B_T2_STAR = 0
    val patternNodeCrossProduct = N * N
    val labelSelectivity = Asel * Bsel
    val maxRelCount = patternNodeCrossProduct * labelSelectivity
    val relSelectivityT1 = (A_T1_STAR / maxRelCount) * (B_T1_STAR / maxRelCount)
    val relSelectivityT2 = (A_T2_STAR / maxRelCount) * (B_T2_STAR / maxRelCount)
    val relSelectivity = or(relSelectivityT1, relSelectivityT2)

    forQuery("MATCH (a:A:B)-[:T1|:T2]->()").
    shouldHaveQueryGraphCardinality(patternNodeCrossProduct * labelSelectivity * relSelectivity)
  }

  test("cardinality for rel-patterns with multiple rel types and multiple labels on one side - test 2") {
    val D_T1_STAR = D_T1_C
    val D_T2_STAR = D_T2_C
    val patternNodeCrossProduct = N * N
    val labelSelectivity = Asel * Dsel
    val maxRelCount = patternNodeCrossProduct * labelSelectivity
    val relSelectivityT1 = (A_T1_STAR / maxRelCount) * (D_T1_STAR / maxRelCount)
    val relSelectivityT2 = (A_T2_STAR / maxRelCount) * (D_T2_STAR / maxRelCount)
    val relSelectivity = or(relSelectivityT1, relSelectivityT2)

    forQuery("MATCH (a:A:D)-[:T1|:T2]->()").
    shouldHaveQueryGraphCardinality(patternNodeCrossProduct * labelSelectivity * relSelectivity)
  }

  //TODO this test is weird since relSelectivity will often have relSelectivity >> 1?
  test("cardinality for rel-patterns with multiple rel types and multiple labels on both sides") {
    val A_T2_C = 0
    val A_T2_D = 0
    val B_T2_C = 0
    val B_T2_D = 0
    val patternNodeCrossProduct = N * N
    val labelSelectivity = Asel * Bsel * Csel * Dsel
    val maxRelCount = patternNodeCrossProduct * labelSelectivity
    val relSelT1 = (A_T1_C / maxRelCount) * (A_T1_D / maxRelCount) * (B_T1_C / maxRelCount) * (B_T1_D / maxRelCount)
    val relSelT2 = (A_T2_C / maxRelCount) * (A_T2_D / maxRelCount) * (B_T2_C / maxRelCount) * (B_T2_D / maxRelCount)
    val relSelectivity = or(relSelT1, relSelT2)

    forQuery("MATCH (a:A:B)-[:T1|:T2]->(c:C:D)").
    shouldHaveQueryGraphCardinality(patternNodeCrossProduct * labelSelectivity * relSelectivity)
  }

  test("given empty database, all predicates should return 0 cardinality") {
    givenPattern("MATCH a WHERE a.prop = 10").
    withGraphNodes(0).
    withKnownProperty('prop).
    shouldHaveQueryGraphCardinality(0)
  }

  test("optional match from a node with no label specified") {
    forQuery("MATCH (a) OPTIONAL MATCH (a)-[:T1]->(:B)").
    shouldHaveQueryGraphCardinality(A_T1_B + B_T1_B)
  }

  test("optional match from a known label") {
    forQuery("MATCH (a:A) OPTIONAL MATCH (a)-[:T1]->(:B)").
    shouldHaveQueryGraphCardinality(A_T1_B)
  }

  test("predicates in optional match do not decrease the cardinality matches") {
    forQuery("MATCH (a:A) OPTIONAL MATCH (a)-[:MISSING]->()").
    shouldHaveQueryGraphCardinality(A)
  }

  ignore("honours bound arguments") {
    givenPattern("MATCH (a:FOO)-[:TYPE]->(b:BAR)").
    withQueryGraphArgumentIds(IdName("a")).
    withInboundCardinality(13.0).
    withGraphNodes(500).
    withLabel('FOO -> 100).
    withLabel('BAR -> 400).
    withRelationshipCardinality('FOO -> 'TYPE -> 'BAR -> 1000).
    shouldHaveQueryGraphCardinality(1000.0 / 500.0 * 13.0)
  }

  test("optional match will in worst case be a cartesian product") {
    forQuery("MATCH (a) OPTIONAL MATCH (b)").
    shouldHaveQueryGraphCardinality(N * N)
  }

  test("multiple optional matches - multiple cross joins") {
    forQuery("MATCH (a:A) OPTIONAL MATCH (b:B) OPTIONAL MATCH (c:C)").
    shouldHaveQueryGraphCardinality(A * B * C)
  }

  test("node by id should be recognized as such") {
    forQuery("MATCH (a:A) WHERE id(a) IN [1,2,3]").
    shouldHaveQueryGraphCardinality(A * (3.0 / N))
  }

  test("two levels") {
    val selA = .1
    val selB = .2
    val selC = .3
    val _A = N * selA
    val _B = N * selB
    val _C = N * selC
    val selT1 = .01
    val selT2 = .02
    givenPattern("MATCH (a:A)-[:T1]->(b:B)-[:T2]->(c:C)").
    withGraphNodes(N).
    withLabel('A, _A).
    withLabel('B, _B).
    withLabel('C, _C).
    withRelationshipCardinality('A -> 'T1 -> 'B, _A * _B * selT1).
    withRelationshipCardinality('B -> 'T2 -> 'C, _B * _C * selT2).
    shouldHaveQueryGraphCardinality(_A * selT1 * _B * selT2 * _C)
  }

  test("three levels") {
    val selA = .1
    val selB = .2
    val selC = .3
    val selD = .4
    val _A = N * selA
    val _B = N * selB
    val _C = N * selC
    val _D = N * selD
    val selT1 = .01
    val selT2 = .02
    val selT3 = .03
    givenPattern("MATCH (a:A)-[:T1]->(b:B)-[:T2]->(c:C)-[:T3]->(d:D)").
    withGraphNodes(N).
    withLabel('A, _A).
    withLabel('B, _B).
    withLabel('C, _C).
    withLabel('D, _D).
    withRelationshipCardinality('A -> 'T1 -> 'B, _A * _B * selT1).
    withRelationshipCardinality('B -> 'T2 -> 'C, _B * _C * selT2).
    withRelationshipCardinality('C -> 'T3 -> 'D, _C * _D * selT3).
    shouldHaveQueryGraphCardinality(_A * selT1 * _B * selT2 * _C * selT3 * _D)
  }

  test("four levels") {
    val Esel = 0.12
    val C_T3_E_sel = 0.04
    val E = N * Esel
    val C_T3_E = C * E * C_T3_E_sel
    forQuery("MATCH (b:B)-[:T1]->(a:A)-[:T1]->(d:D)-[:T1]->(c:C)-[:T3]->(e:E)").
      withLabel('E,E).
      withRelationshipCardinality('C -> 'T3 -> 'E, C_T3_E).
    shouldHaveQueryGraphCardinality(B * B_T1_A_sel * A * A_T1_D_sel * D * D_T1_C_sel * C * C_T3_E_sel * E / 8)
  }

  ignore("varlength two steps out") {
    forQuery("MATCH (a:A)-[r:T1*1..2]->(b:A)").
      shouldHaveQueryGraphCardinality(???)
  }

  ignore("two relationships with property") {
    val selectivity = R / (A * B * N)
    forQuery("MATCH (a:A)--()--(b:B) WHERE a.prop = 61").
    shouldHaveQueryGraphCardinality(A * N * B * selectivity * selectivity)
  }

  private def forQuery(q: String) =
    givenPattern(q).
    withGraphNodes(N).
    withLabel('A, A).
    withLabel('B, B).
    withLabel('C, C).
    withLabel('D, D).
    withLabel('EMPTY, 0).
    withIndexSelectivity(('A, 'prop) -> Aprop).
    withIndexSelectivity(('B, 'prop) -> Bprop).
    withIndexSelectivity(('A, 'bar) -> Abar).
    withRelationshipCardinality('A -> 'T1 -> 'A, A_T1_A).
    withRelationshipCardinality('A -> 'T1 -> 'B, A_T1_B).
    withRelationshipCardinality('A -> 'T1 -> 'C, A_T1_C).
    withRelationshipCardinality('A -> 'T1 -> 'D, A_T1_D).
    withRelationshipCardinality('B -> 'T1 -> 'B, B_T1_B).
    withRelationshipCardinality('B -> 'T1 -> 'C, B_T1_C).
    withRelationshipCardinality('B -> 'T1 -> 'A, B_T1_A).
    withRelationshipCardinality('B -> 'T1 -> 'D, B_T1_D).
    withRelationshipCardinality('A -> 'T2 -> 'A, A_T2_A).
    withRelationshipCardinality('A -> 'T2 -> 'B, A_T2_B).
    withRelationshipCardinality('D -> 'T1 -> 'C, D_T1_C).
    withRelationshipCardinality('D -> 'T2 -> 'C, D_T2_C)

  def or(numbers: Double*) = 1 - numbers.map(1 - _).reduce(_ * _)

  implicit def toLong(d: Double): Long = d.toLong

  def createCardinalityModel(stats: GraphStatistics, semanticTable: SemanticTable): QueryGraphCardinalityModel =
    AssumeIndependenceQueryGraphCardinalityModel(stats, semanticTable, IndependenceCombiner)
}
