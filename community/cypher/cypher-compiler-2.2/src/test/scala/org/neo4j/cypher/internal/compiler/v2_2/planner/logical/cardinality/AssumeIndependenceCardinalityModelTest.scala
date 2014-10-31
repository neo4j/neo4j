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
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.assumeIndependence.{IndependenceCombiner, AssumeIndependenceQueryGraphCardinalityModel}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_2.planner.{SemanticTable, LogicalPlanningTestSupport}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.CardinalityTestHelper
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics

class AssumeIndependenceCardinalityModelTest extends CypherFunSuite with LogicalPlanningTestSupport with CardinalityTestHelper {

  test("all nodes is gotten from stats") {
    givenPattern("MATCH (n)").
      withGraphNodes(425).
      shouldHaveCardinality(425)
  }

  test("all nodes of given label") {
    givenPattern("MATCH (n:A)").
      withGraphNodes(425).
      withLabel('A -> 42).
      shouldHaveCardinality(42)
  }

  test("cross product of all nodes of two labels") {
    givenPattern("MATCH (n:A) MATCH (m:B)").
      withGraphNodes(425).
      withLabel('A -> 42).
      withLabel('B -> 10).
      shouldHaveCardinality(42 * 10)
  }

  test("cross product of all nodes") {
    givenPattern("MATCH a, b").
      withGraphNodes(425).
      shouldHaveCardinality(425 * 425)
  }

  test("empty pattern yields single result") {
    givenPattern("").
      withGraphNodes(425).
      shouldHaveCardinality(1)
  }

  test("cross product of all nodes and a label scan") {
    givenPattern("MATCH a, (b:B)").
      withGraphNodes(40).
      withLabel('B -> 30).
      shouldHaveCardinality(40 * 30)
  }

  test("node cardinality given multiple labels") {
    givenPattern("MATCH (a:A:B)").
      withGraphNodes(40).
      withLabel('A -> 20).
      withLabel('B -> 30).
      shouldHaveCardinality(40.0 * (20.0 / 40 ) * (30.0 / 40))
  }

  test("node cardinality given multiple labels 2") {
    givenPattern("MATCH (a:A:B)").
      withGraphNodes(40).
      withLabel('A -> 30).
      withLabel('B -> 20).
      shouldHaveCardinality(40.0 * (30.0 / 40) *  (20.0 / 40))
  }

  test("node cardinality when label is missing from store") {
    givenPattern("MATCH (a:A)").
      withGraphNodes(40).
      shouldHaveCardinality(0)
  }

  test("node cardinality when label is missing from store 2") {
    givenPattern("MATCH (a:A:B)").
      withGraphNodes(40).
      withLabel('B -> 30).
      shouldHaveCardinality(0)
  }

  test("node cardinality when one label is missing empty") {
    givenPattern("MATCH (a:A:B)").
      withGraphNodes(40).
      withLabel('A -> 0).
      withLabel('B -> 30).
      shouldHaveCardinality(0)
  }

  test("cardinality for label and property equality when index is present") {
    givenPattern("MATCH (a:A) WHERE a.prop = 42").
      withGraphNodes(40).
      withLabel('A -> 30).
      withIndexSelectivity(('A, 'prop) -> .05).
      shouldHaveCardinality(30 * .05)
  }

  test("cardinality for label and property equality when index is not present") {
    givenPattern("MATCH (a:A) WHERE a.prop = 42").
      withGraphNodes(40).
      withKnownProperty('prop).
      withLabel('A -> 10).
      shouldHaveCardinality(40.0 * (10.0 / 40) * DEFAULT_EQUALITY_SELECTIVITY)
  }

  test("cardinality for label and property equality when index is not present 2") {
    givenPattern("MATCH (a:A) WHERE a.prop = 42").
      withGraphNodes(40).
      withKnownProperty('prop).
      withLabel('A -> 40).
      shouldHaveCardinality(40 * DEFAULT_EQUALITY_SELECTIVITY)
  }

  test("cardinality for label and property NOT-equality when index is present") {
    givenPattern("MATCH (a:A) WHERE NOT a.prop = 42").
      withGraphNodes(40).
      withLabel('A -> 30).
      withIndexSelectivity(('A, 'prop) -> .3).
      shouldHaveCardinality(30 * (1 - .3))
  }

  test("cardinality for multiple OR:ed equality predicates on a single index") {
    givenPattern("MATCH (a:A) WHERE a.prop = 42 OR a.prop = 43").
      withGraphNodes(40).
      withLabel('A -> 30).
      withIndexSelectivity(('A, 'prop) -> .01).
      shouldHaveCardinality(40 * (30.0 / 40) * (1 - (1 - .01) * (1 - .01)))
  }

  test("cardinality for multiple OR:ed equality predicates on two indexes") {
    givenPattern("MATCH (a:A) WHERE a.prop = 42 OR a.bar = 43").
      withGraphNodes(40).
      withLabel('A -> 30).
      withIndexSelectivity(('A, 'prop) -> .3).
      withIndexSelectivity(('A, 'bar) -> .4).
      shouldHaveCardinality(40.0 * (30.0 / 40) * (1 - (1 - .3) * (1 - .4)))
  }

  test("cardinality for multiple OR:ed equality predicates where one is backed by index and one is not") {
    givenPattern("MATCH (a:A) WHERE a.prop = 42 OR a.bar = 43").
      withGraphNodes(40).
      withLabel('A -> 30).
      withIndexSelectivity(('A, 'prop) -> .3).
      withKnownProperty('bar).
      shouldHaveCardinality(40.0 * (30.0 / 40) * (1 - (1 - DEFAULT_EQUALITY_SELECTIVITY) * (1 - .3)))
  }

  ignore("cardinality for property equality predicate when property name is unknown") { // We can get away with not doing this
    givenPattern("MATCH (a) WHERE a.prop = 42").
      withGraphNodes(40).
      shouldHaveCardinality(0)
  }

  test("cardinality for hardcoded false") {
    givenPattern("MATCH (a) WHERE false").
      withGraphNodes(40).
      shouldHaveCardinality(0)
  }

  test("cardinality for multiple AND:ed equality predicates on two indexes") {
    givenPattern("MATCH (a:A) WHERE a.prop = 42 AND a.bar = 43").
      withGraphNodes(40).
      withLabel('A -> 30).
      withIndexSelectivity(('A, 'prop) -> .03).
      withIndexSelectivity(('A, 'bar) -> .04).
      shouldHaveCardinality(30 * .03 * .04)
  }

  test("cardinality for multiple AND:ed equality predicates where one is backed by index and one is not") {
    givenPattern("MATCH (a:A) WHERE a.prop = 42 AND a.bar = 43").
      withGraphNodes(40).
      withLabel('A -> 30).
      withIndexSelectivity(('A, 'prop) -> .03).
      withKnownProperty('bar).
      shouldHaveCardinality(30 * .03 * DEFAULT_EQUALITY_SELECTIVITY)
  }

  test("cardinality for label and property equality when no index is present") {
    givenPattern("MATCH (a:A) WHERE a.prop = 42").
      withGraphNodes(40).
      withLabel('A -> 30).
      withKnownProperty('prop).
      shouldHaveCardinality(40.0 * (30.0 / 40) * DEFAULT_EQUALITY_SELECTIVITY)
  }

  test("relationship cardinality given no labels or types") {
    givenPattern("MATCH (a)-->(b)").
      withGraphNodes(40).
      withLabel('A -> 30).
      withLabel('B -> 20).
      withRelationshipCardinality('A -> 'TYPE -> 'B -> 10).
      withRelationshipCardinality('B -> 'TYPE -> 'A -> 10).
      shouldHaveCardinality(40 * 40 * (20.0 / (40.0 * 40)))
  }

  test("relationship cardinality given labels on both sides") {
    givenPattern("MATCH (a:A)-[r:TYPE]->(b:B)").
      withGraphNodes(40).
      withLabel('A -> 30).
      withLabel('B -> 20).
      withRelationshipCardinality('A -> 'TYPE -> 'B -> 50).
      shouldHaveCardinality(50)
  }

  test("relationship cardinality given labels on both sides for incoming pattern") {
    givenPattern("MATCH (b:B)<-[r:TYPE]-(a:A)").
      withGraphNodes(40).
      withLabel('A -> 30).
      withLabel('B -> 20).
      withRelationshipCardinality('A -> 'TYPE -> 'B -> 50).
      shouldHaveCardinality(50)
  }

  test("relationship cardinality given labels on both sides bidirectional") {

    val patternNodeCrossProduct = 40.0 * 40.0
    val labelSelectivity = (30.0 / 40) * (20.0 / 40)
    val maxRelCount = patternNodeCrossProduct * labelSelectivity
    val relSelectivity = (10.0 + 20.0) / maxRelCount - (10.0 / maxRelCount) * (20.0 / maxRelCount)

    givenPattern("MATCH (a:A)-[r:TYPE]-(b:B)").
      withGraphNodes(40).
      withLabel('A -> 30).
      withLabel('B -> 20).
      withRelationshipCardinality('A -> 'TYPE -> 'B -> 10).
      withRelationshipCardinality('B -> 'TYPE -> 'A -> 20).
      shouldHaveCardinality(patternNodeCrossProduct * labelSelectivity * relSelectivity)
  }

  test("relationship cardinality given a label on one side") {
    val N = 10000.0
    val labelSelectivity = 30.0 / 10000.0
    val maxRelCount = N * N * labelSelectivity
    val relSelectivity = (50.0 + 50) / maxRelCount

    givenPattern("MATCH (a:A)-[r:TYPE]->(b)").
      withGraphNodes(10000).
      withLabel('A -> 30).
      withLabel('B -> 20).
      withLabel('C -> 40).
      withRelationshipCardinality('A -> 'TYPE -> 'B -> 50).
      withRelationshipCardinality('A -> 'TYPE -> 'C -> 50).
      shouldHaveCardinality(N * N * labelSelectivity * relSelectivity)
  }

  test("relationship cardinality given a label on one side bidirectional") {
    givenPattern("MATCH (a:A)-[r:TYPE]-(b)").
      withGraphNodes(10000).
      withLabel('A -> 300).
      withLabel('B -> 200).
      withRelationshipCardinality('A -> 'TYPE -> 'B -> 80).
      withRelationshipCardinality('B -> 'TYPE -> 'A -> 50).
      shouldHaveCardinality(50 + 80) //Assume independence
  }

  test("cardinality for rel-patterns with multiple labels on one end") {
    val patternNodeCrossProduct = 40.0 * 40.0
    val labelSelectivity = (30.0 / 40) * (20.0 / 40)
    val maxRelCount = patternNodeCrossProduct * labelSelectivity
    val relSelectivity = (50.0 / maxRelCount) * (30 / maxRelCount)

    givenPattern("MATCH (a:A:B)-->()").
      withGraphNodes(40).
      withLabel('A -> 30).
      withLabel('B -> 20).
      withRelationshipCardinality('A -> 'TYPE -> 'B -> 50).
      withRelationshipCardinality('B -> 'TYPE -> 'B -> 30).
      shouldHaveCardinality(patternNodeCrossProduct * labelSelectivity * relSelectivity)
  }

  test("cardinality for rel-patterns with multiple rel types") {
    val patternNodeCrossProduct = 40.0 * 40.0
    val labelSelectivity = (30.0 / 40) * (20.0 / 40)
    val maxRelCount = patternNodeCrossProduct * labelSelectivity
    val relSelectivity = (10.0 + 30) / maxRelCount - (10.0 / maxRelCount) * (30.0 / maxRelCount)

    givenPattern("MATCH (a:A)-[:T1|:T2]->(:B)").
      withGraphNodes(40).
      withLabel('A -> 30).
      withLabel('B -> 20).
      withRelationshipCardinality('A -> 'T1 -> 'B -> 10).
      withRelationshipCardinality('A -> 'T2 -> 'B -> 30).
      shouldHaveCardinality(patternNodeCrossProduct * labelSelectivity * relSelectivity)
  }

  test("cardinality for rel-patterns with multiple rel types and multiple labels on one side - test 1") {
    val patternNodeCrossProduct = 40.0 * 40.0
    val labelSelectivity = (30.0 / 40) * (20.0 / 40)
    val maxRelCount = patternNodeCrossProduct * labelSelectivity
    val relSelectivityT1 = (10.0 / maxRelCount) * 0
    val relSelectivityT2 = (15.0 / maxRelCount) * (30.0 / maxRelCount)
    val relSelectivity = 1 - (1 - relSelectivityT1) * (1 - relSelectivityT2) // OR the two together

    givenPattern("MATCH (a:A:B)-[:T1|:T2]->()").
      withGraphNodes(40).
      withLabel('A -> 30).
      withLabel('B -> 20).
      withRelationshipCardinality('A -> 'T1 -> 'B -> 10).
      withRelationshipCardinality('B -> 'T2 -> 'B -> 30).
      withRelationshipCardinality('A -> 'T2 -> 'B -> 15).
      shouldHaveCardinality(patternNodeCrossProduct * labelSelectivity * relSelectivity)
  }

  test("cardinality for rel-patterns with multiple rel types and multiple labels on one side - test 2") {
    val patternNodeCrossProduct = 40.0 * 40.0
    val labelSelectivity = (30.0 / 40) * (20.0 / 40)

    val maxRelCount = patternNodeCrossProduct * labelSelectivity
    val relSelectivityT1 = (10.0 / maxRelCount) * (3.0 / maxRelCount)
    val relSelectivityT2 = (30.0 / maxRelCount) * (15.0 / maxRelCount)
    val relSelectivity = 1 - (1 - relSelectivityT1) * (1 - relSelectivityT2) // OR the two together


    givenPattern("MATCH (a:A:B)-[:T1|:T2]->()").
      withGraphNodes(40).
      withLabel('A -> 30).
      withLabel('B -> 20).
      withRelationshipCardinality('A -> 'T1 -> 'B -> 10).
      withRelationshipCardinality('B -> 'T1 -> 'B -> 3).
      withRelationshipCardinality('A -> 'T2 -> 'B -> 30).
      withRelationshipCardinality('B -> 'T2 -> 'B -> 15).
      shouldHaveCardinality(patternNodeCrossProduct * labelSelectivity * relSelectivity)
  }

  test("cardinality for rel-patterns with multiple rel types and multiple labels on both sides") {
    val patternNodeCrossProduct = 40.0 * 40.0
    val labelSelectivity = (30.0 / 40) * (20.0 / 40)
    val maxRelCount = patternNodeCrossProduct * labelSelectivity
    val relSelectivityT1 = (1.0 / maxRelCount) * (2.0 / maxRelCount) * (5.0 / maxRelCount) * (6.0 / maxRelCount)
    val relSelectivityT2 = (3.0 / maxRelCount) * (4.0 / maxRelCount) * (7.0 / maxRelCount) * (8.0 / maxRelCount)
    val relSelectivity = 1 - (1 - relSelectivityT1) * (1 - relSelectivityT2) // OR the two together


    givenPattern("MATCH (a:A:B)-[:T1|:T2]->(c:C:D)").
      withGraphNodes(40).
      withLabel('A -> 30).
      withLabel('B -> 20).
      withRelationshipCardinality('A -> 'T1 -> 'C -> 1).
      withRelationshipCardinality('B -> 'T1 -> 'C -> 2).
      withRelationshipCardinality('A -> 'T2 -> 'C -> 3).
      withRelationshipCardinality('B -> 'T2 -> 'C -> 4).
      withRelationshipCardinality('A -> 'T1 -> 'D -> 5).
      withRelationshipCardinality('B -> 'T1 -> 'D -> 6).
      withRelationshipCardinality('A -> 'T2 -> 'D -> 7).
      withRelationshipCardinality('B -> 'T2 -> 'D -> 8).
      shouldHaveCardinality(patternNodeCrossProduct * labelSelectivity * relSelectivity)
  }

  test("given empty database, all predicates should return 0 cardinality") {
    givenPattern("MATCH a WHERE a.prop = 10").
      withGraphNodes(0).
      withKnownProperty('prop).
      shouldHaveCardinality(0)
  }

  test("optional match from an unknown known label") {
    givenPattern("MATCH (a) OPTIONAL MATCH (a)-[:TYPE]->(:BAR)").
      withGraphNodes(10000).
      withLabel('FOO -> 1).
      withLabel('BAR -> 1000).
      withRelationshipCardinality('FOO -> 'TYPE -> 'BAR -> 1000).
      withRelationshipCardinality('BAZ -> 'TYPE -> 'BAR -> 300).
      shouldHaveCardinality(10000)
  }

  test("optional match from a known label") {
    givenPattern("MATCH (a:FOO) OPTIONAL MATCH (a)-[:TYPE]->(:BAR)").
      withGraphNodes(10000).
      withLabel('FOO -> 1).
      withLabel('BAR -> 1000).
      withRelationshipCardinality('FOO -> 'TYPE -> 'BAR -> 100).
      shouldHaveCardinality(100)
  }

  test("predicates in optional match do not decrease the cardinality matches") {
    givenPattern("MATCH (a:FOO) OPTIONAL MATCH (a)-[:TYPE]->(:BAR)").
      withGraphNodes(1000).
      withLabel('FOO -> 500).
      withLabel('BAR -> 0).
      withRelationshipCardinality('FOO -> 'TYPE -> 'BAR -> 0).
      shouldHaveCardinality(500)
  }

  test("honours bound arguments") {
    givenPattern("MATCH (a:FOO)-[:TYPE]->(b:BAR)").
      withQueryGraphArgumentIds(IdName("a")).
      withGraphNodes(500).
      withLabel('FOO -> 100).
      withLabel('BAR -> 400).
      withRelationshipCardinality('FOO -> 'TYPE -> 'BAR -> 1000).
      shouldHaveCardinality( 1000 / 500)
  }

  test("optional match will in worst case be a cartesian product") {
    givenPattern("MATCH (a) OPTIONAL MATCH (b)").
      withGraphNodes(1000).
      shouldHaveCardinality(1000 * 1000)
  }

  test("multiple optional matches - multiple cross joins") {
    givenPattern("MATCH (a:FOO) OPTIONAL MATCH (b:BAR) OPTIONAL MATCH (a) WHERE a.prop = 42").
      withGraphNodes(10000).
      withLabel('FOO -> 100).
      withLabel('BAR -> 200).
      withRelationshipCardinality('FOO -> 'TYPE -> 'BAR -> 0).
      withRelationshipCardinality('FOO -> 'TYPE -> 'BAZ -> 1).
      withIndexSelectivity(('FOO, 'prop) -> .3).
      shouldHaveCardinality(100 * 200)
  }

  test("node by id should be recognized as such") {
    givenPattern("MATCH (a:FOO) WHERE id(a) IN [1,2,3]").
      withGraphNodes(1000).
      withLabel('FOO -> 500).
      shouldHaveCardinality(1000.0 * (500.0 / 1000) * (3.0 / 1000))
  }

  test("two relationships with property") {
    val patternNodeCrossProduct = 6200.0 * 6200.0 * 6200.0
    val createdSelectivity = 1000.0 / (6200.0 * 6200.0)
    val appearsOnSelectivity = 5000.0 / (6200.0 * 6200.0)
    val indexSelectivity = 0.005

    givenPattern("MATCH (t:Track)--(al)--(a:Artist) WHERE t.duration = 61").
      withGraphNodes(6200).
      withLabel('Track -> 5000).
      withLabel('Artist -> 200).
      withRelationshipCardinality('Track -> 'APPEARS_ON -> 'Album -> 5000).
      withRelationshipCardinality('Artist -> 'CREATED -> 'Album -> 1000).
      withIndexSelectivity(('Track, 'duration) -> .005).
      shouldHaveCardinality(patternNodeCrossProduct * createdSelectivity * appearsOnSelectivity * indexSelectivity)
  }

  def createCardinalityModel(stats: GraphStatistics, semanticTable: SemanticTable): QueryGraphCardinalityModel =
    AssumeIndependenceQueryGraphCardinalityModel(stats, semanticTable, IndependenceCombiner)
}
