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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{IdName, PatternRelationship, SimplePatternLength}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{CardinalityTestHelper, QueryGraphProducer}
import org.neo4j.graphdb.Direction

class EstimateSelectivityTest extends CypherFunSuite with LogicalPlanningTestSupport with QueryGraphProducer with CardinalityTestHelper {

  val idA: Identifier = ident("a")
  val labelPredicate: Expression = HasLabels(idA, Seq(LabelName("BAR") _)) _
  val id1 = ident("a")
  val id2 = ident("b")
  val BAR: LabelName = LabelName("BAR") _
  val FOO: LabelName = LabelName("FOO") _
  val BAR2: LabelName = LabelName("BAR2") _

  val lit: Literal = SignedDecimalIntegerLiteral("1") _
  val property: Expression = Property(id1, PropertyKeyName("prop") _) _
  val inComparison: Expression = In(property, Collection(Seq(lit)) _) _
  val hasLabelId1: Expression = HasLabels(id1, Seq(BAR)) _
  val hasLabelId1Bis: Expression = HasLabels(id1, Seq(BAR2)) _
  val hasLabelId2: Expression = HasLabels(id2, Seq(FOO)) _
  val relType: RelTypeName = RelTypeName("TYPE") _
  val pattern = PatternRelationship(IdName("r1"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq(relType), SimplePatternLength)
  val reversePattern = PatternRelationship(IdName("r1"), (IdName("a"), IdName("b")), Direction.INCOMING, Seq(relType), SimplePatternLength)

  test("lonely hasLabel gets it selectivity from statistics") {
    givenPredicate("(a:BAR)").
      withGraphNodes(40).
      withLabel('BAR -> 20).
      shouldHaveSelectivity(.5)
  }

  test("hasLabel on unknown label gives selectivity 0") {
    givenPredicate("(a:BAR)").
      withGraphNodes(40).
      shouldHaveSelectivity(0)
  }

  test("relationship given labels, type and direction") {
    givenPredicate("(a:BAR)-[r:TYPE]->(b:FOO)").
      withGraphNodes(200).
      withLabel('BAR -> 50).
      withLabel('FOO -> 100).
      withRelationshipCardinality('BAR -> 'TYPE -> 'FOO -> 25).
      shouldHaveSelectivity(25.0 / (200 * 200))
  }

  test("relationship given type and directions, no labels") {
    givenPredicate("(a)-[r:TYPE]->(b)").
      withGraphNodes(100).
      withLabel('BAR -> 20).
      withLabel('FOO -> 30).
      withLabel('BAZ -> 1).
      withRelationshipCardinality('BAR -> 'TYPE -> 'FOO -> 25).
      withRelationshipCardinality('BAR -> 'TYPE -> 'BAZ -> 25).
      withRelationshipCardinality('BAR -> 'TYPE2 -> 'FOO -> 50).
      shouldHaveSelectivity((25.0 + 25) / (100 * 100))
  }

  test("relationship given unknown type and directions, no labels") { // should work
    givenPredicate("(a)-[r]->(b)").
      withGraphNodes(100).
      withLabel('BAR -> 20).
      withLabel('FOO -> 30).
      withLabel('BAZ -> 1).
      withRelationshipCardinality('BAR -> 'TYPE -> 'FOO -> 25).
      withRelationshipCardinality('BAR -> 'TYPE -> 'BAZ -> 25).
      withRelationshipCardinality('BAR -> 'TYPE2 -> 'FOO -> 50).
      shouldHaveSelectivity((25.0 + 25 + 50) / (100 * 100))
  }

  test("relationship given left label, type and direction") { // Should work
    givenPredicate("(a:FOO)-[r:TYPE]->(b)").
      withGraphNodes(200).
      withLabel('BAR -> 50).
      withLabel('FOO -> 100).
      withRelationshipCardinality('BAR -> 'TYPE -> 'FOO -> 30).
      withRelationshipCardinality('FOO -> 'TYPE -> 'FOO -> 25).
      shouldHaveSelectivity( 25.0 / (200 * 200) )
  }

  test("relationship given left label, type and incoming direction") { // Should work
    givenPredicate("(a:FOO)<-[r:TYPE]-(b)").
      withGraphNodes(200).
      withLabel('BAR -> 50).
      withLabel('FOO -> 100).
      withRelationshipCardinality('BAR -> 'TYPE -> 'FOO -> 25).
      withRelationshipCardinality('FOO -> 'TYPE -> 'FOO -> 30).
      shouldHaveSelectivity( (25.0 + 30) / (200 * 200) )
  }

  test("relationship given right label, type and direction") { // Should work
    givenPredicate("(a)-[r:TYPE]->(b:FOO)").
      withGraphNodes(200).
      withLabel('BAR -> 50).
      withLabel('FOO -> 100).
      withRelationshipCardinality('BAR -> 'TYPE -> 'FOO -> 25).
      withRelationshipCardinality('FOO -> 'TYPE -> 'FOO -> 30).
      shouldHaveSelectivity( (25.0 + 30) / (200 * 200) )
  }

  test("equality comparisons on node property") {
    givenPredicate("(a) WHERE a.prop = 42").
      withGraphNodes(200).
      shouldHaveSelectivity( .1 )
  }

  test("equality comparisons") {
    givenPredicate("(a) WHERE a = 42").
    withGraphNodes(200).
    shouldHaveSelectivity( .75 )
  }
}
