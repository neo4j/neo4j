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

import org.mockito.Mockito._
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{IdName, PatternRelationship, SimplePatternLength}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{CardinalityTestHelper, Cardinality, QueryGraphProducer, Selectivity}
import org.neo4j.cypher.internal.compiler.v2_2.spi.{GraphStatistics, TokenContext}
import org.neo4j.cypher.internal.compiler.v2_2.{LabelId, RelTypeId}
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
    val tokens = mock[TokenContext]
    val stats = mock[GraphStatistics]

    val labelId = 1337
    when(tokens.getOptLabelId("BAR")).thenReturn(Some(labelId))
    when(stats.nodesWithLabelCardinality(None)).thenReturn(Cardinality(100))
    when(stats.nodesWithLabelCardinality(Some(LabelId(labelId)))).thenReturn(Cardinality(10))

    val estimator = estimateSelectivity(stats, tokens)

    estimator(SingleExpression(labelPredicate)) should equal(Selectivity(.1))
  }

  /*test("lonely hasLabel gets it selectivity from statistics") {
    givenPredicate("a:BAR").
      withAllNodes(40).
      withLabel('BAR -> 20).
      shouldHaveSelectivity(.5)



    val tokens = mock[TokenContext]
    val stats = mock[GraphStatistics]

    val labelId = 1337
    when(tokens.getOptLabelId("BAR")).thenReturn(Some(labelId))
    when(stats.nodesWithLabelCardinality(None)).thenReturn(Cardinality(100))
    when(stats.nodesWithLabelCardinality(Some(LabelId(labelId)))).thenReturn(Cardinality(10))

    val estimator = estimateSelectivity(stats, tokens)

    estimator(SingleExpression(labelPredicate)) should equal(Selectivity(.1))
  }*/

  test("hasLabel on unknown label gives selectivity 0") {
    val tokens = mock[TokenContext]
    val stats = mock[GraphStatistics]

    when(tokens.getOptLabelId("BAR")).thenReturn(None)
    when(stats.nodesWithLabelCardinality(None)).thenReturn(Cardinality(100))

    val estimator = estimateSelectivity(stats, tokens)

    estimator(SingleExpression(labelPredicate)) should equal(Selectivity(0))
  }

  test("relationship given labels, type and direction") {
    val tokens = mock[TokenContext]
    val stats = mock[GraphStatistics]
    val lhsId = LabelId(1)
    val rhsId = LabelId(2)

    when(stats.nodesWithLabelCardinality(None)).thenReturn(Cardinality(100))
    when(stats.nodesWithLabelCardinality(Some(lhsId))).thenReturn(Cardinality(100))
    when(stats.nodesWithLabelCardinality(Some(rhsId))).thenReturn(Cardinality(100))
    when(tokens.getOptLabelId("BAR")).thenReturn(Some(1))
    when(tokens.getOptLabelId("FOO")).thenReturn(Some(2))
    when(tokens.getOptRelTypeId("TYPE")).thenReturn(Some(3))
    when(stats.cardinalityByLabelsAndRelationshipType(Some(lhsId), Some(RelTypeId(3)), Some(rhsId))).thenReturn(Cardinality(25))
    when(stats.cardinalityByLabelsAndRelationshipType(None, None, None)).thenReturn(Cardinality(100))

    val estimator = estimateSelectivity(stats, tokens)

    estimator(RelationshipWithLabels(Some(BAR), pattern, Some(FOO), Set.empty)) should equal(Selectivity(.0025))
  }

  test("relationship given type and directions, no labels") {
    val tokens = mock[TokenContext]
    val stats = mock[GraphStatistics]

    when(tokens.getOptRelTypeId("TYPE")).thenReturn(Some(3))
    when(stats.nodesWithLabelCardinality(None)).thenReturn(Cardinality(100))
    when(stats.cardinalityByLabelsAndRelationshipType(None, Some(RelTypeId(3)), None)).thenReturn(Cardinality(50))
    when(stats.cardinalityByLabelsAndRelationshipType(None, None, None)).thenReturn(Cardinality(100))

    val estimator = estimateSelectivity(stats, tokens)

    estimator(RelationshipWithLabels(None, pattern, None, Set.empty)) should equal(Selectivity(.005))
  }

  test("relationship given unknown type and directions, no labels") {
    val tokens = mock[TokenContext]
    val stats = mock[GraphStatistics]

    when(tokens.getOptRelTypeId("TYPE")).thenReturn(None)
    when(stats.nodesWithLabelCardinality(None)).thenReturn(Cardinality(100))
    when(stats.cardinalityByLabelsAndRelationshipType(None, None, None)).thenReturn(Cardinality(100))

    val estimator = estimateSelectivity(stats, tokens)

    estimator(RelationshipWithLabels(None, pattern, None, Set.empty)) should equal(Selectivity(0))
  }

  test("relationship given left label, type and direction") {
    val tokens = mock[TokenContext]
    val stats = mock[GraphStatistics]
    val lhsId = LabelId(1)

    when(stats.nodesWithLabelCardinality(None)).thenReturn(Cardinality(100))
    when(stats.nodesWithLabelCardinality(Some(lhsId))).thenReturn(Cardinality(100))
    when(tokens.getOptLabelId("BAR")).thenReturn(Some(1))
    when(tokens.getOptRelTypeId("TYPE")).thenReturn(Some(3))
    when(stats.cardinalityByLabelsAndRelationshipType(Some(lhsId), Some(RelTypeId(3)), None)).thenReturn(Cardinality(30))
    when(stats.cardinalityByLabelsAndRelationshipType(None, None, None)).thenReturn(Cardinality(100))

    val estimator = estimateSelectivity(stats, tokens)

    estimator(RelationshipWithLabels(Some(BAR), pattern, None, Set.empty)) should equal(Selectivity(.003))
  }

  test("relationship given left label, type and incoming direction") {
    val tokens = mock[TokenContext]
    val stats = mock[GraphStatistics]
    val lhsId = LabelId(1)

    when(stats.nodesWithLabelCardinality(None)).thenReturn(Cardinality(100))
    when(stats.nodesWithLabelCardinality(Some(lhsId))).thenReturn(Cardinality(100))
    when(tokens.getOptLabelId("BAR")).thenReturn(Some(1))
    when(tokens.getOptRelTypeId("TYPE")).thenReturn(Some(3))
    when(stats.cardinalityByLabelsAndRelationshipType(None, Some(RelTypeId(3)), Some(lhsId))).thenReturn(Cardinality(30))
    when(stats.cardinalityByLabelsAndRelationshipType(None, None, None)).thenReturn(Cardinality(100))

    val estimator = estimateSelectivity(stats, tokens)

    estimator(RelationshipWithLabels(Some(BAR), reversePattern, None, Set.empty)) should equal(Selectivity(.003))
  }

  test("relationship given right label, type and direction") {
    val tokens = mock[TokenContext]
    val stats = mock[GraphStatistics]
    val lhsId = LabelId(2)

    when(stats.nodesWithLabelCardinality(None)).thenReturn(Cardinality(100))
    when(stats.nodesWithLabelCardinality(Some(lhsId))).thenReturn(Cardinality(100))
    when(tokens.getOptLabelId("FOO")).thenReturn(Some(2))
    when(tokens.getOptRelTypeId("TYPE")).thenReturn(Some(3))
    when(stats.cardinalityByLabelsAndRelationshipType(None, Some(RelTypeId(3)), Some(lhsId))).thenReturn(Cardinality(30))
    when(stats.cardinalityByLabelsAndRelationshipType(None, None, None)).thenReturn(Cardinality(100))

    val estimator = estimateSelectivity(stats, tokens)

    estimator(RelationshipWithLabels(None, pattern, Some(FOO), Set.empty)) should equal(Selectivity(.003))
  }
}
