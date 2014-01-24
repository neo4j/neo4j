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
package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.scalatest.Assertions
import org.junit.Test
import org.scalatest.mock.MockitoSugar
import org.neo4j.cypher.internal.compiler.v2_1.spi.PlanContext
import org.mockito.Mockito._
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.planner.Id
import org.neo4j.cypher.internal.compiler.v2_1.planner.Cost
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_1.planner.GraphRelationship
import org.neo4j.cypher.internal.compiler.v2_1.planner.LabelScan
import org.neo4j.cypher.internal.compiler.v2_1.planner.AllNodesScan
import org.neo4j.cypher.internal.compiler.v2_1.planner.NodeLabelSelection
import org.neo4j.cypher.internal.compiler.v2_1.planner.ExpandRelationships


class PlanGeneratorTest extends Assertions with MockitoSugar {

  val planContext = mock[PlanContext]
  val costEstimator = mock[CostEstimator]
  val generator = new PlanGenerator(costEstimator)
  
  @Test def simplePattern() {
    // MATCH (a) RETURN a
    // GIVEN
    val queryGraph = QueryGraph(Id(0), Seq.empty, Seq.empty, Seq.empty)
    when(costEstimator.costForAllNodes()).thenReturn(Cost(1000, 1))

    // WHEN
    val result = generator.generatePlan(planContext, queryGraph)

    // THEN
    assert(AllNodesScan(Id(0), Cost(1000, 1)) === result)
  }

  @Test def simpleLabeledPattern() {
    // MATCH (a:Person) RETURN a
    // Given
    val personLabelId: Int = 1337
    when(planContext.labelGetId("Person")).thenReturn(Token(personLabelId))
    val queryGraph = QueryGraph(Id(0), Seq.empty, Seq(Id(0) -> NodeLabelSelection(Label("Person"))), Seq.empty)
    when(costEstimator.costForScan(Token(personLabelId))).thenReturn(Cost(100, 1))

    // When
    val result = generator.generatePlan(planContext, queryGraph)

    // Then
    assert(LabelScan(Id(0), Token(personLabelId), Cost(100, 1)) === result)
  }

  @Test def simpleRelationshipPatternWithCheaperLabelScan() {
    // MATCH (a:Person) -[:KNOWS]-> (b) RETURN a
    // Given

    val personLabelId: Int = 1337
    val knowsTypeId: Int = 12

    when(planContext.labelGetId("Person")).thenReturn(Token(personLabelId))
    when(planContext.relationshipTypeGetId("KNOWS")).thenReturn(Token(knowsTypeId))
    val queryGraph = QueryGraph(Id(1), Seq(
      GraphRelationship(Id(0), Id(1), Direction.OUTGOING, Seq(RelationshipType("KNOWS")))
    ), Seq(Id(0) -> NodeLabelSelection(Label("Person"))), Seq.empty)
    when(costEstimator.costForAllNodes()).thenReturn(Cost(1000, 1))
    when(costEstimator.costForScan(Token(personLabelId))).thenReturn(Cost(100, 1))
    when(costEstimator.costForExpandRelationship(Seq.empty, Seq(Token(knowsTypeId)), Direction.OUTGOING)).thenReturn(Cost(500, 1))

    // When
    val result = generator.generatePlan(planContext, queryGraph)

    // Then
    assert(ExpandRelationships(LabelScan(Id(0), Token(personLabelId), Cost(100, 1)), Direction.OUTGOING, Cost(500, 1)) === result)
  }

  @Test def longer_path_with_values_pushing_a_hash_join() {
    // MATCH (a:Person) -[:KNOWS]-> (b) <-[:KNOWS]- (c) RETURN c
    // Given

    val personLabelId: Int = 1337
    val knowsTypeId: Int = 12

    when(planContext.labelGetId("Person")).thenReturn(Token(personLabelId))
    when(planContext.relationshipTypeGetId("KNOWS")).thenReturn(Token(knowsTypeId))
    val queryGraph = QueryGraph(Id(1), Seq(
      GraphRelationship(Id(0), Id(1), Direction.OUTGOING, Seq(RelationshipType("KNOWS"))),
      GraphRelationship(Id(0), Id(1), Direction.INCOMING, Seq(RelationshipType("KNOWS")))
    ), Seq(Id(0) -> NodeLabelSelection(Label("Person"))), Seq.empty)
    when(costEstimator.costForAllNodes()).thenReturn(Cost(1000, 1))
    when(costEstimator.costForScan(Token(personLabelId))).thenReturn(Cost(100, 1))
    when(costEstimator.costForExpandRelationship(Seq.empty, Seq(Token(knowsTypeId)), Direction.OUTGOING)).thenReturn(Cost(500, 1))

    // When
    val result = generator.generatePlan(planContext, queryGraph)

    // Then
    assert(ExpandRelationships(LabelScan(Id(0), Token(personLabelId), Cost(100, 1)), Direction.OUTGOING, Cost(500, 1)) === result)
  }
}
