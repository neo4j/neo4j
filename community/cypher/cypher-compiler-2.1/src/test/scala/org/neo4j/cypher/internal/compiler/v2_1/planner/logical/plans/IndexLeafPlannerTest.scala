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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.mockito.Mockito._
import org.mockito.stubbing.Answer
import org.neo4j.kernel.api.index.IndexDescriptor
import org.mockito.invocation.InvocationOnMock
import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{uniqueIndexSeekLeafPlanner, indexSeekLeafPlanner}
import org.mockito.Matchers._

class IndexLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val statistics = newMockedStatistics

  test("index scan when there is an index on the property") {
    // given
    val identifier = Identifier("n")_
    val projections: Map[String, Expression]  = Map("n" -> identifier)
    val labelId = LabelId(12)
    val propertyKeyId = PropertyKeyId(15)
    val idName = IdName("n")
    val hasLabels = HasLabels(identifier, Seq(LabelName("Awesome")(Some(labelId))_))_
    val equals = Equals(
      Property(identifier, PropertyKeyName("prop")(Some(propertyKeyId))_)_,
      SignedIntegerLiteral("42")_
    )_
    val expressions: Seq[Expression] = Seq(equals, hasLabels)
    val qg = MainQueryGraph(projections, Selections(Seq(Set(idName) -> equals, Set(idName) -> hasLabels)), Set(idName), Set.empty, Seq.empty)

    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: AllNodesScan    => 1000
      case _: NodeByLabelScan => 100
      case _                  => Double.MaxValue
    })
    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = qg,
      metrics = factory.newMetrics(statistics))
    when(context.semanticTable.isNode(identifier)).thenReturn(true)
    when(context.planContext.indexesGetForLabel(12)).thenAnswer(new Answer[Iterator[IndexDescriptor]] {
      override def answer(invocation: InvocationOnMock) = Iterator(new IndexDescriptor(12, 15))
    })
    when(context.planContext.uniqueIndexesGetForLabel(12)).thenReturn(Iterator())

    // when
    val resultPlans = indexSeekLeafPlanner(expressions, Map(idName -> Set(hasLabels)))()

    // then
    resultPlans should equal(Seq(NodeIndexSeek(idName, labelId, propertyKeyId, SignedIntegerLiteral("42")_)()))
  }

  test("index seek when there is an index on the property") {
    // given
    val identifier = Identifier("n")_
    val projections: Map[String, Expression] = Map("n" -> identifier)
    val labelId = LabelId(12)
    val propertyKeyId = PropertyKeyId(15)
    val idName = IdName("n")
    val hasLabels = HasLabels(identifier, Seq(LabelName("Awesome")(Some(labelId))_))_
    val equals = Equals(
      Property(identifier, PropertyKeyName("prop")(Some(propertyKeyId))_)_,
      SignedIntegerLiteral("42")_
    )_
    val expressions: Seq[Expression] = Seq(equals, hasLabels)
    val qg = MainQueryGraph(projections, Selections(Seq(Set(idName) -> equals, Set(idName) -> hasLabels)), Set(idName), Set.empty, Seq.empty)

    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: AllNodesScan    => 1000
      case _: NodeByLabelScan => 100
      case _                  => Double.MaxValue
    })
    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = qg,
      metrics = factory.newMetrics(statistics))
    when(context.semanticTable.isNode(identifier)).thenReturn(true)
    when(context.planContext.indexesGetForLabel(12)).thenReturn(Iterator())
    when(context.planContext.uniqueIndexesGetForLabel(12)).thenAnswer(new Answer[Iterator[IndexDescriptor]] {
      override def answer(invocation: InvocationOnMock) = Iterator(new IndexDescriptor(12, 15))
    })

    // when
    val resultPlans = uniqueIndexSeekLeafPlanner(expressions, Map(idName -> Set(hasLabels)))()

    // then
    resultPlans should equal(Seq(NodeIndexUniqueSeek(idName, labelId, propertyKeyId, SignedIntegerLiteral("42")_)()))
  }
}
