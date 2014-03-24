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
package org.neo4j.cypher.internal.compiler.v2_1.planner.execution

import org.mockito.Mockito
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.commands.{expressions => legacy}
import org.neo4j.cypher.internal.compiler.v2_1.ast.convert.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v2_1.pipes._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.ast.Collection
import org.neo4j.cypher.internal.compiler.v2_1.ast.SignedIntegerLiteral

class PipeExecutionPlanBuilderTest extends CypherFunSuite with LogicalPlanningTestSupport {

  implicit val monitor = mock[PipeMonitor]
  implicit val context = newMockedLogicalPlanContext

  val monitors = mock[Monitors]
  val planner = new PipeExecutionPlanBuilder(monitors)
  val pos = DummyPosition(0)

  Mockito.when(monitors.newMonitor[PipeMonitor]()).thenReturn(monitor)

  test("projection only query") {
    val logicalPlan = Projection(SingleRow(), Map("42" -> SignedIntegerLiteral("42")(pos)))
    val pipeInfo = planner.build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(ProjectionNewPipe(NullPipe(), Map("42" -> legacy.Literal(42))))
  }

  test("simple pattern query") {
    val logicalPlan = AllNodesScan(IdName("n"))
    val pipeInfo = planner.build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(AllNodesScanPipe("n"))
  }

  test("simple label scan query") {
    val logicalPlan = NodeByLabelScan(IdName("n"), Right(LabelId(12)))(Seq.empty)
    val pipeInfo = planner.build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(NodeByLabelScanPipe("n", Right(LabelId(12))))
  }

  test("simple node by id seek query") {
    val astLiteral = SignedIntegerLiteral("42")(pos)
    val logicalPlan = NodeByIdSeek(IdName("n"), astLiteral, 1)(Seq.empty)
    val pipeInfo = planner.build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(NodeByIdSeekPipe("n", astLiteral.asCommandExpression))
  }

  test("simple node by id seek query with multiple values") {
    val astCollection = Collection(
      Seq(SignedIntegerLiteral("42")(pos), SignedIntegerLiteral("43")(pos), SignedIntegerLiteral("43")(pos))
    )(pos)
    val logicalPlan = NodeByIdSeek(IdName("n"), astCollection, 3)(Seq.empty)
    val pipeInfo = planner.build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(NodeByIdSeekPipe("n", astCollection.asCommandExpression))
  }

  // 2014-03-19 - Andres: turn on once we have patterns in the query graph
  ignore("simple relationship by id seek query") {
    val astLiteral = SignedIntegerLiteral("42")(pos)
    val logicalPlan = RelationshipByIdSeek(IdName("r"), astLiteral, 1)(Seq.empty)
    val pipeInfo = planner.build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(RelationshipByIdSeekPipe("r", astLiteral.asCommandExpression))
  }

  // 2014-03-20 - Davide: turn on once we have patterns in the query graph
  ignore("simple relationship by id seek query with multiple values") {
    val astCollection = Collection(
      Seq(SignedIntegerLiteral("42")(pos), SignedIntegerLiteral("43")(pos), SignedIntegerLiteral("43")(pos))
    )(pos)
    val logicalPlan = RelationshipByIdSeek(IdName("r"), astCollection, 3)(Seq.empty)
    val pipeInfo = planner.build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(RelationshipByIdSeekPipe("r", astCollection.asCommandExpression))
  }

  test("simple cartesian product") {
    val lhs = AllNodesScan(IdName("n"))
    val rhs = AllNodesScan(IdName("m"))
    val logicalPlan = CartesianProduct(lhs, rhs)
    val pipeInfo = planner.build(logicalPlan)

    pipeInfo.pipe should equal(CartesianProductPipe(AllNodesScanPipe("n"), AllNodesScanPipe("m")))
  }
}
