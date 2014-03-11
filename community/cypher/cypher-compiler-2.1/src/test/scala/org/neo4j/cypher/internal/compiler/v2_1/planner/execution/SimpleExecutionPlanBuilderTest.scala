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

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.DummyPosition
import org.neo4j.cypher.internal.compiler.v2_1.commands.{expressions => legacy}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_1.pipes.ProjectionNewPipe
import org.neo4j.cypher.internal.compiler.v2_1.pipes.NodeByIdSeekPipe
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.SingleRow
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.IdName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.Projection
import org.neo4j.cypher.internal.compiler.v2_1.pipes.NodeByLabelScanPipe
import org.neo4j.cypher.internal.compiler.v2_1.pipes.NullPipe
import org.neo4j.cypher.internal.compiler.v2_1.pipes.AllNodesScanPipe
import org.neo4j.cypher.internal.compiler.v2_1.LabelId
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.NodeByLabelScan
import org.neo4j.cypher.internal.compiler.v2_1.ast.SignedIntegerLiteral
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.AllNodesScan
import org.neo4j.cypher.internal.compiler.v2_1.ast.convert.ExpressionConverters._

class SimpleExecutionPlanBuilderTest extends CypherFunSuite {

  val planner = new SimpleExecutionPlanBuilder
  val pos = DummyPosition(0)

  test("projection only query") {
    val logicalPlan = Projection(SingleRow(), Map("42" -> SignedIntegerLiteral("42")(pos)))
    val pipeInfo = planner.build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(ProjectionNewPipe(NullPipe(), Map("42" -> legacy.Literal(42))))
  }

  test("simple pattern query") {
    val logicalPlan = AllNodesScan(IdName("n"), 1000)
    val pipeInfo = planner.build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(AllNodesScanPipe("n"))
  }

  test("simple label scan query") {
    val logicalPlan = NodeByLabelScan(IdName("n"), Right(LabelId(12)), 1000)
    val pipeInfo = planner.build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(NodeByLabelScanPipe("n", Right(LabelId(12))))
  }


  test("simple node by id scan query") {
    val astLiteral = SignedIntegerLiteral("42")(pos)
    val logicalPlan = NodeByIdScan(IdName("n"), astLiteral, 1)
    val pipeInfo = planner.build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(NodeByIdSeekPipe("n", astLiteral.asCommandExpression))
  }
}
