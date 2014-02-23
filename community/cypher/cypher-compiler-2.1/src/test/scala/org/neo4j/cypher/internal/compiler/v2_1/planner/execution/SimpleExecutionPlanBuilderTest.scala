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
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{Id, AllNodesScan, SingleRow, Projection}
import org.neo4j.cypher.internal.compiler.v2_1.ast.{Identifier, SignedIntegerLiteral}
import org.neo4j.cypher.internal.compiler.v2_1.DummyPosition
import org.neo4j.cypher.internal.compiler.v2_1.pipes.{AllNodesScanPipe, NullPipe, ExtractPipe}
import org.neo4j.cypher.internal.compiler.v2_1.commands.{expressions => legacy}

class SimpleExecutionPlanBuilderTest extends CypherFunSuite {

  val planner = new SimpleExecutionPlanBuilder

  test("projection only query") {
    val logicalPlan = Projection(SingleRow(), Seq("42" -> SignedIntegerLiteral("42")(DummyPosition(0))))
    val pipeInfo = planner.build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(ExtractPipe(NullPipe(), Map("42" -> legacy.Literal(42))))
  }

  test("simple pattern query") {
    val logicalPlan = Projection(AllNodesScan(Id("n"), 1000), Seq("n" -> Identifier("n")(DummyPosition(0))))
    val pipeInfo = planner.build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(ExtractPipe(AllNodesScanPipe("n"), Map("n" -> legacy.Identifier("n"))))
  }
}