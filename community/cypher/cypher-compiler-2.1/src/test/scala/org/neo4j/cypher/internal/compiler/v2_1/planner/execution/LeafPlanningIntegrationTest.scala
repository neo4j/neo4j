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
import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.ast.SignedIntegerLiteral
import org.mockito.Mockito._
import org.mockito.Matchers._

class LeafPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val factory: SpyableMetricsFactory = newMockedMetricsFactory
  when(factory.newCardinalityEstimator(any(), any(), any())).thenReturn((plan: LogicalPlan) => plan match {
    case _: AllNodesScan => 1000
    case _: NodeByLabelScan => 100
    case _: NodeIndexSeek => 10
    case _: NodeByIdSeek => 1
    case _: Expand => 100.0
    case _ => Double.MaxValue
  })
  implicit val planner = newPlanner(factory)


  test("should use node by id when possible") {
    implicit val planContext = newMockedPlanContext

    produceLogicalPlan("MATCH n WHERE ID(n) = 0 RETURN n") should equal(
      NodeByIdSeek(IdName("n"), Seq(SignedIntegerLiteral("0") _))(Seq.empty)
    )

    produceLogicalPlan("MATCH n WHERE id(n) = 0 RETURN n") should equal(
      NodeByIdSeek(IdName("n"), Seq(SignedIntegerLiteral("0") _))(Seq.empty)
    )
  }
}
