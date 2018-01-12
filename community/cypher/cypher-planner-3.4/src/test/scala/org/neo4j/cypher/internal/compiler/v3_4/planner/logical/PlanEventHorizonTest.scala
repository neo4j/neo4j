/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical

import org.neo4j.cypher.internal.compiler.v3_4.planner.{LogicalPlanningTestSupport2, ProcedureCallProjection}
import org.neo4j.cypher.internal.ir.v3_4.{PlannerQuery, RegularPlannerQuery, RegularQueryProjection}
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.v3_4.logical.plans._

class PlanEventHorizonTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should do projection if necessary") {
    // Given
    new given().withLogicalPlanningContext { (cfg, context, solveds, cardinalities) =>
      val literal = SignedDecimalIntegerLiteral("42")(pos)
      val pq = RegularPlannerQuery(horizon = RegularQueryProjection(Map("a" -> literal)))
      val inputPlan = Argument()
      solveds.set(inputPlan.id, PlannerQuery.empty)
      cardinalities.set(inputPlan.id, 0.0)

      // When
      val producedPlan = PlanEventHorizon(pq, inputPlan, context, solveds, cardinalities)

      // Then
      producedPlan should equal(Projection(inputPlan, Map("a" -> literal)))
    }
  }

  test("should plan procedure calls") {
    // Given
    new given().withLogicalPlanningContext { (cfg, context, solveds, cardinalities) =>
      val literal = SignedDecimalIntegerLiteral("42")(pos)
      val call = mock[ResolvedCall]
      val pq = RegularPlannerQuery(horizon = ProcedureCallProjection(call))
      val inputPlan = Argument()
      solveds.set(inputPlan.id, PlannerQuery.empty)
      cardinalities.set(inputPlan.id, 0.0)

      // When
      val producedPlan = PlanEventHorizon(pq, inputPlan, context, solveds, cardinalities)

      // Then
      producedPlan should equal(ProcedureCall(inputPlan, call))
    }
  }
}
