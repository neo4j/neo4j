/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_5.planner._
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.PlanMatchHelp
import org.neo4j.cypher.internal.ir.v3_5.{DistinctQueryProjection, RequiredOrder}
import org.neo4j.cypher.internal.v3_5.logical.plans.Distinct
import org.opencypher.v9_0.ast.ASTAnnotationMap
import org.opencypher.v9_0.ast.semantics.{ExpressionTypeInfo, SemanticTable}
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

class DistinctTest extends CypherFunSuite with LogicalPlanningTestSupport with PlanMatchHelp {

  test("adds renaming distinct when variable available from index") {
    val prop = Property(Variable("x")(pos), PropertyKeyName("prop")(pos))(pos)
    val projection = DistinctQueryProjection(Map("x.prop" -> prop))

    val context = newMockedLogicalPlanningContextWithFakeAttributes(
      planContext = newMockedPlanContext,
      semanticTable = new SemanticTable(types = mock[ASTAnnotationMap[Expression, ExpressionTypeInfo]])
    )

    val startPlan = newMockedLogicalPlan(idNames = Set("x", "x.prop"), availablePropertiesFromIndexes = Map(prop -> "x.prop"))

    // When
    val (result, _) = distinct(startPlan, projection, RequiredOrder.empty, context)
    // Then
    result should equal(
      Distinct(startPlan, Map(cachedNodePropertyProj("x", "prop")))
    )
  }
}
