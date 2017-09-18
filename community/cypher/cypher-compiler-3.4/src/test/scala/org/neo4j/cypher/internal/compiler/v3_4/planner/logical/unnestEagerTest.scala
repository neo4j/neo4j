/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.Eagerness.unnestEager
import org.neo4j.cypher.internal.frontend.v3_4.ast.{PropertyKeyName, RelTypeName}
import org.neo4j.cypher.internal.frontend.v3_4.helpers.fixedPoint
import org.neo4j.cypher.internal.ir.v3_4.IdName
import org.neo4j.cypher.internal.v3_4.logical.plans._

class unnestEagerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("should unnest create node from rhs of apply") {
    val lhs = newMockedLogicalPlan()
    val rhs = newMockedLogicalPlan()
    val create = CreateNode(rhs, IdName("a"), Seq.empty, None)(solved)
    val input = Apply(lhs, create)(solved)

    rewrite(input) should equal(CreateNode(Apply(lhs, rhs)(solved), IdName("a"), Seq.empty, None)(solved))
  }

  test("should unnest create relationship from rhs of apply") {
    val lhs = newMockedLogicalPlan()
    val rhs = newMockedLogicalPlan()
    val create = CreateRelationship(rhs, IdName("a"), IdName("b"), RelTypeName("R")(pos), IdName("c"), None)(solved)
    val input = Apply(lhs, create)(solved)

    rewrite(input) should equal(CreateRelationship(Apply(lhs, rhs)(solved), IdName("a"), IdName("b"), RelTypeName("R")(pos), IdName("c"), None)(solved))
  }

  test("should unnest delete relationship from rhs of apply") {
    val lhs = newMockedLogicalPlan()
    val rhs = newMockedLogicalPlan()
    val delete = DeleteRelationship(rhs, null)(solved)
    val input = Apply(lhs, delete)(solved)

    rewrite(input) should equal(DeleteRelationship(Apply(lhs, rhs)(solved), null)(solved))
  }

  test("should unnest delete node from rhs of apply") {
    val lhs = newMockedLogicalPlan()
    val rhs = newMockedLogicalPlan()
    val delete = DeleteNode(rhs, null)(solved)
    val input = Apply(lhs, delete)(solved)

    rewrite(input) should equal(DeleteNode(Apply(lhs, rhs)(solved), null)(solved))
  }

  test("should unnest detach delete node from rhs of apply") {
    val lhs = newMockedLogicalPlan()
    val rhs = newMockedLogicalPlan()
    val delete = DetachDeleteNode(rhs, null)(solved)
    val input = Apply(lhs, delete)(solved)

    rewrite(input) should equal(DetachDeleteNode(Apply(lhs, rhs)(solved), null)(solved))
  }

  test("should unnest set node property from rhs of apply") {
    val lhs = newMockedLogicalPlan()
    val rhs = newMockedLogicalPlan()
    val set = SetNodeProperty(rhs, IdName("a"), PropertyKeyName("prop")(pos), null)(solved)
    val input = Apply(lhs, set)(solved)

    rewrite(input) should equal(SetNodeProperty(Apply(lhs, rhs)(solved), IdName("a"), PropertyKeyName("prop")(pos), null)(solved))
  }

  test("should unnest set node property from map from rhs of apply") {
    val lhs = newMockedLogicalPlan()
    val rhs = newMockedLogicalPlan()
    val set = SetNodePropertiesFromMap(rhs, IdName("a"), null, removeOtherProps = false)(solved)
    val input = Apply(lhs, set)(solved)

    rewrite(input) should equal(SetNodePropertiesFromMap(Apply(lhs, rhs)(solved), IdName("a"), null, removeOtherProps = false)(solved))
  }

  test("should unnest set labels from rhs of apply") {
    val lhs = newMockedLogicalPlan()
    val rhs = newMockedLogicalPlan()
    val set = SetLabels(rhs, IdName("a"), Seq.empty)(solved)
    val input = Apply(lhs, set)(solved)

    rewrite(input) should equal(SetLabels(Apply(lhs, rhs)(solved), IdName("a"), Seq.empty)(solved))
  }

  test("should unnest remove labels from rhs of apply") {
    val lhs = newMockedLogicalPlan()
    val rhs = newMockedLogicalPlan()
    val remove = RemoveLabels(rhs, IdName("a"), Seq.empty)(solved)
    val input = Apply(lhs, remove)(solved)

    rewrite(input) should equal(RemoveLabels(Apply(lhs, rhs)(solved), IdName("a"), Seq.empty)(solved))
  }

  private def rewrite(p: LogicalPlan): LogicalPlan =
    fixedPoint((p: LogicalPlan) => p.endoRewrite(unnestEager))(p)
}
