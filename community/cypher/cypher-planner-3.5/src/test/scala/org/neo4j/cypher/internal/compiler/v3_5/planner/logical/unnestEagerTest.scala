/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical

import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.Eagerness.unnestEager
import org.neo4j.cypher.internal.ir.v3_5.CreateNode
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.cypher.internal.v3_5.expressions.PropertyKeyName
import org.neo4j.cypher.internal.v3_5.util.attribution.Attributes
import org.neo4j.cypher.internal.v3_5.util.helpers.fixedPoint
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class unnestEagerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("should unnest create from rhs of apply") {
    val lhs = newMockedLogicalPlan()
    val rhs = newMockedLogicalPlan()
    val create = Create(rhs, nodes = List(CreateNode("a", Seq.empty, None)), Nil)
    val input = Apply(lhs, create)

    rewrite(input) should equal(create.copy(source = Apply(lhs, rhs)))
  }

  test("should unnest delete relationship from rhs of apply") {
    val lhs = newMockedLogicalPlan()
    val rhs = newMockedLogicalPlan()
    val delete = DeleteRelationship(rhs, null)
    val input = Apply(lhs, delete)

    rewrite(input) should equal(DeleteRelationship(Apply(lhs, rhs), null))
  }

  test("should unnest delete node from rhs of apply") {
    val lhs = newMockedLogicalPlan()
    val rhs = newMockedLogicalPlan()
    val delete = DeleteNode(rhs, null)
    val input = Apply(lhs, delete)

    rewrite(input) should equal(DeleteNode(Apply(lhs, rhs), null))
  }

  test("should unnest detach delete node from rhs of apply") {
    val lhs = newMockedLogicalPlan()
    val rhs = newMockedLogicalPlan()
    val delete = DetachDeleteNode(rhs, null)
    val input = Apply(lhs, delete)

    rewrite(input) should equal(DetachDeleteNode(Apply(lhs, rhs), null))
  }

  test("should unnest set node property from rhs of apply") {
    val lhs = newMockedLogicalPlan()
    val rhs = newMockedLogicalPlan()
    val set = SetNodeProperty(rhs, "a", PropertyKeyName("prop")(pos), null)
    val input = Apply(lhs, set)

    rewrite(input) should equal(SetNodeProperty(Apply(lhs, rhs), "a", PropertyKeyName("prop")(pos), null))
  }

  test("should unnest set node property from map from rhs of apply") {
    val lhs = newMockedLogicalPlan()
    val rhs = newMockedLogicalPlan()
    val set = SetNodePropertiesFromMap(rhs, "a", null, removeOtherProps = false)
    val input = Apply(lhs, set)

    rewrite(input) should equal(SetNodePropertiesFromMap(Apply(lhs, rhs), "a", null, removeOtherProps = false))
  }

  test("should unnest set labels from rhs of apply") {
    val lhs = newMockedLogicalPlan()
    val rhs = newMockedLogicalPlan()
    val set = SetLabels(rhs, "a", Seq.empty)
    val input = Apply(lhs, set)

    rewrite(input) should equal(SetLabels(Apply(lhs, rhs), "a", Seq.empty))
  }

  test("should unnest remove labels from rhs of apply") {
    val lhs = newMockedLogicalPlan()
    val rhs = newMockedLogicalPlan()
    val remove = RemoveLabels(rhs, "a", Seq.empty)
    val input = Apply(lhs, remove)

    rewrite(input) should equal(RemoveLabels(Apply(lhs, rhs), "a", Seq.empty))
  }

  private def rewrite(p: LogicalPlan) =
    fixedPoint((p: LogicalPlan) => p.endoRewrite(unnestEager(new StubSolveds, new Attributes(idGen))))(p)
}
