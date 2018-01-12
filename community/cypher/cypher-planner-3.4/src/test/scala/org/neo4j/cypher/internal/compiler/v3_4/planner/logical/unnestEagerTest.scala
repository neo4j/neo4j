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

import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.Eagerness.unnestEager
import org.neo4j.cypher.internal.v3_4.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_4.helpers.fixedPoint
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_4.IdName
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.util.v3_4.attribution.Attributes
import org.neo4j.cypher.internal.v3_4.expressions.{PropertyKeyName, RelTypeName}

class unnestEagerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("should unnest create node from rhs of apply") {
    val solveds = new Solveds
    val lhs = newMockedLogicalPlan(solveds, new Cardinalities)
    val rhs = newMockedLogicalPlan(solveds, new Cardinalities)
    val create = CreateNode(rhs, IdName("a"), Seq.empty, None)
    val input = Apply(lhs, create)

    rewrite(input, solveds) should equal(CreateNode(Apply(lhs, rhs), IdName("a"), Seq.empty, None))
  }

  test("should unnest create relationship from rhs of apply") {
    val solveds = new Solveds
    val lhs = newMockedLogicalPlan(solveds, new Cardinalities)
    val rhs = newMockedLogicalPlan(solveds, new Cardinalities)
    val create = CreateRelationship(rhs, IdName("a"), IdName("b"), RelTypeName("R")(pos), IdName("c"), None)
    val input = Apply(lhs, create)

    rewrite(input, solveds) should equal(CreateRelationship(Apply(lhs, rhs), IdName("a"), IdName("b"), RelTypeName("R")(pos), IdName("c"), None))
  }

  test("should unnest delete relationship from rhs of apply") {
    val solveds = new Solveds
    val lhs = newMockedLogicalPlan(solveds, new Cardinalities)
    val rhs = newMockedLogicalPlan(solveds, new Cardinalities)
    val delete = DeleteRelationship(rhs, null)
    val input = Apply(lhs, delete)

    rewrite(input, solveds) should equal(DeleteRelationship(Apply(lhs, rhs), null))
  }

  test("should unnest delete node from rhs of apply") {
    val solveds = new Solveds
    val lhs = newMockedLogicalPlan(solveds, new Cardinalities)
    val rhs = newMockedLogicalPlan(solveds, new Cardinalities)
    val delete = DeleteNode(rhs, null)
    val input = Apply(lhs, delete)

    rewrite(input, solveds) should equal(DeleteNode(Apply(lhs, rhs), null))
  }

  test("should unnest detach delete node from rhs of apply") {
    val solveds = new Solveds
    val lhs = newMockedLogicalPlan(solveds, new Cardinalities)
    val rhs = newMockedLogicalPlan(solveds, new Cardinalities)
    val delete = DetachDeleteNode(rhs, null)
    val input = Apply(lhs, delete)

    rewrite(input, solveds) should equal(DetachDeleteNode(Apply(lhs, rhs), null))
  }

  test("should unnest set node property from rhs of apply") {
    val solveds = new Solveds
    val lhs = newMockedLogicalPlan(solveds, new Cardinalities)
    val rhs = newMockedLogicalPlan(solveds, new Cardinalities)
    val set = SetNodeProperty(rhs, IdName("a"), PropertyKeyName("prop")(pos), null)
    val input = Apply(lhs, set)

    rewrite(input, solveds) should equal(SetNodeProperty(Apply(lhs, rhs), IdName("a"), PropertyKeyName("prop")(pos), null))
  }

  test("should unnest set node property from map from rhs of apply") {
    val solveds = new Solveds
    val lhs = newMockedLogicalPlan(solveds, new Cardinalities)
    val rhs = newMockedLogicalPlan(solveds, new Cardinalities)
    val set = SetNodePropertiesFromMap(rhs, IdName("a"), null, removeOtherProps = false)
    val input = Apply(lhs, set)

    rewrite(input, solveds) should equal(SetNodePropertiesFromMap(Apply(lhs, rhs), IdName("a"), null, removeOtherProps = false))
  }

  test("should unnest set labels from rhs of apply") {
    val solveds = new Solveds
    val lhs = newMockedLogicalPlan(solveds, new Cardinalities)
    val rhs = newMockedLogicalPlan(solveds, new Cardinalities)
    val set = SetLabels(rhs, IdName("a"), Seq.empty)
    val input = Apply(lhs, set)

    rewrite(input, solveds) should equal(SetLabels(Apply(lhs, rhs), IdName("a"), Seq.empty))
  }

  test("should unnest remove labels from rhs of apply") {
    val solveds = new Solveds
    val lhs = newMockedLogicalPlan(solveds, new Cardinalities)
    val rhs = newMockedLogicalPlan(solveds, new Cardinalities)
    val remove = RemoveLabels(rhs, IdName("a"), Seq.empty)
    val input = Apply(lhs, remove)

    rewrite(input, solveds) should equal(RemoveLabels(Apply(lhs, rhs), IdName("a"), Seq.empty))
  }

  private def rewrite(p: LogicalPlan, solveds: Solveds): LogicalPlan =
    fixedPoint((p: LogicalPlan) => p.endoRewrite(unnestEager(solveds, new Attributes(idGen))))(p)
}
