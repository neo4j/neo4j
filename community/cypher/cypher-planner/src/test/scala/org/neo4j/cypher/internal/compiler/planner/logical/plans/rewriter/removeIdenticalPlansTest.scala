/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class removeIdenticalPlansTest extends CypherFunSuite with LogicalPlanningTestSupport {
  private val noAttributes = Attributes[LogicalPlan](idGen)

  test("should not contain copies") {
    val scan = AllNodesScan(v"a", Set.empty)
    val join = NodeHashJoin(Set(v"a"), scan, scan)

    val rewritten = join.endoRewrite(removeIdenticalPlans(noAttributes))

    rewritten should equal(join)
    rewritten shouldNot be theSameInstanceAs join
    rewritten.left shouldNot be theSameInstanceAs rewritten.right
  }

  test("should not rewrite when not needed") {
    val scan1 = AllNodesScan(v"a", Set.empty)
    val scan2 = AllNodesScan(v"a", Set.empty)
    val join = NodeHashJoin(Set(v"a"), scan1, scan2)

    val rewritten = join.endoRewrite(removeIdenticalPlans(noAttributes))

    rewritten should equal(join)
    rewritten.left should be theSameInstanceAs join.left
    rewritten.right should be theSameInstanceAs join.right
  }
}
