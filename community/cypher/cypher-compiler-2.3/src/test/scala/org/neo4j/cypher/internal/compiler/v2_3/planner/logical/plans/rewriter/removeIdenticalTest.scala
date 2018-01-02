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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.v2_3.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class removeIdenticalTest extends CypherFunSuite with LogicalPlanningTestSupport {
  test("should not contain copies") {
    val scan = AllNodesScan(IdName("a"), Set.empty)(solved)
    val join = NodeHashJoin(Set(IdName("a")), scan, scan)(solved)

    val rewritten = join.endoRewrite(removeIdenticalPlans)

    rewritten should equal(join)
    rewritten shouldNot be theSameInstanceAs join
    rewritten.left shouldNot be theSameInstanceAs rewritten.right
  }

  test("should not rewrite when not needed") {
    val scan1 = AllNodesScan(IdName("a"), Set.empty)(solved)
    val scan2 = AllNodesScan(IdName("a"), Set.empty)(solved)
    val join = NodeHashJoin(Set(IdName("a")), scan1, scan2)(solved)

    val rewritten = join.endoRewrite(removeIdenticalPlans)

    rewritten should equal(join)
    rewritten.left should be theSameInstanceAs join.left
    rewritten.right should be theSameInstanceAs join.right
  }
}
