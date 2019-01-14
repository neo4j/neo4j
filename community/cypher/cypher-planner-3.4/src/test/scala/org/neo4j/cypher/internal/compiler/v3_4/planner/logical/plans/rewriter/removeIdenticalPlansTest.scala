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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.v3_4.attribution.Attributes
import org.neo4j.cypher.internal.v3_4.logical.plans.{AllNodesScan, NodeHashJoin}

class removeIdenticalPlansTest extends CypherFunSuite with LogicalPlanningTestSupport {
  val noAttributes = new Attributes(idGen)

  test("should not contain copies") {
    val scan = AllNodesScan("a", Set.empty)
    val join = NodeHashJoin(Set("a"), scan, scan)

    val rewritten = join.endoRewrite(removeIdenticalPlans(noAttributes))

    rewritten should equal(join)
    rewritten shouldNot be theSameInstanceAs join
    rewritten.left shouldNot be theSameInstanceAs rewritten.right
  }

  test("should not rewrite when not needed") {
    val scan1 = AllNodesScan("a", Set.empty)
    val scan2 = AllNodesScan("a", Set.empty)
    val join = NodeHashJoin(Set("a"), scan1, scan2)

    val rewritten = join.endoRewrite(removeIdenticalPlans(noAttributes))

    rewritten should equal(join)
    rewritten.left should be theSameInstanceAs join.left
    rewritten.right should be theSameInstanceAs join.right
  }
}
