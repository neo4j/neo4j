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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.logical.plans.{Apply, Argument}

class LogicalPlanTest extends CypherFunSuite with LogicalPlanningTestSupport  {

  test("single row returns itself as the leafs") {
    val argument = Argument(Set("a"))

    argument.leaves should equal(Seq(argument))
  }

  test("apply with two arguments should return them both") {
    val argument1 = Argument(Set("a"))
    val argument2 = Argument()
    val apply = Apply(argument1, argument2)

    apply.leaves should equal(Seq(argument1, argument2))
  }

  test("apply pyramid should work multiple levels deep") {
    val argument1 = Argument(Set("a"))
    val argument2 = Argument()
    val argument3 = Argument(Set("b"))
    val argument4 = Argument()
    val apply1 = Apply(argument1, argument2)
    val apply2 = Apply(argument3, argument4)
    val metaApply = Apply(apply1, apply2)

    metaApply.leaves should equal(Seq(argument1, argument2, argument3, argument4))
  }
}
