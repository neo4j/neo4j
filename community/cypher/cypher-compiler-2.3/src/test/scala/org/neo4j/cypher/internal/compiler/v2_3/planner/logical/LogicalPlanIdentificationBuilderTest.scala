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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical

import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{Apply, AllNodesScan, IdName}
import org.neo4j.cypher.internal.compiler.v2_3.planner.{CardinalityEstimation, PlannerQuery}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class LogicalPlanIdentificationBuilderTest extends CypherFunSuite {
  val solved = CardinalityEstimation.lift(PlannerQuery.empty, Cardinality(1))

  test("plan is put into ID map") {
    val A = AllNodesScan(IdName("a"), Set.empty)(solved)

    val map = LogicalPlanIdentificationBuilder(A)
    map.keys.toList should equal(List(A))
    map.values.toList should equal(map.values.toList.distinct) // Ids must be unique
    map.values shouldNot contain(null)
  }

  test("plan and it's children are identified") {
    val A = AllNodesScan(IdName("a"), Set.empty)(solved)
    val B = AllNodesScan(IdName("b"), Set.empty)(solved)
    val AB = Apply(A, B)(solved)

    val map = LogicalPlanIdentificationBuilder(AB)
    map.keys.toSet should equal(Set(A, B, AB))
    map.values.toList should equal(map.values.toList.distinct)
    map.values shouldNot contain(null)
  }

  test("plan and decedents") {
    val A = AllNodesScan(IdName("a"), Set.empty)(solved)
    val B = AllNodesScan(IdName("b"), Set.empty)(solved)
    val AB = Apply(A, B)(solved)

    val C = AllNodesScan(IdName("c"), Set.empty)(solved)
    val D = AllNodesScan(IdName("d"), Set.empty)(solved)
    val CD = Apply(C, D)(solved)

    val ABCD = Apply(AB, CD)(solved)

    val map = LogicalPlanIdentificationBuilder(ABCD)
    map.keys.toSet should equal(Set(A, B, C, D, AB, CD, ABCD))
    map.values.toList should equal(map.values.toList.distinct)
    map.values shouldNot contain(null)
  }
}
