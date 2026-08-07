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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.planner.logical.idp
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPCache.SatisfiedExtraRequirements
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

import scala.collection.immutable.BitSet

class IDPTableTest extends CypherPlannerTestSuite {

  test("removes all traces of a goal") {
    val table = IDPTable.empty[LogicalPlan]

    // 0 is the sorted bit
    addTo(table, Goal(BitSet(1)))
    addTo(table, Goal(BitSet(2)))
    addTo(table, Goal(BitSet(3)))
    addTo(table, Goal(BitSet(1, 2)))
    addTo(table, Goal(BitSet(2, 3)))
    addTo(table, Goal(BitSet(1, 3)))

    table.removeAllTracesOf(Goal(BitSet(1, 2)))

    table.plans.map(_._1).toSet should equal(Set(
      (Goal(BitSet(3)), SatisfiedExtraRequirements(sorted = true, hasExtraProperties = false)),
      (Goal(BitSet(3)), SatisfiedExtraRequirements(sorted = false, hasExtraProperties = false))
    ))
  }

  private def addTo(table: IDPTable[LogicalPlan], goal: idp.Goal): Unit = {
    table.put(goal, sorted = false, hasExtraProperties = false, mock[LogicalPlan])
    table.put(goal, sorted = true, hasExtraProperties = false, mock[LogicalPlan])
  }

  test("goal coveringSplits empty") {
    val goal = Goal(BitSet())
    goal.coveringSplits.toSeq should be(empty)
  }

  test("goal coveringSplits single element") {
    val goal = Goal(BitSet(1))
    goal.coveringSplits.toSeq should be(empty)
  }

  test("goal coveringSplits multiple elements") {
    val goal = Goal(BitSet(1, 2, 3))
    goal.coveringSplits.toSeq should contain theSameElementsAs Seq(
      (Goal(BitSet(1)), Goal(BitSet(2, 3))),
      (Goal(BitSet(2)), Goal(BitSet(1, 3))),
      (Goal(BitSet(3)), Goal(BitSet(1, 2))),
      (Goal(BitSet(1, 2)), Goal(BitSet(3))),
      (Goal(BitSet(1, 3)), Goal(BitSet(2))),
      (Goal(BitSet(2, 3)), Goal(BitSet(1)))
    )
  }
}
