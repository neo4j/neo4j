/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.neo4j.cypher.internal.compiler.planner.logical.idp
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.immutable.BitSet

class IDPTableTest extends CypherFunSuite {

  test("removes all traces of a goal") {
    val table = new IDPTable[LogicalPlan]()

    // 0 is the sorted bit
    addTo(table, BitSet(1))
    addTo(table, BitSet(2))
    addTo(table, BitSet(3))
    addTo(table, BitSet(1, 2))
    addTo(table, BitSet(2, 3))
    addTo(table, BitSet(1, 3))

    table.removeAllTracesOf(BitSet(1, 2))

    table.plans.map(_._1).toSet should equal(Set((BitSet(3), true), (BitSet(3), false)))
  }

  private def addTo(table: IDPTable[LogicalPlan], goal: idp.Goal): Unit = {
    table.put(goal, sorted = false, mock[LogicalPlan])
    table.put(goal, sorted = true, mock[LogicalPlan])
  }
}
