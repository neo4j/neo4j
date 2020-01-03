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
import org.neo4j.cypher.internal.ir.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

import scala.collection.immutable.BitSet

class IDPTableTest extends CypherFunSuite {

  test("removes all traces of a goal") {
    val table = new IDPTable[LogicalPlan, InterestingOrder]()

    addTo(table, BitSet(0))
    addTo(table, BitSet(1))
    addTo(table, BitSet(2))
    addTo(table, BitSet(0, 1))
    addTo(table, BitSet(1, 2))
    addTo(table, BitSet(0, 2))

    table.removeAllTracesOf(BitSet(0, 1))

    table.plans.map(_._1._1).toSet should equal(Set(BitSet(2)))
  }

  private def addTo(table: IDPTable[LogicalPlan, InterestingOrder], goal: idp.Goal): Unit = {
    table.put(goal, InterestingOrder.empty, mock[LogicalPlan])
  }
}
