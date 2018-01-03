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
/**
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

import scala.collection.immutable.BitSet

class IDPTableTest extends CypherFunSuite {

  test("removes all traces of a goal") {
    val table = new IDPTable[LogicalPlan]()

    table.put(BitSet(0), mock[LogicalPlan])
    table.put(BitSet(1), mock[LogicalPlan])
    table.put(BitSet(2), mock[LogicalPlan])
    table.put(BitSet(0, 1), mock[LogicalPlan])
    table.put(BitSet(1, 2), mock[LogicalPlan])
    table.put(BitSet(0, 2), mock[LogicalPlan])

    table.removeAllTracesOf(BitSet(0, 1))

    table.plans.map(_._1).toSet should equal(Set(BitSet(2)))
  }
}
