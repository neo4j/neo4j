/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration
import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values.stringValue

class PrimitiveExecutionContextTest extends CypherFunSuite {

  private def slots(longs: Int, refs: Int) = SlotConfiguration(Map.empty, longs, refs)

  test("copy fills upp the first few elements") {
    val input = SlottedExecutionContext(slots(2, 1))
    val result = SlottedExecutionContext(slots(3, 2))

    input.setLongAt(0, 42)
    input.setLongAt(1, 666)
    input.setRefAt(0, stringValue("21"))

    result.copyFrom(input, 2, 1)

    result.getLongAt(0) should equal(42)
    result.getLongAt(1) should equal(666)
    result.getRefAt(0) should equal(stringValue("21"))
  }

  test("copy fails if copy from larger") {
    val input = SlottedExecutionContext(slots(4, 0))
    val result = SlottedExecutionContext(slots(2, 0))

    intercept[InternalException](result.copyFrom(input, 4, 0))
  }

  test("copy fails if copy from larger 2") {
    val input = SlottedExecutionContext(slots(0, 4))
    val result = SlottedExecutionContext(slots(0, 2))

    intercept[InternalException](result.copyFrom(input, 0, 4))
  }
}
