/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration
import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.cypher.internal.util.v3_4.symbols._

class SlottedExecutionContextTest extends CypherFunSuite {

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

  test("can merge nullable RefSlots which are null") {
    val leftSlots = slots(0, 0).newReference("a", nullable = true, CTAny)
    SlottedPipeBuilder.generateSlotAccessorFunctions(leftSlots)
    val rightSlots = slots(0, 0).newReference("a", nullable = true, CTAny)
    SlottedExecutionContext(leftSlots).mergeWith(SlottedExecutionContext(rightSlots)) // should not fail
  }
}
