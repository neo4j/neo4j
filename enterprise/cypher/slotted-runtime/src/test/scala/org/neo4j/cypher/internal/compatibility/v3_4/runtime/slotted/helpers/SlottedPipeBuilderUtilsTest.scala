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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.slotted.helpers

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.slotted.helpers.SlottedPipeBuilderUtils._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{Slot, SlotConfiguration}
import org.neo4j.cypher.internal.util.v3_4.ParameterWrongTypeException
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

// If this test class gets in your way you can just delete it
class SlottedPipeBuilderUtilsTest extends CypherFunSuite {
  private val slots = SlotConfiguration.empty
    .newLong("n1", false, CTNode)
    .newLong("n2", true, CTNode)
    .newLong("r1", false, CTRelationship)
    .newLong("r2", true, CTRelationship)
    .newReference("x", true, CTAny)

  // GETTING

  private def assertGetLong(slot: Slot, longValue: Long, expectedValue: AnyValue) = {
    val context = SlottedExecutionContext(slots)
    val getter = makeGetValueFromSlotFunctionFor(slot)

    context.setLongAt(slot.offset, longValue)
    val value = getter(context)
    value should equal(expectedValue)
  }

  private def assertGetNode(slot: Slot, id: Long) = assertGetLong(slot, id, VirtualValues.node(id))
  private def assertGetRelationship(slot: Slot, id: Long) = assertGetLong(slot, id, VirtualValues.edge(id))

  test("getter for non-nullable node slot") {
    assertGetNode(slots("n1"), 42L)
  }

  test("getter for nullable node slots with null") {
    assertGetLong(slots("n2"), -1, Values.NO_VALUE)
  }

  test("getter for nullable node slot") {
    assertGetNode(slots("n2"), 42L)
  }

  test("getter for non-nullable relationship slot") {
    assertGetRelationship(slots("r1"), 42L)
  }

  test("getter for nullable relationship slots with null") {
    assertGetLong(slots("r2"), -1, Values.NO_VALUE)
  }

  test("getter for nullable relationship slot") {
    assertGetRelationship(slots("r2"), 42L)
  }

  test("getter for ref slot") {
    val slot = slots("x")

    val context = SlottedExecutionContext(slots)
    val getter = makeGetValueFromSlotFunctionFor(slot)

    val expectedValue = Values.stringValue("the value")
    context.setRefAt(slot.offset, expectedValue)
    val value = getter(context)
    value should equal(expectedValue)
  }

  // SETTING

  private def assertSetLong(slot: Slot, value: AnyValue, expected: Long): Unit = {
    val context = SlottedExecutionContext(slots)
    val setter = makeSetValueInSlotFunctionFor(slot)

    setter(context, value)
    context.getLongAt(slot.offset) should equal(expected)
  }

  private def assertSetNode(slot: Slot, id: Long) = assertSetLong(slot, VirtualValues.node(id), id)
  private def assertSetRelationship(slot: Slot, id: Long) = assertSetLong(slot, VirtualValues.edge(id), id)

  private def assertSetFails(slot: Slot, value: AnyValue): Unit = {
    val context = SlottedExecutionContext(slots)
    val setter = makeSetValueInSlotFunctionFor(slot)

    a [ParameterWrongTypeException] should be thrownBy(setter(context, value))
  }

  test("setter for non-nullable node slot") {
    assertSetNode(slots("n1"), 42L)
  }

  test("setter for nullable node slots with null") {
    assertSetLong(slots("n2"), Values.NO_VALUE, -1)
  }

  test("setter for nullable node slot") {
    assertSetNode(slots("n2"), 42L)
  }

  test("setter for non-nullable relationship slot") {
    assertSetRelationship(slots("r1"), 42L)
  }

  test("setter for nullable relationship slots with null") {
    assertSetLong(slots("r2"), Values.NO_VALUE, -1)
  }

  test("setter for nullable relationship slot") {
    assertSetRelationship(slots("r2"), 42L)
  }

  test("setter for non-nullable node slot should throw") {
    assertSetFails(slots("n1"), Values.stringValue("oops"))
  }

  test("setter for nullable node slot should throw") {
    assertSetFails(slots("n2"), Values.stringValue("oops"))
  }

  test("setter for non-nullable relationship slot should throw") {
    assertSetFails(slots("r1"), Values.stringValue("oops"))
  }

  test("setter for nullable relationship slot should throw") {
    assertSetFails(slots("r2"), Values.stringValue("oops"))
  }

  test("setter for ref slot") {
    val slot = slots("x")

    val context = SlottedExecutionContext(slots)
    val setter = makeSetValueInSlotFunctionFor(slot)

    val expectedValue = Values.stringValue("the value")
    setter(context, expectedValue)
    val value = context.getRefAt(slot.offset)
    value should equal(expectedValue)
  }
}
