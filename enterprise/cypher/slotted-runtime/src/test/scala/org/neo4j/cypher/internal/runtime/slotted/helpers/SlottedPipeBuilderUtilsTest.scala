/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.runtime.slotted.helpers

import SlottedPipeBuilderUtils._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{Slot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.util.v3_4.AssertionUtils._
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
    .newReference("y", false, CTAny)

  // GETTING

  private def assertGetLong(slot: Slot, longValue: Long, expectedValue: AnyValue) = {
    val context = SlottedExecutionContext(slots)
    val getter = makeGetValueFromSlotFunctionFor(slot)

    context.setLongAt(slot.offset, longValue)
    val value = getter(context)
    value should equal(expectedValue)
  }

  private def assertGetNode(slot: Slot, id: Long) = assertGetLong(slot, id, VirtualValues.node(id))
  private def assertGetRelationship(slot: Slot, id: Long) = assertGetLong(slot, id, VirtualValues.relationship(id))

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
  private def assertSetRelationship(slot: Slot, id: Long) = assertSetLong(slot, VirtualValues.relationship(id), id)

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

  private def assertPrimitiveNodeSetLong(slot: Slot, id: Long): Unit = {
    val context = SlottedExecutionContext(slots)
    val primitiveNodeSetter = makeSetPrimitiveNodeInSlotFunctionFor(slot)

    primitiveNodeSetter(context, id)
    context.getLongAt(slot.offset) should equal(id)
  }

  private def assertPrimitiveRelationshipSetLong(slot: Slot, id: Long): Unit = {
    val context = SlottedExecutionContext(slots)
    val primitiveRelationshipSetter = makeSetPrimitiveRelationshipInSlotFunctionFor(slot)

    primitiveRelationshipSetter(context, id)
    context.getLongAt(slot.offset) should equal(id)
  }

  private def assertPrimitiveNodeSetRef(slot: Slot, id: Long, expected: AnyValue): Unit = {
    val context = SlottedExecutionContext(slots)
    val primitiveNodeSetter = makeSetPrimitiveNodeInSlotFunctionFor(slot)

    primitiveNodeSetter(context, id)
    context.getRefAt(slot.offset) should equal(expected)
  }

  private def assertPrimitiveRelationshipSetRef(slot: Slot, id: Long, expected: AnyValue): Unit = {
    val context = SlottedExecutionContext(slots)
    val primitiveRelationshipSetter = makeSetPrimitiveRelationshipInSlotFunctionFor(slot)

    primitiveRelationshipSetter(context, id)
    context.getRefAt(slot.offset) should equal(expected)
  }

  private def assertPrimitiveNodeSetFails(slot: Slot, id: Long): Unit = {
    val context = SlottedExecutionContext(slots)
    val setter = makeSetPrimitiveNodeInSlotFunctionFor(slot)

    // The setter only throws if assertions are enabled
    ifAssertionsEnabled {
      a[ParameterWrongTypeException] should be thrownBy (setter(context, id))
    }
  }

  private def assertPrimitiveRelationshipSetFails(slot: Slot, id: Long): Unit = {
    val context = SlottedExecutionContext(slots)
    val setter = makeSetPrimitiveRelationshipInSlotFunctionFor(slot)

    // The setter only throws if assertions are enabled
    ifAssertionsEnabled {
      a[ParameterWrongTypeException] should be thrownBy (setter(context, id))
    }
  }

  test("primitive node setter for non-nullable node slot") {
    assertPrimitiveNodeSetLong(slots("n1"), 42L)
  }

  test("primitive node setter for nullable node slots with null") {
    assertPrimitiveNodeSetLong(slots("n2"), -1L)
  }

  test("primitive node setter for nullable node slot") {
    assertPrimitiveNodeSetLong(slots("n2"), 42L)
  }

  test("primitive relationship setter for non-nullable relationship slot") {
    assertPrimitiveRelationshipSetLong(slots("r1"), 42L)
  }

  test("primitive relationship setter for nullable relationship slots with null") {
    assertPrimitiveRelationshipSetLong(slots("r2"), -1)
  }

  test("primitive relationship setter for nullable relationship slot") {
    assertPrimitiveRelationshipSetLong(slots("r2"), 42L)
  }

  test("primitive node setter for non-nullable node slot should throw") {
    assertPrimitiveNodeSetFails(slots("n1"), -1L)
  }

  test("primitive relationship setter for non-nullable relationship slot should throw") {
    assertPrimitiveRelationshipSetFails(slots("r1"), -1L)
  }

  test("primitive node setter for ref slot") {
    assertPrimitiveNodeSetRef(slots("x"), 42L, VirtualValues.node(42L))
  }

  test("primitive node setter for nullable ref slot with null") {
    assertPrimitiveNodeSetRef(slots("x"), -1L, Values.NO_VALUE)
  }

  test("primitive node setter for non-nullable ref slot should throw") {
    assertPrimitiveNodeSetFails(slots("y"), -1L)
  }

  test("primitive relationship setter for ref slot") {
    assertPrimitiveRelationshipSetRef(slots("x"), 42L, VirtualValues.relationship(42L))
  }

  test("primitive relationship setter for nullable ref slot with null") {
    assertPrimitiveRelationshipSetRef(slots("x"), -1L, Values.NO_VALUE)
  }

  test("primitive relationship setter for non-nullable ref slot should throw") {
    assertPrimitiveRelationshipSetFails(slots("y"), -1L)
  }
}
