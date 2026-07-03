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
package org.neo4j.cypher.internal.runtime.slotted.helpers

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.KeyedSlot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.VariableSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationBuilder
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetValueFromSlotFunctionFor
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeSetPrimitiveNodeInSlotFunctionFor
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeSetPrimitiveRelationshipInSlotFunctionFor
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeSetValueInSlotFunctionFor
import org.neo4j.cypher.internal.runtime.EntityById
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.AssertionRunner.isAssertionsEnabled
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.RelationshipValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues

// TODO: Extract abstract base class in physical planning module, with subclass tests for both slotted and pipelined runtimes

// If this test class gets in your way you can just delete it
class SlotConfigurationUtilsTest extends CypherFunSuite {

  private val slots = SlotConfigurationBuilder.empty
    .newLong("n1", nullable = false, CTNode)
    .newLong("n2", nullable = true, CTNode)
    .newLong("r1", nullable = false, CTRelationship)
    .newLong("r2", nullable = true, CTRelationship)
    .newReference("x", nullable = true, CTAny)
    .newReference("y", nullable = false, CTAny)
    .build()

  // GETTING

  private def assertGetLong(slot: KeyedSlot, longValue: Long, expectedValue: AnyValue) = {
    val context = SlottedRow(slots)
    val getter = makeGetValueFromSlotFunctionFor(slot.slot)

    context.setLongAt(slot.offset, longValue)
    val value = getter(context)
    value should equal(expectedValue)
    context.getByName(slot.key.asInstanceOf[VariableSlotKey].name) shouldBe expectedValue
  }

  private def assertGetNode(slot: KeyedSlot, id: Long) = assertGetLong(slot, id, VirtualValues.node(id))
  private def assertGetRelationship(slot: KeyedSlot, id: Long) = assertGetLong(slot, id, VirtualValues.relationship(id))

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

    val context = SlottedRow(slots)
    val getter = makeGetValueFromSlotFunctionFor(slot.slot)

    val expectedValue = Values.stringValue("the value")
    context.setRefAt(slot.offset, expectedValue)
    val value = getter(context)
    value should equal(expectedValue)
  }

  // SETTING

  private def assertSetLong(slot: KeyedSlot, value: AnyValue, expected: Long): Unit = {
    val context = SlottedRow(slots)
    val setter = makeSetValueInSlotFunctionFor(slot.slot)
    setter(context, value)
    context.getLongAt(slot.offset) should equal(expected)

    val row = SlottedRow(slots)
    row.set(slot.key.asInstanceOf[VariableSlotKey].name, value)
    row.getLongAt(slot.offset) should equal(expected)
  }

  private def assertSetNode(slot: KeyedSlot, id: Long): Unit = assertSetLong(slot, VirtualValues.node(id), id)

  private def assertSetRelationship(slot: KeyedSlot, id: Long): Unit =
    assertSetLong(slot, VirtualValues.relationship(id), id)

  private def assertSetFails(slot: KeyedSlot, value: AnyValue): Unit = {
    val context = SlottedRow(slots)
    val setter = makeSetValueInSlotFunctionFor(slot.slot)

    a[ParameterWrongTypeException] should be thrownBy setter(context, value)
    a[ParameterWrongTypeException] should be thrownBy context.set(slot.key.asInstanceOf[VariableSlotKey].name, value)
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

    val context = SlottedRow(slots)
    val setter = makeSetValueInSlotFunctionFor(slot.slot)

    val expectedValue = Values.stringValue("the value")
    setter(context, expectedValue)
    context.getRefAt(slot.offset) shouldBe expectedValue

    val row = SlottedRow(slots)
    row.set("x", expectedValue)
    row.getRefAt(slot.offset) shouldBe expectedValue
    row.getByName("x") shouldBe expectedValue
  }

  private def assertPrimitiveNodeSetLong(slot: KeyedSlot, id: Long): Unit = {
    val context = SlottedRow(slots)
    val primitiveNodeSetter = makeSetPrimitiveNodeInSlotFunctionFor(slot.slot)

    primitiveNodeSetter(context, id, TestEntityById)
    context.getLongAt(slot.offset) should equal(id)

    val row = SlottedRow(slots)
    row.set(slot.key.asInstanceOf[VariableSlotKey].name, if (id == -1) Values.NO_VALUE else VirtualValues.node(id))
    row.getLongAt(slot.offset) shouldBe id
    row.getByName(slot.key.asInstanceOf[VariableSlotKey].name) shouldBe
      (if (id == -1) Values.NO_VALUE else VirtualValues.node(id))
  }

  private def assertPrimitiveRelationshipSetLong(slot: KeyedSlot, id: Long): Unit = {
    val context = SlottedRow(slots)
    val primitiveRelationshipSetter = makeSetPrimitiveRelationshipInSlotFunctionFor(slot.slot)

    primitiveRelationshipSetter(context, id, TestEntityById)
    context.getLongAt(slot.offset) shouldBe id

    val row = SlottedRow(slots)
    row.setPrimitiveRel(slot.key, id)
    context.getLongAt(slot.offset) shouldBe id
  }

  private def assertPrimitiveNodeSetRef(slot: KeyedSlot, id: Long, expected: AnyValue): Unit = {
    val context = SlottedRow(slots)
    val primitiveNodeSetter = makeSetPrimitiveNodeInSlotFunctionFor(slot.slot)

    primitiveNodeSetter(context, id, TestEntityById)
    context.getRefAt(slot.offset) should equal(expected)

    val row = SlottedRow(slots)
    row.setPrimitiveNode(slot.key, id)
    row.getRefAt(slot.offset) shouldBe expected
  }

  private def assertPrimitiveRelationshipSetRef(slot: KeyedSlot, id: Long, expected: AnyValue): Unit = {
    val context = SlottedRow(slots)
    val primitiveRelationshipSetter = makeSetPrimitiveRelationshipInSlotFunctionFor(slot.slot)

    primitiveRelationshipSetter(context, id, TestEntityById)
    context.getRefAt(slot.offset) should equal(expected)

    val row = SlottedRow(slots)
    row.setPrimitiveRel(slot.key, id)
    row.getRefAt(slot.offset) shouldBe expected
  }

  private def assertPrimitiveNodeSetFails(slot: KeyedSlot, id: Long): Unit = {
    val context = SlottedRow(slots)
    val setter = makeSetPrimitiveNodeInSlotFunctionFor(slot.slot)

    // The setter only throws if assertions are enabled
    if (isAssertionsEnabled) {
      a[ParameterWrongTypeException] should be thrownBy setter(context, id, TestEntityById)
      a[AssertionError] should be thrownBy context.setPrimitiveNode(slot.key, id)
    }
  }

  private def assertPrimitiveRelationshipSetFails(slot: KeyedSlot, id: Long): Unit = {
    val context = SlottedRow(slots)
    val setter = makeSetPrimitiveRelationshipInSlotFunctionFor(slot.slot)

    // The setter only throws if assertions are enabled
    if (isAssertionsEnabled) {
      a[ParameterWrongTypeException] should be thrownBy setter(context, id, TestEntityById)
      a[AssertionError] should be thrownBy context.setPrimitiveRel(slot.key, id)
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

  object TestEntityById extends EntityById {

    override def nodeById(id: Long): NodeValue =
      VirtualValues.nodeValue(id, "n", Values.EMPTY_TEXT_ARRAY, VirtualValues.EMPTY_MAP)

    override def relationshipById(id: Long): RelationshipValue = VirtualValues.relationshipValue(
      id,
      "r",
      nodeById(id * 100),
      nodeById(id * 1000),
      Values.EMPTY_STRING,
      VirtualValues.EMPTY_MAP
    )
    override def relationshipById(id: Long, startNode: Long, endNode: Long, `type`: Int): VirtualRelationshipValue = ???
  }
}
