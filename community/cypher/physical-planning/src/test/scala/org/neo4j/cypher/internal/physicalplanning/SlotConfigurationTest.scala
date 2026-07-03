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
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.ApplyPlanSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.CachedPropertySlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.MetaDataSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.OuterNestedApplyPlanSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.SlotWithKeyAndAliases
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.VariableSlotKey
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.InternalException

class SlotConfigurationTest extends CypherFunSuite with AstConstructionTestSupport {

  test("allocating same variable name with compatible type but different nullability should increase nullability 1") {
    // given
    val slots = SlotConfigurationBuilder.empty
    slots.newLong("x", nullable = false, CTNode)

    // when
    slots.newLong("x", nullable = true, CTNode)

    // then
    slots("x") should equal(LongSlot(0, true, CTNode))
  }

  test("allocating same variable name with compatible type but different nullability should increase nullability 2") {
    // given
    val slots = SlotConfigurationBuilder.empty
    slots.newLong("x", nullable = true, CTNode)

    // when
    slots.newLong("x", nullable = false, CTNode)

    // then
    slots("x") should equal(LongSlot(0, true, CTNode))
  }

  test("allocating same variable name with compatible types should work and get the upper bound type 1") {
    // given
    val slots = SlotConfigurationBuilder.empty
    slots.newReference("x", nullable = false, CTInteger)

    // when
    slots.newReference("x", nullable = false, CTNumber)

    // then
    slots("x") should equal(RefSlot(0, false, CTNumber))
  }

  test("allocating same variable name with compatible types should work and get the upper bound type 2") {
    // given
    val slots = SlotConfigurationBuilder.empty
    slots.newReference("x", nullable = false, CTAny)

    // when
    slots.newReference("x", nullable = false, CTNumber)

    // then
    slots("x") should equal(RefSlot(0, false, CTAny))
  }

  test("allocating same variable name with compatible types should work and get the upper bound type 3") {
    // given
    val slots = SlotConfigurationBuilder.empty
    slots.newReference("x", nullable = true, CTMap)

    // when
    slots.newReference("x", nullable = false, CTAny)

    // then
    slots("x") should equal(RefSlot(0, true, CTAny))
  }

  test("allocating same variable name with compatible types should work and get the upper bound type 4") {
    // given
    val slots = SlotConfigurationBuilder.empty
    slots.newReference("x", nullable = false, CTList(CTNumber))

    // when
    slots.newReference("x", nullable = true, CTList(CTInteger))

    // then
    slots("x") should equal(RefSlot(0, nullable = true, CTList(CTNumber)))
  }

  test("allocating metadata with plan id should create new slot") {
    // given
    val slots = SlotConfigurationBuilder.empty

    // when
    slots.newMetaData("meta", Id(1))

    // then
    slots.getMetaDataSlot(MetaDataSlotKey("meta", Id(1))) should equal(Some(RefSlot(0, nullable = false, CTAny)))
  }

  test("allocating metadata with same name and plan id should not create new slot") {
    // given
    val slots = SlotConfigurationBuilder.empty
    slots.newMetaData("meta", Id(1))

    // when
    slots.newMetaData("meta", Id(1))

    // then
    slots.numberOfReferences should equal(1)
  }

  test("allocating metadata with new plan id should create new slot") {
    // given
    val slots = SlotConfigurationBuilder.empty
    slots.newMetaData("meta", Id(1))

    // when
    slots.newMetaData("meta", Id(2))

    // then
    slots.numberOfReferences should equal(2)
    slots.getMetaDataSlot("meta", Id(1)) should equal(Some(RefSlot(0, nullable = false, CTAny)))
    slots.getMetaDataSlot("meta", Id(2)) should equal(Some(RefSlot(1, nullable = false, CTAny)))
  }

  test("can't overwrite variable name by mistake1") {
    // given
    val slots = SlotConfigurationBuilder.empty
    slots.newLong("x", nullable = false, CTNode)

    // when && then
    intercept[InternalException](slots.newLong("x", nullable = false, CTRelationship))
  }

  test("can't overwrite variable name by mistake2") {
    // given
    val slots = SlotConfigurationBuilder.empty
    slots.newLong("x", nullable = false, CTNode)

    // when && then
    intercept[InternalException](slots.newReference("x", nullable = false, CTNode))
  }

  test("can't overwrite variable name by mistake3") {
    // given
    val slots = SlotConfigurationBuilder.empty
    slots.newReference("x", nullable = false, CTNode)

    // when && then
    intercept[InternalException](slots.newLong("x", nullable = false, CTNode))
  }

  test("can't overwrite variable name by mistake4") {
    // given
    val slots = SlotConfigurationBuilder.empty
    slots.newReference("x", nullable = false, CTNode)

    // when && then
    intercept[InternalException](slots.newReference("x", nullable = false, CTRelationship))
  }

  test("copy() should create a deep copy") {
    // given
    val slots = SlotConfigurationBuilder.empty
      .newLong("x", nullable = false, CTNode)
      .newLong("y", nullable = false, CTNode)
      .addAlias("z", "x")

    val clone: SlotConfigurationBuilder = slots.copy()
    slots should equal(clone)

    // when
    slots.newReference("a", nullable = false, CTNode)
    slots.addAlias("w", "y")
    slots.newMetaData("b")

    // then
    assertKeyedSlotsOrderedContains(slots)(Seq(
      SlotWithKeyAndAliases(VariableSlotKey("x"), LongSlot(0, false, CTNode), Set("z")),
      SlotWithKeyAndAliases(VariableSlotKey("y"), LongSlot(1, false, CTNode), Set("w")),
      SlotWithKeyAndAliases(VariableSlotKey("a"), RefSlot(0, false, CTNode), Set.empty),
      SlotWithKeyAndAliases(MetaDataSlotKey("b", Id.INVALID_ID), RefSlot(1, false, CTAny), Set.empty)
    ))

    assertKeyedSlotsOrderedContains(clone)(Seq(
      SlotWithKeyAndAliases(VariableSlotKey("x"), LongSlot(0, false, CTNode), Set("z")),
      SlotWithKeyAndAliases(VariableSlotKey("y"), LongSlot(1, false, CTNode), Set.empty)
    ))
  }

  test("foreachSlotAndAliasesOrdered should not choke on LongSlot aliases") {
    // given
    val slots = SlotConfigurationBuilder.empty
      .newLong("x", nullable = false, CTNode)
      .newLong("y", nullable = false, CTNode)
      .addAlias("z", "x")
      .newArgument(Id(0))
      .newNestedArgument(Id(0))

    // then
    assertKeyedSlotsOrderedContains(slots)(Seq(
      SlotWithKeyAndAliases(VariableSlotKey("x"), LongSlot(0, false, CTNode), Set("z")),
      SlotWithKeyAndAliases(VariableSlotKey("y"), LongSlot(1, false, CTNode), Set.empty),
      SlotWithKeyAndAliases(ApplyPlanSlotKey(Id(0)), LongSlot(2, false, CTAny), Set.empty),
      SlotWithKeyAndAliases(OuterNestedApplyPlanSlotKey(Id(0)), LongSlot(3, false, CTAny), Set.empty)
    ))
  }

  test("foreachSlotAndAliasesOrdered with refs/cached props/longs/applyPlans and skipSlots and aliases") {
    // given
    val slots = SlotConfigurationBuilder.empty
    slots.newArgument(Id(0))
    slots.newLong("a", nullable = false, CTNode)
    slots.addAlias("aa", "a")
    slots.newLong("b", nullable = false, CTNode)
    slots.addAlias("bb", "b")
    slots.addAlias("bbb", "b")
    slots.newArgument(Id(1))
    slots.newNestedArgument(Id(1))

    slots.newReference("c", nullable = false, CTNode)
    slots.newMetaData("cc")
    slots.newReference("d", nullable = false, CTNode)
    val dCP = CachedProperty(varFor("d"), varFor("d"), PropertyKeyName("prop")(pos), NODE_TYPE)(pos)
    slots.newCachedProperty(dCP.runtimeKey)
    slots.newReference("e", nullable = false, CTNode)
    slots.addAlias("ee", "e")
    val eCP = CachedProperty(varFor("e"), varFor("e"), PropertyKeyName("prop")(pos), NODE_TYPE)(pos)
    slots.newCachedProperty(eCP.runtimeKey)

    // then
    assertKeyedSlotsOrderedContains(slots, SlotConfiguration.Size(nLongs = 2, nReferences = 1))(Seq(
      SlotWithKeyAndAliases(VariableSlotKey("b"), LongSlot(2, false, CTNode), Set("bb", "bbb")),
      SlotWithKeyAndAliases(ApplyPlanSlotKey(Id(1)), LongSlot(3, false, CTAny), Set.empty),
      SlotWithKeyAndAliases(OuterNestedApplyPlanSlotKey(Id(1)), LongSlot(4, false, CTAny), Set.empty),
      SlotWithKeyAndAliases(MetaDataSlotKey("cc", Id.INVALID_ID), RefSlot(1, false, CTAny), Set.empty),
      SlotWithKeyAndAliases(VariableSlotKey("d"), RefSlot(2, false, CTNode), Set.empty),
      SlotWithKeyAndAliases(CachedPropertySlotKey(dCP.runtimeKey), RefSlot(3, false, CTAny), Set.empty),
      SlotWithKeyAndAliases(VariableSlotKey("e"), RefSlot(4, false, CTNode), Set("ee")),
      SlotWithKeyAndAliases(CachedPropertySlotKey(eCP.runtimeKey), RefSlot(5, false, CTAny), Set.empty)
    ))
  }

  test("foreachSlotAndAliases with refs/cached props/longs/applyPlans and skipSlots and aliases") {
    // given
    val slots = SlotConfigurationBuilder.empty
    slots.newArgument(Id(0))
    slots.newLong("a", nullable = false, CTNode)
    slots.addAlias("aa", "a")
    slots.newLong("b", nullable = false, CTNode)
    slots.addAlias("bb", "b")
    slots.addAlias("bbb", "b")
    slots.newArgument(Id(1))
    slots.newNestedArgument(Id(1))

    slots.newReference("c", nullable = false, CTNode)
    slots.newMetaData("cc")
    slots.newReference("d", nullable = false, CTNode)
    val dCP = CachedProperty(varFor("d"), varFor("d"), PropertyKeyName("prop")(pos), NODE_TYPE)(pos)
    slots.newCachedProperty(dCP.runtimeKey)
    slots.newReference("e", nullable = false, CTNode)
    slots.addAlias("ee", "e")
    val eCP = CachedProperty(varFor("e"), varFor("e"), PropertyKeyName("prop")(pos), NODE_TYPE)(pos)
    slots.newCachedProperty(eCP.runtimeKey)

    // then
    assertKeyedSlotsOrderedContains(slots)(Seq(
      SlotWithKeyAndAliases(ApplyPlanSlotKey(Id(0)), LongSlot(0, false, CTAny), Set.empty),
      SlotWithKeyAndAliases(VariableSlotKey("a"), LongSlot(1, false, CTNode), Set("aa")),
      SlotWithKeyAndAliases(VariableSlotKey("b"), LongSlot(2, false, CTNode), Set("bb", "bbb")),
      SlotWithKeyAndAliases(ApplyPlanSlotKey(Id(1)), LongSlot(3, false, CTAny), Set.empty),
      SlotWithKeyAndAliases(OuterNestedApplyPlanSlotKey(Id(1)), LongSlot(4, false, CTAny), Set.empty),
      SlotWithKeyAndAliases(VariableSlotKey("c"), RefSlot(0, false, CTNode), Set.empty),
      SlotWithKeyAndAliases(MetaDataSlotKey("cc", Id.INVALID_ID), RefSlot(1, false, CTAny), Set.empty),
      SlotWithKeyAndAliases(VariableSlotKey("d"), RefSlot(2, false, CTNode), Set.empty),
      SlotWithKeyAndAliases(CachedPropertySlotKey(dCP.runtimeKey), RefSlot(3, false, CTAny), Set.empty),
      SlotWithKeyAndAliases(VariableSlotKey("e"), RefSlot(4, false, CTNode), Set("ee")),
      SlotWithKeyAndAliases(CachedPropertySlotKey(eCP.runtimeKey), RefSlot(5, false, CTAny), Set.empty)
    ))
  }

  test("foreachSlot with refs/cached props/longs/applyPlans and skipSlots and aliases") {
    // given
    val slots = SlotConfigurationBuilder.empty
    slots.newArgument(Id(0))
    slots.newLong("a", nullable = false, CTNode)
    slots.addAlias("aa", "a")
    slots.newLong("b", nullable = false, CTNode)
    slots.addAlias("bb", "b")
    slots.addAlias("bbb", "b")
    slots.newArgument(Id(1))
    slots.newNestedArgument(Id(1))

    slots.newReference("c", nullable = false, CTNode)
    slots.newReference("d", nullable = false, CTNode)
    val dCP = CachedProperty(varFor("d"), varFor("d"), PropertyKeyName("prop")(pos), NODE_TYPE)(pos)
    slots.newCachedProperty(dCP.runtimeKey)
    slots.newReference("e", nullable = false, CTNode)
    slots.addAlias("ee", "e")
    val eCP = CachedProperty(varFor("e"), varFor("e"), PropertyKeyName("prop")(pos), NODE_TYPE)(pos)
    slots.newCachedProperty(eCP.runtimeKey)
    slots.newMetaData("f")
    slots.newMetaData("g")

    // then
    assertKeyedSlotsOrderedContains(slots)(Seq(
      SlotWithKeyAndAliases(ApplyPlanSlotKey(Id(0)), LongSlot(0, false, CTAny), Set.empty),
      SlotWithKeyAndAliases(VariableSlotKey("a"), LongSlot(1, nullable = false, CTNode), Set("aa")),
      SlotWithKeyAndAliases(VariableSlotKey("b"), LongSlot(2, nullable = false, CTNode), Set("bb", "bbb")),
      SlotWithKeyAndAliases(ApplyPlanSlotKey(Id(1)), LongSlot(3, false, CTAny), Set.empty),
      SlotWithKeyAndAliases(OuterNestedApplyPlanSlotKey(Id(1)), LongSlot(4, false, CTAny), Set.empty),
      SlotWithKeyAndAliases(VariableSlotKey("c"), RefSlot(0, nullable = false, CTNode), Set.empty),
      SlotWithKeyAndAliases(VariableSlotKey("d"), RefSlot(1, nullable = false, CTNode), Set.empty),
      SlotWithKeyAndAliases(CachedPropertySlotKey(dCP.runtimeKey), RefSlot(2, false, CTAny), Set.empty),
      SlotWithKeyAndAliases(VariableSlotKey("e"), RefSlot(3, nullable = false, CTNode), Set("ee")),
      SlotWithKeyAndAliases(CachedPropertySlotKey(eCP.runtimeKey), RefSlot(4, false, CTAny), Set.empty),
      SlotWithKeyAndAliases(MetaDataSlotKey("f", Id.INVALID_ID), RefSlot(5, nullable = false, CTAny), Set.empty),
      SlotWithKeyAndAliases(MetaDataSlotKey("g", Id.INVALID_ID), RefSlot(6, nullable = false, CTAny), Set.empty)
    ))
  }

  test("addAllSlotsInOrderTo with refs/cached props/longs/applyPlans and skipSlots and aliases") {
    // given

    // slots
    val slots = SlotConfigurationBuilder.empty
    slots.newArgument(Id(0)) // skipped
    slots.newLong("a", nullable = false, CTNode) // skipped
    slots.addAlias("aa", "a") // skipped
    slots.newLong("b", nullable = false, CTNode)
    slots.newArgument(Id(1))
    slots.newReference("c", nullable = false, CTNode) // skipped
    slots.newReference("d", nullable = false, CTNode)
    val dCP = CachedProperty(varFor("d"), varFor("d"), PropertyKeyName("prop")(pos), NODE_TYPE)(pos)
    slots.newCachedProperty(dCP.runtimeKey)
    slots.newMetaData("dd")
    slots.newReference("e", nullable = false, CTNode)
    slots.addAlias("ee", "e")
    val eCP = CachedProperty(varFor("e"), varFor("e"), PropertyKeyName("prop")(pos), NODE_TYPE)(pos)
    slots.newCachedProperty(eCP.runtimeKey)

    // ... which are added to result
    val result = SlotConfigurationBuilder.empty
    result.newLong("z", nullable = false, CTNode)
    result.addAlias("zz", "z")
    result.newReference("y", nullable = false, CTNode)
    result.newArgument(Id(2))
    result.newLong("x", nullable = false, CTNode)
    result.addAlias("xx", "x")
    val xCP = CachedProperty(varFor("x"), varFor("x"), PropertyKeyName("prop")(pos), NODE_TYPE)(pos)
    result.newCachedProperty(xCP.runtimeKey)
    result.newMetaData("xxx")

    // when
    slots.addAllSlotsInOrderTo(result, skipFirst = SlotConfiguration.Size(nLongs = 2, nReferences = 1))

    // then
    // the old stuff
    result("z") should equal(LongSlot(0, nullable = false, CTNode))
    result("zz") should equal(LongSlot(0, nullable = false, CTNode))
    result("y") should equal(RefSlot(0, nullable = false, CTNode))
    result.argumentOffset(Id(2)) should equal(1)
    result("x") should equal(LongSlot(2, nullable = false, CTNode))
    result("xx") should equal(LongSlot(2, nullable = false, CTNode))
    result.cachedPropOffset(xCP.runtimeKey) should equal(1)
    result.metaDataOffset("xxx") should equal(2)

    // the new stuff
    result("b") should equal(LongSlot(3, nullable = false, CTNode))
    result.argumentOffset(Id(1)) should equal(4)
    result("d") should equal(RefSlot(3, nullable = false, CTNode))
    result.cachedPropOffset(dCP.runtimeKey) should equal(4)
    result.metaDataOffset("dd") should equal(5)
    result("e") should equal(RefSlot(6, nullable = false, CTNode))
    result("ee") should equal(RefSlot(6, nullable = false, CTNode))
    result.cachedPropOffset(eCP.runtimeKey) should equal(7)
  }

  test("addAllSlotsInOrderTo should increase numberOfReferences when duplicated cached property is in last slot") {
    // given
    val slots = SlotConfigurationBuilder.empty
    val cachedProp = CachedProperty(varFor("a"), varFor("a"), PropertyKeyName("prop")(pos), NODE_TYPE)(pos)
    slots.newCachedProperty(cachedProp.runtimeKey) // ref slot 0
    slots.newCachedProperty(cachedProp.runtimeKey, shouldDuplicate = true) // ref slot 1

    // when
    val result = SlotConfigurationBuilder.empty
    slots.addAllSlotsInOrderTo(result)

    // then
    result.numberOfReferences should equal(2)
    result.cachedPropOffset(cachedProp.runtimeKey) should equal(0)
    cachedPropertiesTest(result, (cachedProp.runtimeKey, RefSlot(0, nullable = false, typ = CTAny)))
  }

  test("addAllSlotsInOrderTo should add nothing with empty source") {
    // given
    val slots = SlotConfigurationBuilder.empty

    // when
    val result = SlotConfigurationBuilder.empty
    slots.addAllSlotsInOrderTo(result)

    // then
    result.numberOfReferences should equal(0)
    result.numberOfLongs should equal(0)
  }

  test("addAllSlotsInOrderTo should handle duplicated cached properties mixed with ref slots") {
    // given
    val slots = SlotConfigurationBuilder.empty
    val cachedProp = CachedProperty(varFor("a"), varFor("a"), PropertyKeyName("prop")(pos), NODE_TYPE)(pos)

    slots.newCachedProperty(cachedProp.runtimeKey) // ref slot 0, 1 CachedPropertySlotKey
    slots.newReference("b", nullable = true, CTNode) // ref slot 1, 1 CachedPropertySlotKey + 1 VariableSlotKey
    slots.newCachedProperty(
      cachedProp.runtimeKey,
      shouldDuplicate = true
    ) // ref slot 2, 1 CachedPropertySlotKey +  1 DuplicatedSlotKey + 1 VariableSlotKey
    slots.newReference(
      "c",
      nullable = false,
      CTNode
    ) // ref slot 3, 1 CachedPropertySlotKey +  1 DuplicatedSlotKey + 2 VariableSlotKey
    slots.newCachedProperty(
      cachedProp.runtimeKey,
      shouldDuplicate = true
    ) // ref slot 4, 1 CachedPropertySlotKey + 2 DuplicatedSlotKey + 2 VariableSlotKey

    // when
    val result = SlotConfigurationBuilder.empty
    slots.addAllSlotsInOrderTo(result)

    // then
    result.numberOfReferences should equal(5)
    cachedPropertiesTest(result, (cachedProp.runtimeKey, RefSlot(0, nullable = false, typ = CTAny)))
    result.refOffset("b") should equal(1)
    result.refOffset("c") should equal(3)
  }

  test("addAllSlotsInOrderTo should increment numberOfRefs when cached property already exists on target ") {
    // given
    val slots = SlotConfigurationBuilder.empty
    val cachedProp = CachedProperty(varFor("a"), varFor("a"), PropertyKeyName("prop")(pos), NODE_TYPE)(pos)
    slots.newCachedProperty(cachedProp.runtimeKey)
    slots.newReference("b", nullable = true, CTNode)

    val result = SlotConfigurationBuilder.empty
    result.newReference("a", nullable = false, CTNode)
    result.newCachedProperty(cachedProp.runtimeKey)

    // when
    slots.addAllSlotsInOrderTo(result)

    // then
    result.numberOfReferences should equal(4)
    result.refOffset("a") should equal(0)
    result.refOffset("b") should equal(3)
    cachedPropertiesTest(result, (cachedProp.runtimeKey, RefSlot(1, nullable = false, typ = CTAny)))
  }

  test("addAllSlotsInOrderTo with skipSlots and cached properties") {
    // given
    val slots = SlotConfigurationBuilder.empty
    val cachedProp = CachedProperty(varFor("a"), varFor("a"), PropertyKeyName("prop")(pos), NODE_TYPE)(pos)

    slots.newReference("skip_this", nullable = false, CTAny) // ref slot 0 --> skip this
    slots.newReference("skip_this_too", nullable = false, CTAny) // ref slot 1 --> skip this
    slots.newCachedProperty(cachedProp.runtimeKey) // ref slot 2 --> skip this
    slots.newCachedProperty(cachedProp.runtimeKey, shouldDuplicate = true) // ref slot 3 --> ref slot 0
    slots.newCachedProperty(cachedProp.runtimeKey, shouldDuplicate = true) // ref slot 4 --> ref slot 1
    slots.newCachedProperty(cachedProp.runtimeKey, shouldDuplicate = true) // ref slot 5 --> ref slot 2
    slots.newReference("b", nullable = true, CTNode) // ref slot 6 --> ref slot 3

    // when
    val result = SlotConfigurationBuilder.empty
    slots.addAllSlotsInOrderTo(result, skipFirst = SlotConfiguration.Size(0, 3))
    val immutableResult = result.build()

    // then
    result.numberOfReferences should equal(4)
    assertThrows[NoSuchElementException](result.cachedPropOffset(cachedProp.runtimeKey))
    assertThrows[NoSuchElementException](immutableResult.cachedPropOffset(cachedProp.runtimeKey))
    result.refOffset("b") should equal(3)
    immutableResult.refOffset("b") should equal(3)
  }

  test("iterate cached property slots") {
    val slots = SlotConfigurationBuilder.empty

    cachedPropertiesTest(slots)

    slots.newMetaData("meta")
    slots.newArgument(Id(1))
    slots.newLong("long", false, CTNode)
    slots.newReference("ref", true, CTNode)

    cachedPropertiesTest(slots)

    val cachedProp1 = CachedProperty(varFor("d"), varFor("d"), PropertyKeyName("prop")(pos), NODE_TYPE)(pos)
    slots.newCachedProperty(cachedProp1.runtimeKey)

    cachedPropertiesTest(slots, cachedProp1.runtimeKey -> RefSlot(2, false, CTAny))

    slots.newCachedProperty(cachedProp1.runtimeKey)

    cachedPropertiesTest(slots, cachedProp1.runtimeKey -> RefSlot(2, false, CTAny))

    val cachedProp2 = CachedProperty(varFor("e"), varFor("e"), PropertyKeyName("prop")(pos), NODE_TYPE)(pos)
    slots.newCachedProperty(cachedProp2.runtimeKey)

    cachedPropertiesTest(
      slots,
      cachedProp1.runtimeKey -> RefSlot(2, false, CTAny),
      cachedProp2.runtimeKey -> RefSlot(3, false, CTAny)
    )

    cachedPropertiesTest(
      slots.copy(),
      cachedProp1.runtimeKey -> RefSlot(2, false, CTAny),
      cachedProp2.runtimeKey -> RefSlot(3, false, CTAny)
    )

    val newSlots = SlotConfigurationBuilder.empty
    slots.addAllSlotsInOrderTo(newSlots)

    cachedPropertiesTest(
      newSlots,
      cachedProp1.runtimeKey -> RefSlot(2, false, CTAny),
      cachedProp2.runtimeKey -> RefSlot(3, false, CTAny)
    )
  }

  test("addAlias on existing slot should insert a DuplicatedSlot") {

    val slots = SlotConfigurationBuilder.empty

    slots.newLong("x", nullable = false, CTNode)
    slots.newLong("y", nullable = false, CTNode)
    slots.addAlias("x", "y")

    slots.longOffset("x") shouldBe 1
    slots.longOffset("y") shouldBe 1
    slots.hasDuplicateSlot("x", 0) shouldBe true
  }

  private def cachedPropertiesTest(
    slots: SlotConfigurationBuilder,
    expected: (ASTCachedProperty.RuntimeKey, RefSlot)*
  ): Unit = {
    expected.foreach {
      case (key, slot) =>
        slots.hasCachedPropertySlot(key) shouldBe true
        slots.cachedPropSlot(key) shouldBe Some(slot)
        slots.cachedPropOffset(key) shouldBe slot.offset
    }

    val immutableSlots = slots.copy().build()

    val collectedSlots = Range(immutableSlots.cachedPropertiesStartIndex, immutableSlots.cachedPropertiesEndIndex)
      .map(i => immutableSlots.slots(i).slot)

    withClue(
      s"cachedPropertiesStartIndex=${immutableSlots.cachedPropertiesStartIndex}, cachedPropertiesEndIndex=${immutableSlots.cachedPropertiesEndIndex}"
    ) {
      collectedSlots should contain theSameElementsAs expected.map(_._2)
    }

    val collectedSlotOffsets = immutableSlots.cachedPropertyOffsets

    collectedSlotOffsets should contain theSameElementsAs expected.map { case (_, slot) => slot.offset }

    slots.iterableWithAliases
      .collect { case (CachedPropertySlotKey(p), slot: RefSlot) => p -> slot }
      .toSeq should contain theSameElementsAs expected
  }

  private def assertKeyedSlotsOrderedContains(
    slots: SlotConfigurationBuilder,
    skip: SlotConfiguration.Size = SlotConfiguration.Size.zero,
    build: Boolean = true
  )(expected: Seq[SlotWithKeyAndAliases]): Unit = {
    val iterA = slots.keyedSlotsOrdered(skip).iterator
    val iterB = expected.iterator

    while (iterA.hasNext && iterB.hasNext) iterA.next() shouldBe iterB.next()

    slots.keyedSlotsOrdered(skip) should contain theSameElementsInOrderAs expected
    if (build) {
      slots.build().legacyView.keyedSlotsOrdered(skip) should contain theSameElementsInOrderAs expected
    } else {
      slots.copy().build().legacyView.keyedSlotsOrdered(skip) should contain theSameElementsInOrderAs expected
    }
  }

  trait HasSlot {
    def slot: Slot
  }
}
