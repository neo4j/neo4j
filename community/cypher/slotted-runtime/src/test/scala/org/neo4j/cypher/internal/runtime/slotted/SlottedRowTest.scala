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
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.SlotWithKeyAndAliases
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.VariableSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationBuilder
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.PRIMITIVE_NULL
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeSetPrimitiveNodeInSlotFunctionFor
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeSetPrimitiveRelationshipInSlotFunctionFor
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeSetValueInSlotFunctionFor
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.EntityById
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTDate
import org.neo4j.cypher.internal.util.symbols.CTDuration
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.InternalException
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.DateValue
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.PointValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues
import org.neo4j.values.virtual.VirtualValues.node
import org.neo4j.values.virtual.VirtualValues.relationship

import scala.util.Failure
import scala.util.Random
import scala.util.Success
import scala.util.Try

class SlottedRowTest extends CypherFunSuite {

  private def slots(longs: Int, refs: Int) = mutableSlots(longs, refs).build()

  private def mutableSlots(longs: Int, refs: Int) = {
    val sc = SlotConfigurationBuilder.empty
    for (i <- 1 to longs) sc.newLong(s"long$i", nullable = false, CTNode)
    for (i <- 1 to refs) sc.newReference(s"ref$i", nullable = true, CTAny)
    sc
  }

  private val nodeSlots = Seq(
    LongSlot(0, true, CTNode),
    LongSlot(0, false, CTNode),
    RefSlot(0, true, CTNode),
    RefSlot(0, false, CTNode),
    RefSlot(0, true, CTMap),
    RefSlot(0, false, CTMap),
    RefSlot(0, true, CTAny),
    RefSlot(0, false, CTAny)
  )

  private val relSlots = Seq(
    LongSlot(0, true, CTRelationship),
    LongSlot(0, false, CTRelationship),
    RefSlot(0, true, CTRelationship),
    RefSlot(0, false, CTRelationship),
    RefSlot(0, true, CTMap),
    RefSlot(0, false, CTMap),
    RefSlot(0, true, CTAny),
    RefSlot(0, false, CTAny)
  )

  private val someTypes: Seq[CypherType] = Seq(
    CTNode,
    CTRelationship,
    CTMap,
    CTAny,
    CTList(CTNode),
    CTList(CTRelationship),
    CTList(CTMap),
    CTList(CTAny),
    CTBoolean,
    CTDate,
    CTDuration,
    CTFloat,
    CTInteger,
    CTString
  )

  private val manySlots = for {
    isLong <- Seq(true, false)
    nullable <- Seq(true, false)
    cypherType <- someTypes
  } yield {
    if (isLong) LongSlot(0, nullable, cypherType)
    else RefSlot(0, nullable, cypherType)
  }

  val someValuesByType: Map[CypherType, Seq[AnyValue]] = {
    val baseValues = Map[CypherType, AnyValue](
      CTNode -> node(14),
      CTRelationship -> relationship(240),
      CTMap -> VirtualValues.map(Array("the key"), Array(Values.stringValue("the value"))),
      CTAny -> Values.pointArray(Array(PointValue.MIN_VALUE)),
      CTBoolean -> BooleanValue.TRUE,
      CTDate -> DateValue.date(2024, 10, 20),
      CTDuration -> DurationValue.duration(1, 1, 1, 1),
      CTFloat -> Values.doubleValue(25.5),
      CTInteger -> Values.intValue(431),
      CTString -> Values.stringValue("hej"),
      CTList(CTNode) -> VirtualValues.list(node(18)),
      CTList(CTRelationship) -> VirtualValues.list(relationship(182)),
      CTList(CTMap) -> VirtualValues.list(VirtualValues.map(Array("the key"), Array(Values.stringValue("the value")))),
      CTList(CTAny) -> VirtualValues.list(Values.pointArray(Array(PointValue.MIN_VALUE)))
    )
    val nulls = baseValues.view.mapValues(_ => NO_VALUE)
    val assignableValues = for {
      typeA <- someTypes
      typeB <- someTypes
      if typeA != typeB && typeA.isAssignableFrom(typeB)
    } yield {
      typeA -> baseValues(typeB)
    }
    (baseValues.toSeq ++ assignableValues ++ nulls).groupMap { case (t, _) => t } { case (_, v) => v }
  }

  test("read and write nodes") {
    for {
      slot <- nodeSlots
    } readWriteTest(slot, node(12), 12)
  }

  test("read and write relationships") {
    for {
      slot <- relSlots
    } readWriteTest(slot, relationship(12), 12)
  }

  test("merge nodes") {
    for {
      slotA <- nodeSlots
      slotB <- nodeSlots
    } mergeTest(slotA, slotB, node(14), 14)
  }

  test("merge relationships") {
    for {
      slotA <- relSlots
      slotB <- relSlots
    } mergeTest(slotA, slotB, relationship(15), 15)
  }

  test("getByName is equivalent to getter function") {
    for {
      slotTemplate <- manySlots
      value <- someValuesByType(slotTemplate.typ)
    } {
      val slots = slotsFrom(VariableSlotKey("v") -> slotTemplate)
      val slot = slots("v").slot
      def get(f: SlottedRow => Unit): SlottedRow = {
        val row = SlottedRow(slots)
        if (slot.isLongSlot) row.setLongAt(slot.offset, if (value == NO_VALUE) PRIMITIVE_NULL else 666)
        else row.setRefAt(slot.offset, value)
        f(row)
        row
      }

      val getByName = Try(get(_.getByName("v")))
      val getByGetter = Try(get(SlotConfigurationUtils.makeGetValueFromSlotFunctionFor(slot).apply))

      withClue(s"slot=$slot\nvalue=$value\n") {
        (getByName, getByGetter) match {
          case (Success(a), Success(b)) =>
            a shouldBe b
          case (Failure(a), Failure(b)) =>
            a.getMessage shouldBe b.getMessage
            a.getClass shouldBe b.getClass
          case (a, b) => fail(s"Result was not equivalent\ngetByName:$a\ngetByGetter:$b")
        }
      }
    }
  }

  test("set is equivalent to setter function") {
    for {
      slotTemplate <- manySlots
      value <- someValuesByType(slotTemplate.typ)
    } {
      val slots = slotsFrom(VariableSlotKey("v") -> slotTemplate)
      val slot = slots("v").slot
      def set(f: SlottedRow => Unit): SlottedRow = {
        val row = SlottedRow(slots)
        f(row)
        row
      }

      val setByName = Try(set(_.set("v", value)))
      val setBySetter = Try(set(makeSetValueInSlotFunctionFor(slot).apply(_, value)))

      withClue(s"slot=$slot\nvalue=$value\n") {
        (setByName, setBySetter) match {
          case (Success(a), Success(b)) =>
            a.longs shouldBe b.longs
            a.refs shouldBe b.refs
            a.getByName("v") shouldBe value
            b.getByName("v") shouldBe value
          case (Failure(a), Failure(b)) =>
            a.getMessage shouldBe b.getMessage
            a.getClass shouldBe b.getClass
          case (a, b) => fail(s"Result was not equivalent\nsetByName:$a\nsetBySetter:$b")
        }
      }
    }
  }

  test("setPrimitiveNode is equivalent to setter function") {
    for {
      slotTemplate <- manySlots
    } {
      val slots = slotsFrom(VariableSlotKey("v") -> slotTemplate)
      val slot = slots("v").slot
      def set(f: SlottedRow => Unit): SlottedRow = {
        val row = SlottedRow(slots)
        f(row)
        row
      }

      val setByName = Try(set(_.setPrimitiveNode(VariableSlotKey("v"), 234)))
      val setBySetter = Try(set(makeSetPrimitiveNodeInSlotFunctionFor(slot).apply(_, 234, Entities)))

      withClue(s"slot=$slot\nvalue=$value\n") {
        (setByName, setBySetter) match {
          case (Success(a), Success(b)) =>
            a.longs shouldBe b.longs
            a.refs shouldBe b.refs
            a.getByName("v") shouldBe b.getByName("v")
          case (Failure(a), Failure(b)) =>
            a.getMessage shouldBe b.getMessage
            a.getClass shouldBe b.getClass
          case (a, b) => fail(s"Result was not equivalent\na:$a\nb:$b")
        }
      }
    }
  }

  test("setPrimitiveRel is equivalent to setter function") {
    for {
      slotTemplate <- manySlots
    } {
      val slots = slotsFrom(VariableSlotKey("v") -> slotTemplate)
      val slot = slots("v").slot
      def set(f: SlottedRow => Unit): SlottedRow = {
        val row = SlottedRow(slots)
        f(row)
        row
      }

      val setByName = Try(set(_.setPrimitiveRel(VariableSlotKey("v"), 234)))
      val setBySetter = Try(set(makeSetPrimitiveRelationshipInSlotFunctionFor(slot).apply(_, 234, Entities)))

      withClue(s"slot=$slot\nvalue=$value\n") {
        (setByName, setBySetter) match {
          case (Success(a), Success(b)) =>
            a.longs shouldBe b.longs
            a.refs shouldBe b.refs
            a.getByName("v") shouldBe b.getByName("v")
          case (Failure(a), Failure(b)) =>
            a.getMessage shouldBe b.getMessage
            a.getClass shouldBe b.getClass
          case (a, b) => fail(s"Result was not equivalent\na:$a\nb:$b")
        }
      }
    }
  }

  test("copy fills upp the first few elements") {
    val input = SlottedRow(slots(2, 1))
    val result = SlottedRow(slots(3, 2))

    input.setLongAt(0, 42)
    input.setLongAt(1, 666)
    input.setRefAt(0, stringValue("21"))

    result.copyFrom(input, 2, 1)

    result.getLongAt(0) should equal(42)
    result.getLongAt(1) should equal(666)
    result.getRefAt(0) should equal(stringValue("21"))
  }

  test("copy fails if copy from larger") {
    val input = SlottedRow(slots(4, 0))
    val result = SlottedRow(slots(2, 0))

    intercept[InternalException](result.copyFrom(input, 4, 0))
  }

  test("copy fails if copy from larger 2") {
    val input = SlottedRow(slots(0, 4))
    val result = SlottedRow(slots(0, 2))

    intercept[InternalException](result.copyFrom(input, 0, 4))
  }

  test("can merge nullable RefSlots which are null") {
    val leftSlots = mutableSlots(0, 0).newReference("a", nullable = true, CTAny).build()
    val rightSlots = mutableSlots(0, 0).newReference("a", nullable = true, CTAny).build()
    SlottedRow(leftSlots).mergeWith(SlottedRow(rightSlots), null) // should not fail
  }

  test("mergeWith - cached properties on rhs only") {
    // given
    val slots = SlotConfigurationBuilder.empty
      .newCachedProperty(prop("n", "name"))
      .newCachedProperty(prop("n", "extra cached"))
      .build()

    val extraCachedOffset = offsetFor(prop("n", "extra cached"), slots)

    val lhsCtx = SlottedRow(slots)

    val rhsCtx = SlottedRow(slots)
    rhsCtx.setCachedProperty(prop("n", "name"), stringValue("b"))

    // when
    lhsCtx.mergeWith(rhsCtx, null)

    // then
    def cachedPropAt(key: ASTCachedProperty.RuntimeKey, ctx: CypherRow) =
      ctx.getCachedPropertyAt(offsetFor(key, slots))

    cachedPropAt(prop("n", "name"), lhsCtx) should be(stringValue("b"))
    cachedPropAt(prop("n", "name"), rhsCtx) should be(stringValue("b"))

    mutatingLeftDoesNotAffectRight(rhsCtx, lhsCtx, extraCachedOffset)
  }

  test("mergeWith() includes cached node properties") {
    // given
    val resultSlots = SlotConfigurationBuilder.empty
      .newCachedProperty(prop("a", "name"))
      .newCachedProperty(prop("b", "name"))
      .newCachedProperty(prop("b", "age"))
      .newCachedProperty(prop("c", "name"))
      .newCachedProperty(prop("c", "age"))
      .build()

    val result = SlottedRow(resultSlots)
    result.setCachedProperty(prop("a", "name"), stringValue("initial"))
    result.setCachedProperty(prop("b", "name"), stringValue("initial"))
    result.setCachedProperty(prop("b", "age"), stringValue("initial"))

    val argSlots = SlotConfigurationBuilder.empty
      .newCachedProperty(prop("b", "name"))
      .newCachedProperty(prop("c", "name"))
      .newCachedProperty(prop("c", "age"))
      .build()

    val arg = SlottedRow(argSlots)
    arg.setCachedProperty(prop("b", "name"), stringValue("arg"))
    arg.setCachedProperty(prop("c", "name"), stringValue("arg"))
    arg.setCachedProperty(prop("c", "age"), stringValue("arg"))

    // when
    result.mergeWith(arg, null)

    // then
    def cachedPropAt(key: ASTCachedProperty.RuntimeKey) =
      result.getCachedPropertyAt(resultSlots.cachedPropOffset(key))

    cachedPropAt(prop("a", "name")) should be(stringValue("initial"))
    cachedPropAt(prop("b", "name")) should be(stringValue("arg"))
    cachedPropAt(prop("b", "age")) should be(stringValue("initial"))
    cachedPropAt(prop("c", "name")) should be(stringValue("arg"))
    cachedPropAt(prop("c", "age")) should be(stringValue("arg"))
  }

  test("copyMapped - should copy all values") {
    val slots = SlotConfigurationBuilder.empty
      .newLong("long1", nullable = false, CTNode)
      .newReference("key1", nullable = false, CTAny)
      .newReference("key2", nullable = false, CTAny)
      .newReference("key3", nullable = false, CTAny)
      .newCachedProperty(prop("n", "cache1"))
      .newCachedProperty(prop("n", "cache2"))
      .newCachedProperty(prop("n", "cache3"))
      .build()

    val row = SlottedRow(slots)

    row.setLongAt(slots.longOffset("long1"), 123L)
    row.setRefAt(slots.refOffset("key1"), Values.booleanValue(false))
    row.setRefAt(slots.refOffset("key2"), Values.stringValue("x"))
    row.setRefAt(slots.refOffset("key3"), Values.doubleValue(1.2))
    row.setCachedPropertyAt(slots.cachedPropOffset(prop("n", "cache1")), Values.stringValue("abc"))

    val newRow = row.copyMapped(identity)

    newRow.getLongAt(slots.longOffset("long1")) should equal(123L)
    newRow.getRefAt(slots.refOffset("key1")) should equal(Values.booleanValue(false))
    newRow.getRefAt(slots.refOffset("key2")) should equal(Values.stringValue("x"))
    newRow.getRefAt(slots.refOffset("key3")) should equal(Values.doubleValue(1.2))
    row.getCachedPropertyAt(slots.cachedPropOffset(prop("n", "cache1"))) should equal(
      Values.stringValue("abc")
    )

    mutatingLeftDoesNotAffectRight(row, newRow, slots.cachedPropOffset(prop("n", "cache2")))
    mutatingLeftDoesNotAffectRight(newRow, row, slots.cachedPropOffset(prop("n", "cache3")))
  }

  test("copyMapped - should transform values") {
    val slots = SlotConfigurationBuilder.empty
      .newLong("long1", nullable = false, CTNode)
      .newReference("key1", nullable = false, CTAny)
      .newReference("key2", nullable = false, CTAny)
      .newReference("key3", nullable = false, CTAny)
      .newCachedProperty(prop("n", "cache1"))
      .newCachedProperty(prop("n", "cache2"))
      .newCachedProperty(prop("n", "cache3"))
      .build()

    val row = SlottedRow(slots)

    row.setLongAt(slots.longOffset("long1"), 123L)
    row.setRefAt(slots.refOffset("key1"), Values.booleanValue(false))
    row.setRefAt(slots.refOffset("key2"), Values.stringValue("x"))
    row.setRefAt(slots.refOffset("key3"), Values.doubleValue(1.2))
    row.setCachedPropertyAt(slots.cachedPropOffset(prop("n", "cache1")), Values.stringValue("abc"))

    val newRow = row.copyMapped(v => if (v != null) Values.stringValue("xyz") else null)

    newRow.getLongAt(slots.longOffset("long1")) should equal(123L)
    newRow.getRefAt(slots.refOffset("key1")) should equal(Values.stringValue("xyz"))
    newRow.getRefAt(slots.refOffset("key2")) should equal(Values.stringValue("xyz"))
    newRow.getRefAt(slots.refOffset("key3")) should equal(Values.stringValue("xyz"))
    newRow.getCachedPropertyAt(slots.cachedPropOffset(prop("n", "cache1"))) should equal(
      Values.stringValue("xyz")
    )

    mutatingLeftDoesNotAffectRight(row, newRow, slots.cachedPropOffset(prop("n", "cache2")))
    mutatingLeftDoesNotAffectRight(newRow, row, slots.cachedPropOffset(prop("n", "cache3")))
  }

  test("deduplicated estimated heap usage long slots") {
    val rows = Range(0, 3).map { i =>
      val slots =
        SlotConfigurationBuilder.empty
          .newLong("long0", nullable = false, CTNode)
          .newLong("long1", nullable = false, CTNode)
          .newLong("long2", nullable = false, CTNode)
          .newLong("long3", nullable = false, CTNode)
          .build()

      val row = SlottedRow(slots)

      row.setLongAt(0, Random.nextLong())
      row.setLongAt(1, Random.nextLong())
      row.setLongAt(2, Random.nextLong())
      row.setLongAt(3, Random.nextLong())

      row
    }

    for {
      rowA <- rows
      rowB <- rows
    } {
      val estA = rowA.estimatedHeapUsage
      rowA.deduplicatedEstimatedHeapUsage(rowB) shouldBe estA
    }
  }

  test("deduplicated estimated heap usage ref slots with no duplication") {
    val rows = Range(0, 4).map { i =>
      val slots =
        SlotConfigurationBuilder.empty
          .newReference("ref0", nullable = false, CTAny)
          .newReference("ref1", nullable = false, CTAny)
          .newReference("ref2", nullable = false, CTAny)
          .newReference("ref3", nullable = false, CTAny)
          .build()

      val row = SlottedRow(slots)

      row.setRefAt(0, stringValue("a".repeat(i)))
      row.setRefAt(1, stringValue("a".repeat(i)))
      row.setRefAt(2, stringValue("a".repeat(i)))
      row.setRefAt(3, stringValue("a".repeat(i)))

      row
    }

    for {
      rowA <- rows
      rowB <- rows
    } {
      withClue(s"$rowA compared to $rowB") {
        val est = rowA.estimatedHeapUsage
        val dedupEst = rowA.deduplicatedEstimatedHeapUsage(rowB)
        if (rowA eq rowB) {
          dedupEst shouldBe (est - rowA.refs.map(_.estimatedHeapUsage()).sum)
        } else {
          dedupEst shouldBe est
        }
      }
    }
  }

  test("deduplicated estimated heap usage ref slots with duplication") {
    val ref1 = stringValue("a")
    val ref4 = stringValue("a".repeat(128))
    val rows = Range(0, 4).map { i =>
      val slots =
        SlotConfigurationBuilder.empty
          .newReference("ref0", nullable = false, CTAny)
          .newReference("ref1", nullable = false, CTAny)
          .newReference("ref2", nullable = false, CTAny)
          .newReference("ref3", nullable = false, CTAny)
          .newReference("ref4", nullable = false, CTAny)
          .newLong("long0", nullable = false, CTNode)
          .build()

      val row = SlottedRow(slots)

      row.setRefAt(0, stringValue("aaa"))
      row.setRefAt(1, ref1)
      row.setRefAt(2, stringValue("aaa"))
      row.setRefAt(3, stringValue("aaa"))
      row.setRefAt(4, ref4)
      row.setLongAt(0, 0)

      row
    }

    for {
      rowA <- rows
      rowB <- rows
    } {
      withClue(s"$rowA compared to $rowB") {
        val est = rowA.estimatedHeapUsage
        val dedupEst = rowA.deduplicatedEstimatedHeapUsage(rowB)
        if (rowA eq rowB) {
          dedupEst shouldBe (est - rowA.refs.map(_.estimatedHeapUsage()).sum)
        } else {
          dedupEst shouldBe (est - ref1.estimatedHeapUsage() - ref4.estimatedHeapUsage())
        }
      }
    }
  }

  test("SlottedRow.toString should not explode") {

    val slotConfig = mutableSlots(2, 2)

    slotConfig.newLong("x", nullable = false, CTNode)
    slotConfig.newLong("y", nullable = false, CTNode)
    slotConfig.addAlias("x", "y")

    slotConfig.newCachedProperty(prop("n", "p"))
    slotConfig.newCachedProperty(prop("n", "p"), shouldDuplicate = true)

    slotConfig.newArgument(Id(0))
    slotConfig.newNestedArgument(Id(1))
    slotConfig.newMetaData("meta", Id(2))

    SlottedRow(slotConfig.build()).toString shouldNot be(null)
  }

  private def prop(node: String, prop: String) =
    CachedProperty(
      Variable(node)(InputPosition.NONE, Variable.isIsolatedDefault),
      Variable(node)(InputPosition.NONE, Variable.isIsolatedDefault),
      PropertyKeyName(prop)(InputPosition.NONE),
      NODE_TYPE
    )(InputPosition.NONE).runtimeKey

  private def mutatingLeftDoesNotAffectRight(left: CypherRow, right: CypherRow, extraCachedOffset: Int): Unit = {
    // given
    left should not be theSameInstanceAs(right)
    left.getCachedPropertyAt(extraCachedOffset) should equal(null)
    right.getCachedPropertyAt(extraCachedOffset) should equal(null)

    // when (left is modified)
    left.setCachedPropertyAt(extraCachedOffset, BooleanValue.FALSE)

    // then (only left should be modified)
    left.getCachedPropertyAt(extraCachedOffset) should equal(BooleanValue.FALSE)
    right.getCachedPropertyAt(extraCachedOffset) should equal(null)
  }

  private def offsetFor(key: ASTCachedProperty.RuntimeKey, slots: SlotConfiguration) =
    slots.cachedPropOffset(key)

  private def mergeTest(slotA: Slot, slotB: Slot, value: AnyValue, primitive: Long): Unit =
    withClue((slotA, slotB, value)) {
      val rows = Seq(slotA, slotB).zipWithIndex
        .map { case (slot, index) =>
          val slots = SlotConfigurationBuilder.empty
          if (index % 2 == 0) {
            slots
              .newLong("extraLong", false, CTNode)
              .newReference("extraRef", false, CTAny)
          }
          if (slot.isLongSlot) slots.newLong("x", slot.nullable, slot.typ)
          else slots.newReference("x", slot.nullable, slot.typ)
          if (index % 2 != 0) {
            slots
              .newLong("extraLong", false, CTNode)
              .newReference("extraRef", false, CTAny)
          }
          SlottedRow(slots.build())
        }
      val rowA = rows(0)
      val rowB = rows(1)

      rowA.set("x", value)
      if (!slotB.isLongSlot) rowB.isRefInitialized(0) shouldBe false
      rowB.mergeWith(rowA, null, nullCheck = false)
      rowB.getByName("x") shouldBe value
      if (slotB.isLongSlot) rowB.getLongAt(0) shouldBe primitive
      else rowB.getRefAt(0) shouldBe value
    }

  private def readWriteTest(slot: Slot, value: AnyValue, primitive: Long): Unit = withClue(slot) {
    val row = SlottedRow(slotsFrom(VariableSlotKey("x") -> slot))
    row.set("x", value)
    row.getByName("x") shouldBe value

    if (slot.isLongSlot) {
      row.getLongAt(0) shouldBe primitive
    } else {
      row.getRefAt(0) shouldBe value
    }

    if (slot.nullable) {
      row.set("x", NO_VALUE)
      row.getByName("x") shouldBe NO_VALUE
      if (slot.isLongSlot) row.getLongAt(0) shouldBe PRIMITIVE_NULL
      else row.getRefAt(0) shouldBe NO_VALUE
    } else if (slot.isLongSlot) {
      intercept[ParameterWrongTypeException](row.set("x", NO_VALUE))
    } else {
      row.set("x", NO_VALUE) // No type check on refs
    }
  }

  // Note offset might not be the same as in the input slot
  private def slotsFrom(keyValues: (VariableSlotKey, Slot)*): SlotConfiguration = {
    if (keyValues.map(_._2).exists(s => s.isLongSlot && s.typ != CTNode && s.typ != CTRelationship)) {
      SlotConfiguration(
        keyValues.map { case (k, v) => SlotWithKeyAndAliases(k, v, Set.empty) },
        keyValues.count(_._2.isLongSlot),
        keyValues.count(e => !e._2.isLongSlot)
      )
    } else {
      keyValues.sortBy(s => (s._2.isLongSlot, s._2.offset))
        .foldLeft(SlotConfigurationBuilder.empty) {
          case (slots, (VariableSlotKey(name), LongSlot(_, nullable, t))) => slots.newLong(name, nullable, t)
          case (slots, (VariableSlotKey(name), RefSlot(_, nullable, t)))  => slots.newReference(name, nullable, t)
        }
        .build()
    }
  }
}

object Entities extends EntityById {
  override def nodeById(id: Long): VirtualNodeValue = node(id)
  override def relationshipById(id: Long): VirtualRelationshipValue = relationship(id)

  override def relationshipById(id: Long, startNode: Long, endNode: Long, t: Int): VirtualRelationshipValue =
    relationship(id, startNode, endNode, t)
}
