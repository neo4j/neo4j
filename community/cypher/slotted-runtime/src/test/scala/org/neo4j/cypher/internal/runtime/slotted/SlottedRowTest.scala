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
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.InternalException
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.stringValue

import scala.util.Random

class SlottedRowTest extends CypherFunSuite {

  private def slots(longs: Int, refs: Int) = {
    val sc = SlotConfiguration.empty
    for (i <- 1 to longs) sc.newLong(s"long$i", nullable = false, CTNode)
    for (i <- 1 to refs) sc.newReference(s"ref$i", nullable = true, CTAny)
    sc
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
    val leftSlots = slots(0, 0).newReference("a", nullable = true, CTAny)
    SlotConfigurationUtils.generateSlotAccessorFunctions(leftSlots)
    val rightSlots = slots(0, 0).newReference("a", nullable = true, CTAny)
    SlottedRow(leftSlots).mergeWith(SlottedRow(rightSlots), null) // should not fail
  }

  test("mergeWith - cached properties on rhs only") {
    // given
    val slots =
      SlotConfiguration.empty
        .newCachedProperty(prop("n", "name"))
        .newCachedProperty(prop("n", "extra cached"))

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
    val resultSlots =
      SlotConfiguration.empty
        .newCachedProperty(prop("a", "name"))
        .newCachedProperty(prop("b", "name"))
        .newCachedProperty(prop("b", "age"))
        .newCachedProperty(prop("c", "name"))
        .newCachedProperty(prop("c", "age"))

    val result = SlottedRow(resultSlots)
    result.setCachedProperty(prop("a", "name"), stringValue("initial"))
    result.setCachedProperty(prop("b", "name"), stringValue("initial"))
    result.setCachedProperty(prop("b", "age"), stringValue("initial"))

    val argSlots =
      SlotConfiguration.empty
        .newCachedProperty(prop("b", "name"))
        .newCachedProperty(prop("c", "name"))
        .newCachedProperty(prop("c", "age"))

    val arg = SlottedRow(argSlots)
    arg.setCachedProperty(prop("b", "name"), stringValue("arg"))
    arg.setCachedProperty(prop("c", "name"), stringValue("arg"))
    arg.setCachedProperty(prop("c", "age"), stringValue("arg"))

    // when
    result.mergeWith(arg, null)

    // then
    def cachedPropAt(key: ASTCachedProperty.RuntimeKey) =
      result.getCachedPropertyAt(resultSlots.getCachedPropertyOffsetFor(key))

    cachedPropAt(prop("a", "name")) should be(stringValue("initial"))
    cachedPropAt(prop("b", "name")) should be(stringValue("arg"))
    cachedPropAt(prop("b", "age")) should be(stringValue("initial"))
    cachedPropAt(prop("c", "name")) should be(stringValue("arg"))
    cachedPropAt(prop("c", "age")) should be(stringValue("arg"))
  }

  test("copyMapped - should copy all values") {
    val slots =
      SlotConfiguration.empty
        .newLong("long1", nullable = false, CTNode)
        .newReference("key1", nullable = false, CTAny)
        .newReference("key2", nullable = false, CTAny)
        .newReference("key3", nullable = false, CTAny)
        .newCachedProperty(prop("n", "cache1"))
        .newCachedProperty(prop("n", "cache2"))
        .newCachedProperty(prop("n", "cache3"))

    val row = SlottedRow(slots)

    row.setLongAt(slots.getLongOffsetFor("long1"), 123L)
    row.setRefAt(slots.getReferenceOffsetFor("key1"), Values.booleanValue(false))
    row.setRefAt(slots.getReferenceOffsetFor("key2"), Values.stringValue("x"))
    row.setRefAt(slots.getReferenceOffsetFor("key3"), Values.doubleValue(1.2))
    row.setCachedPropertyAt(slots.getCachedPropertyOffsetFor(prop("n", "cache1")), Values.stringValue("abc"))

    val newRow = row.copyMapped(identity)

    newRow.getLongAt(slots.getLongOffsetFor("long1")) should equal(123L)
    newRow.getRefAt(slots.getReferenceOffsetFor("key1")) should equal(Values.booleanValue(false))
    newRow.getRefAt(slots.getReferenceOffsetFor("key2")) should equal(Values.stringValue("x"))
    newRow.getRefAt(slots.getReferenceOffsetFor("key3")) should equal(Values.doubleValue(1.2))
    row.getCachedPropertyAt(slots.getCachedPropertyOffsetFor(prop("n", "cache1"))) should equal(
      Values.stringValue("abc")
    )

    mutatingLeftDoesNotAffectRight(row, newRow, slots.getCachedPropertyOffsetFor(prop("n", "cache2")))
    mutatingLeftDoesNotAffectRight(newRow, row, slots.getCachedPropertyOffsetFor(prop("n", "cache3")))
  }

  test("copyMapped - should transform values") {
    val slots =
      SlotConfiguration.empty
        .newLong("long1", nullable = false, CTNode)
        .newReference("key1", nullable = false, CTAny)
        .newReference("key2", nullable = false, CTAny)
        .newReference("key3", nullable = false, CTAny)
        .newCachedProperty(prop("n", "cache1"))
        .newCachedProperty(prop("n", "cache2"))
        .newCachedProperty(prop("n", "cache3"))

    val row = SlottedRow(slots)

    row.setLongAt(slots.getLongOffsetFor("long1"), 123L)
    row.setRefAt(slots.getReferenceOffsetFor("key1"), Values.booleanValue(false))
    row.setRefAt(slots.getReferenceOffsetFor("key2"), Values.stringValue("x"))
    row.setRefAt(slots.getReferenceOffsetFor("key3"), Values.doubleValue(1.2))
    row.setCachedPropertyAt(slots.getCachedPropertyOffsetFor(prop("n", "cache1")), Values.stringValue("abc"))

    val newRow = row.copyMapped(v => if (v != null) Values.stringValue("xyz") else null)

    newRow.getLongAt(slots.getLongOffsetFor("long1")) should equal(123L)
    newRow.getRefAt(slots.getReferenceOffsetFor("key1")) should equal(Values.stringValue("xyz"))
    newRow.getRefAt(slots.getReferenceOffsetFor("key2")) should equal(Values.stringValue("xyz"))
    newRow.getRefAt(slots.getReferenceOffsetFor("key3")) should equal(Values.stringValue("xyz"))
    newRow.getCachedPropertyAt(slots.getCachedPropertyOffsetFor(prop("n", "cache1"))) should equal(
      Values.stringValue("xyz")
    )

    mutatingLeftDoesNotAffectRight(row, newRow, slots.getCachedPropertyOffsetFor(prop("n", "cache2")))
    mutatingLeftDoesNotAffectRight(newRow, row, slots.getCachedPropertyOffsetFor(prop("n", "cache3")))
  }

  test("deduplicated estimated heap usage long slots") {
    val rows = Range(0, 3).map { i =>
      val slots =
        SlotConfiguration.empty
          .newLong("long0", nullable = false, CTNode)
          .newLong("long1", nullable = false, CTNode)
          .newLong("long2", nullable = false, CTNode)
          .newLong("long3", nullable = false, CTNode)

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
        SlotConfiguration.empty
          .newReference("ref0", nullable = false, CTAny)
          .newReference("ref1", nullable = false, CTAny)
          .newReference("ref2", nullable = false, CTAny)
          .newReference("ref3", nullable = false, CTAny)

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
        SlotConfiguration.empty
          .newReference("ref0", nullable = false, CTAny)
          .newReference("ref1", nullable = false, CTAny)
          .newReference("ref2", nullable = false, CTAny)
          .newReference("ref3", nullable = false, CTAny)
          .newReference("ref4", nullable = false, CTAny)
          .newLong("long0", nullable = false, CTNode)

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

    val slotConfig = slots(2, 2)

    slotConfig.newLong("x", nullable = false, CTNode)
    slotConfig.newLong("y", nullable = false, CTNode)
    slotConfig.addAlias("x", "y")

    slotConfig.newCachedProperty(prop("n", "p"))
    slotConfig.newCachedProperty(prop("n", "p"), shouldDuplicate = true)

    slotConfig.newArgument(Id(0))
    slotConfig.newNestedArgument(Id(1))
    slotConfig.newMetaData("meta", Id(2))

    SlottedRow(slotConfig).toString shouldNot be(null)
  }

  private def prop(node: String, prop: String) =
    CachedProperty(
      Variable(node)(InputPosition.NONE),
      Variable(node)(InputPosition.NONE),
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
    slots.getCachedPropertyOffsetFor(key)
}
