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

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.KeyedSlot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.VariableSlotKey
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class KeyedSlotTest extends CypherFunSuite {

  test("slot type") {
    for {
      nullable <- Seq(true, false)
      offset <- Seq(0, 17)
      cypherType <- Seq(
        symbols.CTAny,
        symbols.CTBoolean,
        symbols.CTString,
        symbols.CTStringNotNull,
        symbols.CTNumber,
        symbols.CTFloat,
        symbols.CTInteger,
        symbols.CTMap,
        symbols.CTNode,
        symbols.CTRelationship,
        symbols.CTPoint,
        symbols.CTPointNotNull,
        symbols.CTDateTime,
        symbols.CTLocalDateTime,
        symbols.CTDate,
        symbols.CTTime,
        symbols.CTLocalTime,
        symbols.CTDuration,
        symbols.CTGeometry,
        symbols.CTPath,
        symbols.CTGraphRef,
        symbols.CTList(CTNode),
        symbols.CTList(CTRelationship),
        symbols.CTList(CTAny),
        symbols.CTList(CTMap),
        symbols.CTList(symbols.CTList(CTNode)),
        symbols.CTList(symbols.CTList(CTRelationship))
      )
    } {
      check(LongSlot(offset, nullable, cypherType))
      check(RefSlot(offset, nullable, cypherType))
    }
  }

  private def check(slot: Slot): Unit = {
    val keyedSlot = KeyedSlot(VariableSlotKey("x"), slot, IndexedSeq.empty)
    withClue(keyedSlot) {
      keyedSlot.slot shouldBe slot
      keyedSlot.isLongSlot shouldBe slot.isLongSlot
      SlotType.isValidSlotType(keyedSlot.slotType) shouldBe true
      SlotType.isLongSlot(keyedSlot.slotType) shouldBe slot.isLongSlot
      SlotType.isRefSlot(keyedSlot.slotType) shouldBe !slot.isLongSlot

      keyedSlot.slotType match {
        case SlotType.NodeNonNullLongSlot =>
          slot.nullable shouldBe false
          slot.isLongSlot shouldBe true
          slot.typ shouldBe CTNode
        case SlotType.NodeNullableLongSlot =>
          slot.nullable shouldBe true
          slot.isLongSlot shouldBe true
          slot.typ shouldBe CTNode
        case SlotType.RelNonNullLongSlot =>
          slot.nullable shouldBe false
          slot.isLongSlot shouldBe true
          slot.typ shouldBe CTRelationship
        case SlotType.RelNullableLongSlot =>
          slot.nullable shouldBe true
          slot.isLongSlot shouldBe true
          slot.typ shouldBe CTRelationship
        case SlotType.OtherNonNullLongSlot =>
          slot.nullable shouldBe false
          slot.isLongSlot shouldBe true
          slot.typ should not be CTRelationship
          slot.typ should not be CTNode
        case SlotType.OtherNullableLongSlot =>
          slot.nullable shouldBe true
          slot.isLongSlot shouldBe true
          slot.typ should not be CTRelationship
          slot.typ should not be CTNode
        case SlotType.NodeNonNullRefSlot =>
          slot.nullable shouldBe false
          slot.isLongSlot shouldBe false
          slot.typ shouldBe CTNode
        case SlotType.NodeNullableRefSlot =>
          slot.nullable shouldBe true
          slot.isLongSlot shouldBe false
          slot.typ shouldBe CTNode
        case SlotType.RelNonNullRefSlot =>
          slot.nullable shouldBe false
          slot.isLongSlot shouldBe false
          slot.typ shouldBe CTRelationship
        case SlotType.RelNullableRefSlot =>
          slot.nullable shouldBe true
          slot.isLongSlot shouldBe false
          slot.typ shouldBe CTRelationship
        case SlotType.OtherNonNullRefSlot =>
          slot.nullable shouldBe false
          slot.isLongSlot shouldBe false
          slot.typ should not be CTRelationship
          slot.typ should not be CTNode
        case SlotType.OtherNullableRefSlot =>
          slot.nullable shouldBe true
          slot.isLongSlot shouldBe false
          slot.typ should not be CTRelationship
          slot.typ should not be CTNode
      }
    }
  }
}
