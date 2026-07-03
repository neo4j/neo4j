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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.SlotWithKeyAndAliases
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.VariableSlotKey
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

trait SlottedPipeTestHelper extends CypherFunSuite {

  def testableResult(rows: ClosingIterator[CypherRow], slots: SlotConfiguration): List[Map[String, Any]] = {
    rows
      .map { in =>
        slots.legacyView.keyedSlots
          .flatMap {
            case SlotWithKeyAndAliases(VariableSlotKey(column), LongSlot(offset, _, _), aliases) =>
              Seq((column, in.getLongAt(offset))).appendedAll(aliases.map(alias => alias -> in.getLongAt(offset)))
            case SlotWithKeyAndAliases(VariableSlotKey(column), RefSlot(offset, _, _), aliases) =>
              Seq((column, in.getRefAt(offset))).appendedAll(aliases.map(alias => alias -> in.getRefAt(offset)))
            case _ => Seq.empty // no help here
          }
          .toMap
      }
      .toList
  }
}
