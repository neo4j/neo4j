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
package org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.intValue
import org.neo4j.values.virtual.VirtualValues.EMPTY_LIST
import org.neo4j.values.virtual.VirtualValues.list

class CollectAllFunctionTest extends CypherFunSuite with AggregateTest {

  def createAggregator(inner: Expression) = new CollectAllFunction(inner, EmptyMemoryTracker.INSTANCE)

  test("collect single value") {
    aggregateOn(intValue(1)) should equal(list(intValue(1)))
  }

  test("collect no values") {
    aggregateOn() should equal(EMPTY_LIST)
  }

  test("collect null value") {
    aggregateOn(NO_VALUE) should equal(list(NO_VALUE))
  }

  test("collect multiple values") {
    aggregateOn(intValue(1), NO_VALUE, intValue(2)) should equal(list(intValue(1), NO_VALUE, intValue(2)))
  }
}
