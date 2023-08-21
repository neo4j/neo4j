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
package org.neo4j.cypher.internal.ir.converters

import org.neo4j.cypher.internal.expressions.FixedQuantifier
import org.neo4j.cypher.internal.expressions.IntervalQuantifier
import org.neo4j.cypher.internal.expressions.PlusQuantifier
import org.neo4j.cypher.internal.expressions.StarQuantifier
import org.neo4j.cypher.internal.expressions.UnsignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.UnsignedIntegerLiteral
import org.neo4j.cypher.internal.ir.converters.QuantifiedPathPatternConverters.convertGraphPatternQuantifier
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class QuantifiedPathPatternConvertersTest extends CypherFunSuite {

  final private val position: InputPosition = InputPosition.NONE
  final private val literal123: UnsignedIntegerLiteral = UnsignedDecimalIntegerLiteral("123")(position)

  test("should convert * quantifier to 0 or more repetitions") {
    val quantifier = StarQuantifier()(position)
    convertGraphPatternQuantifier(quantifier) shouldEqual Repetition(min = 0, max = UpperBound.unlimited)
  }

  test("should convert + quantifier to 1 or more repetitions") {
    val quantifier = PlusQuantifier()(position)
    convertGraphPatternQuantifier(quantifier) shouldEqual Repetition(min = 1, max = UpperBound.unlimited)
  }

  test("should convert {123} quantifier to exactly 123 repetitions") {
    val quantifier = FixedQuantifier(literal123)(position)
    convertGraphPatternQuantifier(quantifier) shouldEqual Repetition(min = 123, max = UpperBound.Limited(123))
  }

  test("should convert {, 123} quantifier to between 0 and 123 repetitions") {
    val quantifier = IntervalQuantifier(None, Some(literal123))(position)
    convertGraphPatternQuantifier(quantifier) shouldEqual Repetition(min = 0, max = UpperBound.Limited(123))
  }

  test("should convert {123, } quantifier to 123 or more repetitions") {
    val quantifier = IntervalQuantifier(Some(literal123), None)(position)
    convertGraphPatternQuantifier(quantifier) shouldEqual Repetition(min = 123, max = UpperBound.unlimited)
  }
}
