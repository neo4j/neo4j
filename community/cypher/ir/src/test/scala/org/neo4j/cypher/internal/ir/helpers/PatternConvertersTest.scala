/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.ir.helpers

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.FixedQuantifier
import org.neo4j.cypher.internal.expressions.IntervalQuantifier
import org.neo4j.cypher.internal.expressions.PlusQuantifier
import org.neo4j.cypher.internal.expressions.StarQuantifier
import org.neo4j.cypher.internal.ir.helpers.PatternConverters.GraphPatternQuantifierToRepetitionConverter
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PatternConvertersTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should convert GraphPatternQuantifier to Repetition") {
    StarQuantifier()(pos).toRepetition shouldBe
      Repetition(min = 0, max = UpperBound.unlimited)

    PlusQuantifier()(pos).toRepetition shouldBe
      Repetition(min = 1, max = UpperBound.unlimited)

    FixedQuantifier(literalUnsignedInt(123))(pos).toRepetition shouldBe
      Repetition(min = 123, max = UpperBound.Limited(123))

    IntervalQuantifier(None, Some(literalUnsignedInt(123)))(pos).toRepetition shouldBe
      Repetition(min = 0, max = UpperBound.Limited(123))
    IntervalQuantifier(Some(literalUnsignedInt(123)), None)(pos).toRepetition shouldBe
      Repetition(min = 123, max = UpperBound.unlimited)
  }
}
