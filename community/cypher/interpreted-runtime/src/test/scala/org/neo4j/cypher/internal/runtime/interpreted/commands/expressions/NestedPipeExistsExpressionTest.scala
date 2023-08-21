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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.FakePipe
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values

class NestedPipeExistsExpressionTest extends CypherFunSuite {

  test("Should not pull. Should close pipe results.") {
    // given
    val input = new FakePipe(Seq(Map("a" -> 10), Map("a" -> 11)))
    val npee = NestedPipeExistsExpression(input, Array(), Id(0))
    // when
    npee.apply(CypherRow.from("x" -> Values.intValue(42)), QueryStateHelper.empty)
    // then
    input.wasClosed shouldBe true
    input.numberOfPulledRows shouldBe 0
  }
}
