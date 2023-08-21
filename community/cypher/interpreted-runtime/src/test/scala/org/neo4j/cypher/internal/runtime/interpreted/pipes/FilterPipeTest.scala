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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.True
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class FilterPipeTest extends CypherFunSuite {

  test("should be lazy") {
    val input = new FakePipe(Seq(Map("a" -> 10), Map("a" -> 11), Map("a" -> 12), Map("a" -> 13)))
    val pipe = FilterPipe(input, True())()
    // when
    val res = pipe.createResults(QueryStateHelper.emptyWithValueSerialization)
    res.next()
    // then
    input.numberOfPulledRows shouldBe 1
  }
}
