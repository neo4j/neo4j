/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class UnionPipeTest extends CypherFunSuite {
  test("Close should close RHS and LHS.") {
    val lhs = FakePipe(Seq(Map("a" -> 10), Map("a" -> 11)))
    val rhs = FakePipe(Seq(Map("b" -> 20), Map("b" -> 21)))
    val pipe = UnionPipe(lhs, rhs)()
    val result = pipe.createResults(QueryStateHelper.empty)
    result.next()
    result.close()

    lhs.wasClosed shouldBe true
    rhs.wasClosed shouldBe true
  }
}
