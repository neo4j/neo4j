/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.frontend.v2_3.symbols.CTNumber
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class ApplyPipeTest extends CypherFunSuite with PipeTestSupport {

  test("should work by applying the identity operator on the rhs") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)
    val rhs = pipeWithResults { (state) => Iterator(state.initialContext.get) }

    val result = ApplyPipe(lhs, rhs)()(newMonitor).createResults(QueryStateHelper.empty).toList

    result should equal(lhsData)
  }

  test("should work by applying a  on the rhs") {
    val lhsData = List(Map("a" -> 1, "b" -> 3), Map("a" -> 2, "b" -> 4))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber, "b" -> CTNumber)
    val rhsData = "c" -> 36
    val rhs = pipeWithResults { (state) => Iterator(ExecutionContext.empty += rhsData) }

    val result = ApplyPipe(lhs, rhs)()(newMonitor).createResults(QueryStateHelper.empty).toList

    result should equal(lhsData.map(_ + rhsData))
  }

  test("should work even if inner pipe overwrites values") {
    val lhsData = List(Map("a" -> 1, "b" -> 3), Map("a" -> 2, "b" -> 4))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber, "b" -> CTNumber)
    val rhs = pipeWithResults { (state) => Iterator(state.initialContext.get += "b" -> null) }

    val result = ApplyPipe(lhs, rhs)()(newMonitor).createResults(QueryStateHelper.empty).toList

    result should equal(lhsData)
  }
}
