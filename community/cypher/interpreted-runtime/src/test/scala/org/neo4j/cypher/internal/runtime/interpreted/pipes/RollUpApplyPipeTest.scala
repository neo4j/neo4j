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

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class RollUpApplyPipeTest extends CypherFunSuite with PipeTestSupport {

  test("should set the QueryState when calling down to the RHS") {
    // given
    val lhs = createLhs(1)
    val rhs = mock[Pipe]
    when(rhs.createResults(any())).thenAnswer((invocation: InvocationOnMock) => {
      val state: QueryState = invocation.getArgument(0)
      state.initialContext should not be empty
      ClosingIterator.empty
    })
    val pipe = RollUpApplyPipe(lhs, rhs, collectionName = "x", identifierToCollect = "y")()

    // when
    pipe.createResults(QueryStateHelper.empty).toList

    // then should not throw exception
  }

  private def createLhs(data: Any*) = {
    val lhsData = data.map(v => Map("a" -> v))
    new FakePipe(lhsData.iterator)
  }
}
