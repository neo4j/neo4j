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
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value

trait NodeHashJoinPipeTestSupport extends CypherFunSuite {

  protected val node1 = newMockedNode(1)
  protected val node2 = newMockedNode(2)
  protected val node3 = newMockedNode(3)

  protected def prop(node: String, prop: String) =
    CachedProperty(
      Variable(node)(InputPosition.NONE),
      Variable(node)(InputPosition.NONE),
      PropertyKeyName(prop)(InputPosition.NONE),
      NODE_TYPE
    )(
      InputPosition.NONE
    )

  protected def row(values: (String, AnyValue)*) = CypherRow.from(values: _*)

  protected def rowWithCached(values: (String, AnyValue)*) = CypherRow.from(values: _*)

  case class rowWith(variables: (String, AnyValue)*) {

    def cached(cachedNodePropeties: (CachedProperty, Value)*): CypherRow = {
      val row = CypherRow.from(variables: _*)
      for ((cnp, value) <- cachedNodePropeties) row.setCachedProperty(cnp.runtimeKey, value)
      row
    }
  }

  protected def newMockedNode(id: Int) = {
    val node = mock[Node]
    when(node.getId).thenReturn(id)
    when(node.getElementId).thenReturn(id.toString)
    when(node.toString).thenReturn(s"MockedNode($id)")
    node
  }

  protected def newMockedPipe(rows: CypherRow*): Pipe = {
    val pipe = mock[Pipe]
    when(pipe.createResults(any())).thenReturn(ClosingIterator(rows.iterator))
    pipe
  }
}
