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

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.v3_5.logical.plans.CachedNodeProperty
import org.neo4j.graphdb.Node
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value
import org.neo4j.cypher.internal.v3_5.expressions.PropertyKeyName
import org.neo4j.cypher.internal.v3_5.util.InputPosition
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

trait NodeHashJoinPipeTestSupport extends CypherFunSuite {

  protected val node1 = newMockedNode(1)
  protected val node2 = newMockedNode(2)
  protected val node3 = newMockedNode(3)

  protected def prop(node: String, prop: String) =
    CachedNodeProperty(node, PropertyKeyName(prop)(InputPosition.NONE))(InputPosition.NONE)

  protected def row(values: (String, AnyValue)*) = ExecutionContext.from(values: _*)

  protected def rowWithCached(values: (String, AnyValue)*) = ExecutionContext.from(values: _*)

  case class rowWith(variables: (String, AnyValue)*) {
    def cached(cachedNodePropeties: (CachedNodeProperty, Value)*): ExecutionContext = {
      val row = ExecutionContext.from(variables: _*)
      for ((cnp, value) <- cachedNodePropeties) row.setCachedProperty(cnp, value)
      row
    }
  }

  protected def newMockedNode(id: Int) = {
    val node = mock[Node]
    when(node.getId).thenReturn(id)
    when(node.toString).thenReturn(s"MockedNode($id)")
    node
  }

  protected def newMockedPipe(rows: ExecutionContext*): Pipe = {
    val pipe = mock[Pipe]
    when(pipe.createResults(any())).thenReturn(rows.iterator)
    pipe
  }
}
