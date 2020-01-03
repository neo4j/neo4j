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

import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.runtime.interpreted.ImplicitDummyPos
import org.neo4j.cypher.internal.runtime.{NodeValueHit, QueryContext}
import org.neo4j.cypher.internal.v3_5.logical.plans.CachedNodeProperty
import org.neo4j.internal.kernel.api.{IndexQuery, NodeCursor, NodeValueIndexCursor}
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.storable.{Value, Values}
import org.neo4j.values.virtual.{NodeValue, VirtualNodeValue, VirtualValues}
import org.neo4j.cypher.internal.v3_5.expressions.{PropertyKeyName, PropertyKeyToken}
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

trait IndexMockingHelp extends CypherFunSuite with ImplicitDummyPos {

  def propertyKeys: Seq[PropertyKeyToken]

  protected def indexFor[T](values: (Seq[AnyRef], Iterable[NodeValueHit])*): QueryContext = {
    val query: QueryContext = mockedQueryContext
    when(query.indexSeek(any(), any(), any(), any())).thenReturn(PredefinedCursor())
    when(query.lockingUniqueIndexSeek(any(), any())).thenReturn(PredefinedCursor())

    values.foreach {
      case (searchTerm, resultIterable) =>
        val indexQueries = propertyKeys.zip(searchTerm).map(t => IndexQuery.exact(t._1.nameId.id, t._2))
        when(query.indexSeek(any(), any(), any(), ArgumentMatchers.eq(indexQueries))).thenReturn(PredefinedCursor(resultIterable))
        when(query.lockingUniqueIndexSeek(any(), ArgumentMatchers.eq(indexQueries))).thenReturn(PredefinedCursor(resultIterable))
    }

    query
  }

  protected def stringIndexFor(values: (String, Iterable[NodeValueHit])*): QueryContext = {
    val query = mockedQueryContext
    when(query.indexSeek(any(), any(), any(), any())).thenReturn(PredefinedCursor())
    when(query.lockingUniqueIndexSeek(any(), any())).thenReturn(PredefinedCursor())
    values.foreach {
      case (searchTerm, resultIterable) =>
        when(query.indexSeekByContains(any(), any(), any(), ArgumentMatchers.eq(stringValue(searchTerm)))).thenReturn(PredefinedCursor(resultIterable))
        when(query.indexSeekByEndsWith(any(), any(), any(), ArgumentMatchers.eq(stringValue(searchTerm)))).thenReturn(PredefinedCursor(resultIterable))
    }

    query
  }

  protected def scanFor(nodes: Iterable[NodeValueHit]): QueryContext = {
    val query = mockedQueryContext
    when(query.indexScan(any(), any(), any())).thenReturn(PredefinedCursor(nodes))
    query
  }

  protected def nodeValueHit(nodeValue: VirtualNodeValue, values: Object*): NodeValueHit =
    new NodeValueHit(nodeValue.id, values.map(Values.of).toArray)

  protected def cachedNodeProperty(node: String, property: PropertyKeyToken): CachedNodeProperty =
    CachedNodeProperty("n", PropertyKeyName(property.name)(pos))(pos)

  private def mockedQueryContext[T] = {
    val query = mock[QueryContext]
    when(query.nodeById(any())).thenAnswer(new Answer[NodeValue] {
      override def answer(invocationOnMock: InvocationOnMock): NodeValue =
        VirtualValues.nodeValue(invocationOnMock.getArgument(0), Values.EMPTY_TEXT_ARRAY, VirtualValues.EMPTY_MAP)
    })
    query
  }

  case class PredefinedCursor[T](nodeValueHits: Iterable[NodeValueHit] = Nil) extends NodeValueIndexCursor {

    private var iter = nodeValueHits.iterator
    private var current: NodeValueHit = _

    override def numberOfProperties(): Int = current.numberOfProperties()

    override def propertyKey(offset: Int): Int = current.propertyKey(offset)

    override def hasValue: Boolean = current.hasValue

    override def propertyValue(offset: Int): Value = current.propertyValue(offset)

    override def node(cursor: NodeCursor): Unit = current.node(cursor)

    override def nodeReference(): Long = current.nodeReference()

    override def next(): Boolean = {
      if (iter.hasNext) {
        current = iter.next()
        true
      } else {
        current = null
        false
      }
    }

    override def close(): Unit = {}

    override def isClosed: Boolean = current != null
  }
}
