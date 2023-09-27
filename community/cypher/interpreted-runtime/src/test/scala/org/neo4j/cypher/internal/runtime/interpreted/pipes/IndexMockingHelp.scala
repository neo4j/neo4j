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

import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.RETURNS_DEEP_STUBS
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.runtime.NodeValueHit
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.ImplicitDummyPos
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.DefaultCloseListenable
import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.kernel.api.PropertyIndexQuery
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualValues
import org.neo4j.values.virtual.VirtualValues.nodeValue

trait IndexMockingHelp extends CypherFunSuite with ImplicitDummyPos {

  def propertyKeys: Seq[PropertyKeyToken]

  protected def indexFor[T](values: (Seq[AnyRef], Iterable[NodeValueHit])*): QueryContext = {
    val query: QueryContext = mockedQueryContext
    when(query.nodeIndexSeek(any(), any(), any(), any())).thenReturn(PredefinedCursor())
    when(query.nodeLockingUniqueIndexSeek(any(), any())).thenReturn(PredefinedCursor())

    values.foreach {
      case (searchTerm, resultIterable) =>
        val indexQueries = propertyKeys.zip(searchTerm).map(t => PropertyIndexQuery.exact(t._1.nameId.id, t._2))
        when(query.nodeIndexSeek(any(), any(), any(), ArgumentMatchers.eq(indexQueries))).thenReturn(PredefinedCursor(
          resultIterable
        ))
        when(query.nodeLockingUniqueIndexSeek(any(), ArgumentMatchers.eq(indexQueries))).thenReturn(PredefinedCursor(
          resultIterable
        ))
    }

    query
  }

  protected def stringIndexFor(values: (String, Iterable[NodeValueHit])*): QueryContext = {
    val query = mockedQueryContext
    when(query.nodeIndexSeek(any(), any(), any(), any())).thenReturn(PredefinedCursor())
    when(query.nodeLockingUniqueIndexSeek(any(), any())).thenReturn(PredefinedCursor())
    values.foreach {
      case (searchTerm, resultIterable) =>
        when(
          query.nodeIndexSeekByContains(any(), any(), any(), ArgumentMatchers.eq(stringValue(searchTerm)))
        ).thenReturn(PredefinedCursor(resultIterable))
        when(
          query.nodeIndexSeekByEndsWith(any(), any(), any(), ArgumentMatchers.eq(stringValue(searchTerm)))
        ).thenReturn(PredefinedCursor(resultIterable))
    }

    query
  }

  protected def scanFor(nodes: Iterable[NodeValueHit]): QueryContext = {
    val query = mockedQueryContext
    when(query.nodeIndexScan(any(), any(), any())).thenReturn(PredefinedCursor(nodes))
    query
  }

  protected def nodeValueHit(nodeValue: VirtualNodeValue, values: Object*): NodeValueHit =
    new NodeValueHit(nodeValue.id, values.map(Values.of).toArray, null)

  protected def cachedProperty(node: String, property: PropertyKeyToken): CachedProperty =
    CachedProperty(Variable("n")(pos), Variable(node)(pos), PropertyKeyName(property.name)(pos), NODE_TYPE)(pos)

  private def mockedQueryContext[T] = {
    val query = mock[QueryContext](RETURNS_DEEP_STUBS)
    when(query.nodeById(any())).thenAnswer(new Answer[NodeValue] {
      override def answer(invocationOnMock: InvocationOnMock): NodeValue =
        nodeValue(invocationOnMock.getArgument(0), "n", Values.EMPTY_TEXT_ARRAY, VirtualValues.EMPTY_MAP)
    })
    query
  }

  case class PredefinedCursor[T](nodeValueHits: Iterable[NodeValueHit] = Nil) extends DefaultCloseListenable
      with NodeValueIndexCursor {

    private val iter = nodeValueHits.iterator
    private var current: NodeValueHit = _

    override def numberOfProperties(): Int = current.numberOfProperties()

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

    override def closeInternal(): Unit = {}

    override def isClosed: Boolean = current != null

    override def score(): Float = Float.NaN

    override def setTracer(tracer: KernelReadTracer): Unit = throw new UnsupportedOperationException("not implemented")

    override def removeTracer(): Unit = throw new UnsupportedOperationException("not implemented")
  }
}
