/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, ImplicitDummyPos}
import org.neo4j.cypher.internal.runtime.{NodeValueHit, QueryContext, ResultCreator}
import org.neo4j.internal.kernel.api.IndexQuery
import org.neo4j.values.storable.{Value, Values}
import org.neo4j.values.virtual.{VirtualNodeValue, VirtualValues}
import org.opencypher.v9_0.expressions.PropertyKeyToken
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

trait IndexMockingHelp extends CypherFunSuite with ImplicitDummyPos {

  def propertyKeys: Seq[PropertyKeyToken]

  protected def indexFor(values: (Seq[AnyRef], Iterable[NodeValueHit])*): QueryContext = {
    val query = mock[QueryContext]
    when(query.indexSeek(any(), any(), any(), any())).thenReturn(Iterator.empty)
    when(query.lockingUniqueIndexSeek(any(), any(), any())).thenReturn(None)

    values.foreach {
      case (searchTerm, resultIterable) =>
        val indexQueries = propertyKeys.zip(searchTerm).map(t => IndexQuery.exact(t._1.nameId.id, t._2))
        when(query.indexSeek(any(), any(), any(), ArgumentMatchers.eq(indexQueries))).thenAnswer(PredefinedIterator(resultIterable))
        when(query.lockingUniqueIndexSeek(any(), any(), ArgumentMatchers.eq(indexQueries))).thenAnswer(PredefinedOption(resultIterable))
    }

    query
  }

  protected def stringIndexFor(values: (String, Iterable[NodeValueHit])*): QueryContext = {
    val query = mock[QueryContext]
    when(query.indexSeek(any(), any(), any(), any())).thenReturn(Iterator.empty)
    when(query.lockingUniqueIndexSeek(any(), any(), any())).thenReturn(None)

    values.foreach {
      case (searchTerm, resultIterable) =>
        when(query.indexSeekByContains(any(), any(), any(), ArgumentMatchers.eq(searchTerm))).thenAnswer(PredefinedIterator(resultIterable))
        when(query.indexSeekByEndsWith(any(), any(), any(), ArgumentMatchers.eq(searchTerm))).thenAnswer(PredefinedIterator(resultIterable))
    }

    query
  }

  protected def scanFor(nodes: Iterable[TestNodeValueHit]): QueryContext = {
    val query = mock[QueryContext]
    when(query.indexScan(any(), any(), any())).thenAnswer(PredefinedIterator(nodes))
    query
  }

  protected def nodeValueHit(nodeValue: VirtualNodeValue, values: Object*): TestNodeValueHit =
    TestNodeValueHit(nodeValue.id, values.map(Values.of).toArray)

  case class TestNodeValueHit(nodeId: Long, values: Array[Value]) extends NodeValueHit {
    override def node: VirtualNodeValue = VirtualValues.node(nodeId)
    override def numberOfProperties: Int = values.size
    override def propertyValue(i: Int): Value = values(i)
  }

  case class PredefinedIterator(nodeValueHits: Iterable[NodeValueHit]) extends Answer[Iterator[ExecutionContext]] {
    override def answer(invocationOnMock: InvocationOnMock): Iterator[ExecutionContext] = {
      val resultCreator = invocationOnMock.getArgument[ResultCreator[ExecutionContext]](2)
      nodeValueHits.iterator.map(resultCreator.createResult)
    }
  }

  case class PredefinedOption(nodeValueHits: Iterable[NodeValueHit]) extends Answer[Option[ExecutionContext]] {
    override def answer(invocationOnMock: InvocationOnMock): Option[ExecutionContext] = {
      val resultCreator = invocationOnMock.getArgument[ResultCreator[ExecutionContext]](2)
      nodeValueHits.headOption.map(resultCreator.createResult)
    }
  }
}
