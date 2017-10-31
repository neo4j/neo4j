/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.{ArgumentMatchers, Mockito}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.graphdb.spatial.Point
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.kernel.impl.util.BaseToObjectValueWriter
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP

import scala.collection.mutable

object QueryStateHelper {
  def empty: QueryState = emptyWith()

  def emptyWith(query: QueryContext = null, resources: ExternalCSVResource = null,
                params: MapValue = EMPTY_MAP, decorator: PipeDecorator = NullPipeDecorator,
                initialContext: Option[ExecutionContext] = None) =
    new QueryState(query = query, resources = resources, params = params, decorator = decorator,
      initialContext = initialContext, triadicState = mutable.Map.empty, repeatableReads = mutable.Map.empty)

  def emptyWithValueSerialization: QueryState = emptyWith(query = context)

  private val context = Mockito.mock(classOf[QueryContext])
  Mockito.when(context.asObject(ArgumentMatchers.any())).thenAnswer(new Answer[Any] {
    override def answer(invocationOnMock: InvocationOnMock): AnyRef = toObject(invocationOnMock.getArgument(0))
  })

  private def toObject(any: AnyValue) = {
    val writer = new BaseToObjectValueWriter[RuntimeException] {
      override protected def newNodeProxyById(id: Long): Node = ???
      override protected def newRelationshipProxyById(id: Long): Relationship = ???
      override protected def newGeographicPoint(longitude: Double, latitude: Double, name: String,
                                                code: Int,
                                                href: String): Point = ???
      override protected def newCartesianPoint(x: Double, y: Double, name: String, code: Int,
                                               href: String): Point = ???
    }
    any.writeTo(writer)
    writer.value()
  }
}
