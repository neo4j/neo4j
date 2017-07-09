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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.values.AnyValue

import scala.collection.mutable

object QueryStateHelper {
  def empty: QueryState = emptyWith()

  def emptyWith(query: QueryContext = null, resources: ExternalCSVResource = null,
                params: Map[String, AnyValue] = Map.empty, decorator: PipeDecorator = NullPipeDecorator,
                initialContext: Option[ExecutionContext] = None) =
    new QueryState(query = query, resources = resources, params = params, decorator = decorator,
      initialContext = initialContext, triadicState = mutable.Map.empty, repeatableReads = mutable.Map.empty)
}
