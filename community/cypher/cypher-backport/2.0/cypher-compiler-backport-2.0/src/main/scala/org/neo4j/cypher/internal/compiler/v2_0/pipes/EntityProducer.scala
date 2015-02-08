/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.pipes

import org.neo4j.cypher.internal.compiler.v2_0._
import data.SimpleVal
import org.neo4j.graphdb.PropertyContainer

trait EntityProducer[T <: PropertyContainer] extends ((ExecutionContext, QueryState) => Iterator[T]) {
  def producerType: String
  def description: Seq[(String, SimpleVal)] = Seq("producer" -> SimpleVal.fromStr(producerType))
}

object EntityProducer {
  def apply[T <: PropertyContainer](nameStr: String, args: (String, SimpleVal)*)(f:(ExecutionContext, QueryState) => Iterator[T]) =
    new EntityProducer[T] {
      def producerType = nameStr
      override def description = args ++ super.description
      def apply(m: ExecutionContext, q: QueryState) = f(m, q)
    }
}
