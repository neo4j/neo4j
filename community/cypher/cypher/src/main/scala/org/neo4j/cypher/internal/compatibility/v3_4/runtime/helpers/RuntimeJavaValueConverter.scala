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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.helpers

import java.util.{List => JavaList, Map => JavaMap}

import org.neo4j.cypher.internal.frontend.v3_4.helpers.Eagerly.immutableMapValues
import org.neo4j.cypher.result.QueryResult.{QueryResultVisitor, Record}
import org.neo4j.values.AnyValue

import scala.collection.JavaConverters._
import scala.collection.Map

// This converts runtime scala values into runtime Java value
//
// Main use: Converting parameters when using ExecutionEngine from scala
//
class RuntimeJavaValueConverter(skip: Any => Boolean) {

  final def asDeepJavaMap[S](map: Map[S, Any]): JavaMap[S, Any] =
    if (map == null) null else immutableMapValues(map, asDeepJavaValue).asJava: JavaMap[S, Any]

  def asDeepJavaValue(value: Any): Any = value match {
    case anything if skip(anything) => anything
    case map: Map[_, _] => immutableMapValues(map, asDeepJavaValue).asJava: JavaMap[_, _]
    case JavaListWrapper(inner, _) => inner
    case iterable: Iterable[_] => iterable.map(asDeepJavaValue).toIndexedSeq.asJava: JavaList[_]
    case traversable: TraversableOnce[_] => traversable.map(asDeepJavaValue).toVector.asJava: JavaList[_]
    case anything => anything
  }

  case class feedIteratorToVisitable[EX <: Exception](fields: Iterator[Array[AnyValue]]) {
    def accept(visitor: QueryResultVisitor[EX]) = {
      val row = new ResultRecord()
      var continue = true
      while (continue && fields.hasNext) {
        row._fields = fields.next()
        continue = visitor.visit(row)
      }
    }
  }

  private class ResultRecord extends Record {
    var _fields: Array[AnyValue] = Array.empty
    override def fields(): Array[AnyValue] = _fields
  }
}


