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
package org.neo4j.cypher.internal.compiler.v3_2.helpers

import java.util.{List => JavaList, Map => JavaMap}

import org.neo4j.cypher.internal.compiler.v3_2.spi.{InternalResultRow, InternalResultVisitor}
import org.neo4j.cypher.internal.frontend.v3_2.helpers.Eagerly.immutableMapValues
import org.neo4j.graphdb.{Node, Path, Relationship}

import scala.collection.JavaConverters._
import scala.collection.Map

// This converts runtime scala values into runtime Java value
//
// Main use: Converting parameters when using ExecutionEngine from scala
//
class RuntimeJavaValueConverter(skip: Any => Boolean, converter: Any => Any) {

  final def asDeepJavaMap[S](map: Map[S, Any]): JavaMap[S, Any] =
    if (map == null) null else immutableMapValues(map, asDeepJavaValue).asJava: JavaMap[S, Any]

  def asDeepJavaValue(value: Any): Any = value match {
    case anything if skip(anything) => anything
    case map: Map[_, _] => immutableMapValues(map, asDeepJavaValue).asJava: JavaMap[_, _]
    case JavaListWrapper(inner, _) => inner
    case iterable: Iterable[_] => iterable.map(asDeepJavaValue).toIndexedSeq.asJava: JavaList[_]
    case traversable: TraversableOnce[_] => traversable.map(asDeepJavaValue).toVector.asJava: JavaList[_]
    case anything => converter(anything)
  }

  case class feedIteratorToVisitable[EX <: Exception](iterator: Iterator[Map[String, Any]]) {
    def accept(visitor: InternalResultVisitor[EX]) = {
      val row = new MapBasedRow()
      var continue = true
      while (continue && iterator.hasNext) {
        row.map = iterator.next()
        continue = visitor.visit(row)
      }
    }
  }

  private class MapBasedRow extends InternalResultRow {
    var map: Map[String, Any] = Map.empty

    override def getNode(key: String): Node = getWithType(key, classOf[Node])
    override def getRelationship(key: String): Relationship = getWithType(key, classOf[Relationship])
    override def get(key: String): Object = getWithType(key, classOf[Object])
    override def getNumber(key: String): Number = getWithType(key, classOf[Number])
    override def getBoolean(key: String): java.lang.Boolean = getWithType(key, classOf[java.lang.Boolean])
    override def getPath(key: String): Path = getWithType(key, classOf[Path])
    override def getString(key: String): String = getWithType(key, classOf[String])

    private def getWithType[T](key: String, clazz: Class[T]): T = {
      map.get(key) match {
        case None =>
          throw new IllegalArgumentException("No column \"" + key + "\" exists")
        case Some(value) if clazz.isInstance(value) || value == null =>
          clazz.cast(asDeepJavaValue(value))
        case Some(value) =>
          throw new NoSuchElementException("The current item in column \"" + key + "\" is not a " + clazz + ": \"" + value + "\"")
      }
    }
  }
}


