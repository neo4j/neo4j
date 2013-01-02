/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan.builders

import org.neo4j.graphdb.PropertyContainer
import org.neo4j.cypher.ParameterWrongTypeException
import collection.JavaConverters._

object GetGraphElements {
  def getElements[T](data: Any, name: String, getElement: Long => T): Seq[T] = {
    def castElement(x: Any): T = x match {
      case i: Int => getElement(i)
      case i: Long => getElement(i)
      case i: String => getElement(i.toLong)
      case element: T => element
    }

    data match {
      case result: Int => Seq(getElement(result))
      case result: Long => Seq(getElement(result))
      case result: java.lang.Iterable[_] => result.asScala.map(castElement).toSeq
      case result: Seq[_] => result.map(castElement).toSeq
      case element: PropertyContainer => Seq(element.asInstanceOf[T])
      case x => throw new ParameterWrongTypeException("Expected a propertycontainer or number here, but got: " + x.toString)
    }
  }
}