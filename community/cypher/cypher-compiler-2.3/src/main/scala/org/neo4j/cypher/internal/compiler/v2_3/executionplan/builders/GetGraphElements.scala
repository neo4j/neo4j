/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders

import org.neo4j.graphdb.PropertyContainer

import scala.collection.JavaConverters._

object GetGraphElements {
  def getOptionalElements[T: Manifest](data: Any, name: String, getElement: Long => Option[T]): Iterator[T] =
    getElements(data, name, getElement).flatten

  def getElements[T: Manifest](data: Any, name: String, getElement: Long => T): Iterator[T] = {
    def castElement(x: Any): T = x match {
      case i: Int     => getElement(i)
      case i: Long    => getElement(i)
      case i: String  => getElement(i.toLong)
      case element: T => element
    }

    data match {
      case result: Int                   => Iterator.single(getElement(result))
      case result: Long                  => Iterator.single(getElement(result))
      case result: java.util.Iterator[_] => result.asScala.map(castElement)
      case result: java.lang.Iterable[_] => result.asScala.view.map(castElement).iterator
      case result: Seq[_]                => result.view.map(castElement).iterator
      case element: PropertyContainer    => Iterator.single(element.asInstanceOf[T])
      case x                             => Iterator.empty
    }
  }
}
