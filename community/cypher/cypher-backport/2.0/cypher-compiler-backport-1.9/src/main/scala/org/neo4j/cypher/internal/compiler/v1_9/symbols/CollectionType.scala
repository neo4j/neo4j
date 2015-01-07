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
package org.neo4j.cypher.internal.compiler.v1_9.symbols

import java.lang.String

class CollectionType(override val iteratedType: CypherType) extends AnyType {

  override def toString: String =
    if (iteratedType.isInstanceOf[AnyType])
      "Collection"
    else
      "Collection<" + iteratedType + ">"

  override def isAssignableFrom(other:CypherType):Boolean = super.isAssignableFrom(other) &&
                                                            iteratedType.isAssignableFrom(other.asInstanceOf[CollectionType].iteratedType)

  override def mergeWith(other: CypherType) = other match {
    case otherCollection: CollectionType =>
      new CollectionType(iteratedType mergeWith otherCollection.iteratedType)
    case _ =>
      super.mergeWith(other)
  }

  override val isCollection = true
}
