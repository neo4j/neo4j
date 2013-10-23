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
package org.neo4j.cypher.internal.compiler.v2_0.symbols

object CollectionType {
  val anyCollectionTypeInstance = new CollectionType(AnyType())

  def apply(iteratedType: CypherType) = if (iteratedType == AnyType()) anyCollectionTypeInstance else new CollectionType(iteratedType)
}

class CollectionType(override val iteratedType: CypherType) extends AnyType {

  override def toString: String = s"Collection<$iteratedType>"

  override def isCoercibleFrom(other: CypherType): Boolean =
    super.isCoercibleFrom(other) ||
    other == BooleanType()

  override def isAssignableFrom(other: CypherType): Boolean = super.isAssignableFrom(other) &&
    iteratedType.isAssignableFrom(other.asInstanceOf[CollectionType].iteratedType)

  override def mergeDown(other: CypherType) = other match {
    case otherCollection: CollectionType =>
      CollectionType(iteratedType mergeDown otherCollection.iteratedType)
    case _ =>
      super.mergeDown(other)
  }

  override def mergeUp(other: CypherType) = other match {
    case otherCollection: CollectionType =>
      for (ctype <- iteratedType mergeUp otherCollection.iteratedType) yield CollectionType(ctype)
    case _ =>
      super.mergeUp(other)
  }

  override val isCollection = true

  override def equals(other: Any) = other match {
    case null => false
    case t: CollectionType => t.iteratedType.equals(iteratedType)
    case _ => false
  }

  override def hashCode = 37 * iteratedType.hashCode

  override def rewrite(f: CypherType => CypherType) = f(CollectionType(this.iteratedType.rewrite(f)))
}
