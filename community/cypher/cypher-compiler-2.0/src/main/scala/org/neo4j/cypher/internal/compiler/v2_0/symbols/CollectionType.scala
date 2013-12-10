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
  private val anyCollectionTypeInstance = new CollectionTypeImpl(AnyType())

  def apply(iteratedType: CypherType) = if (iteratedType == AnyType()) anyCollectionTypeInstance else new CollectionTypeImpl(iteratedType)

  final case class CollectionTypeImpl(innerType: CypherType) extends CollectionType {
    val parentType = AnyType()
    override val legacyIteratedType = innerType

    override def parents = innerType.parents.map(copy) ++ super.parents

    override val toString = s"Collection<$innerType>"

    override def isAssignableFrom(other: CypherType): Boolean = other match {
      case otherCollection: CollectionType =>
        innerType isAssignableFrom otherCollection.innerType
      case _ =>
        super.isAssignableFrom(other)
    }

    override def mergeDown(other: CypherType) = other match {
      case otherCollection: CollectionType =>
        copy(innerType mergeDown otherCollection.innerType)
      case _ =>
        super.mergeDown(other)
    }

    override def mergeUp(other: CypherType) = other match {
      case otherCollection: CollectionType =>
        (innerType mergeUp otherCollection.innerType).map(copy)
      case _ =>
        super.mergeUp(other)
    }

    override def rewrite(f: CypherType => CypherType) = f(copy(innerType.rewrite(f)))
  }
}

sealed abstract class CollectionType extends CypherType {
  def innerType: CypherType
}
