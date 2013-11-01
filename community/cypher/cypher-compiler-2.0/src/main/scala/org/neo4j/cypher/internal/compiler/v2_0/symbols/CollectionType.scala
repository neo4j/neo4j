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

  final case class CollectionTypeImpl(override val iteratedType: CypherType) extends CollectionType {
    val parentType = AnyType()

    override def parents = iteratedType.parents.map(copy) ++ super.parents

    override def toString = s"Collection<$iteratedType>"

    override def isAssignableFrom(other: CypherType): Boolean =
      (other.isInstanceOf[CollectionType] || super.isAssignableFrom(other)) &&
      iteratedType.isAssignableFrom(other.asInstanceOf[CollectionType].iteratedType)

    override def isCoercibleFrom(other: CypherType): Boolean =
      super.isCoercibleFrom(other) ||
      other == BooleanType()

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

    override def rewrite(f: CypherType => CypherType) = f(CollectionType(this.iteratedType.rewrite(f)))
  }
}

sealed abstract class CollectionType extends CypherType
