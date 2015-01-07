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
package org.neo4j.cypher.internal.compiler.v2_0.symbols

object CollectionType {
  private val anyCollectionTypeInstance = new CollectionTypeImpl(CTAny)

  def apply(iteratedType: CypherType) = if (iteratedType == CTAny) anyCollectionTypeInstance else new CollectionTypeImpl(iteratedType)

  final case class CollectionTypeImpl(innerType: CypherType) extends CollectionType {
    val parentType = CTAny
    override val legacyIteratedType = innerType

    override lazy val coercibleTo: Set[CypherType] = Set(CTBoolean)

    override def parents = innerType.parents.map(copy) ++ super.parents

    override val toString = s"Collection<$innerType>"

    override def isAssignableFrom(other: CypherType): Boolean = other match {
      case otherCollection: CollectionType =>
        innerType isAssignableFrom otherCollection.innerType
      case _ =>
        super.isAssignableFrom(other)
    }

    override def mergeUp(other: CypherType) = other match {
      case otherCollection: CollectionType =>
        copy(innerType mergeUp otherCollection.innerType)
      case _ =>
        super.mergeUp(other)
    }

    override def mergeDown(other: CypherType) = other match {
      case otherCollection: CollectionType =>
        (innerType mergeDown otherCollection.innerType).map(copy)
      case _ =>
        super.mergeDown(other)
    }

    override def rewrite(f: CypherType => CypherType) = f(copy(innerType.rewrite(f)))
  }
}

sealed abstract class CollectionType extends CypherType {
  def innerType: CypherType
}
