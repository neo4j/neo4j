/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_1.symbols

object ListType {
  private val anyCollectionTypeInstance = new ListTypeImpl(CTAny)

  def apply(iteratedType: CypherType) = if (iteratedType == CTAny) anyCollectionTypeInstance else new ListTypeImpl(iteratedType)

  final case class ListTypeImpl(innerType: CypherType) extends ListType {
    val parentType = CTAny
    override val legacyIteratedType = innerType

    override lazy val coercibleTo: Set[CypherType] = Set(CTBoolean)

    override def parents = innerType.parents.map(copy) ++ super.parents

    override val toString = s"List<$innerType>"
    override val toNeoTypeString = s"LIST? OF ${innerType.toNeoTypeString}"


    override def isAssignableFrom(other: CypherType): Boolean = other match {
      case otherCollection: ListType =>
        innerType isAssignableFrom otherCollection.innerType
      case _ =>
        super.isAssignableFrom(other)
    }

    override def leastUpperBound(other: CypherType) = other match {
      case otherCollection: ListType =>
        copy(innerType leastUpperBound otherCollection.innerType)
      case _ =>
        super.leastUpperBound(other)
    }

    override def greatestLowerBound(other: CypherType) = other match {
      case otherCollection: ListType =>
        (innerType greatestLowerBound otherCollection.innerType).map(copy)
      case _ =>
        super.greatestLowerBound(other)
    }

    override def rewrite(f: CypherType => CypherType) = f(copy(innerType.rewrite(f)))
  }

  def unapply(x: CypherType): Option[CypherType] = x match {
    case x: ListType => Some(x.innerType)
    case _ => None
  }
}


sealed abstract class ListType extends CypherType {
  def innerType: CypherType
}
