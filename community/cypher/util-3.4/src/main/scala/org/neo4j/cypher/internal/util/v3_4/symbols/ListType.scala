/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util.v3_4.symbols

object ListType {
  private val anyCollectionTypeInstance = new ListTypeImpl(CTAny)

  def apply(iteratedType: CypherType) =
    if (iteratedType == CTAny) anyCollectionTypeInstance else new ListTypeImpl(iteratedType)

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
