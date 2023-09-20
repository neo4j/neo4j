/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.util.symbols

import org.neo4j.cypher.internal.util.InputPosition

case class ListType(innerType: CypherType, isNullable: Boolean)(val position: InputPosition) extends CypherType {

  val parentType: CypherType = CTAny
  override val legacyIteratedType: CypherType = innerType

  override lazy val coercibleTo: Set[CypherType] = Set(CTBoolean) ++ parentType.coercibleTo

  override def parents: Seq[CypherType] =
    innerType.parents.map(innerTypeParent => this.copy(innerTypeParent, isNullable)(position)) ++ super.parents

  override val toString = s"List<$innerType>"
  override val toCypherTypeString = s"LIST<${innerType.description}>"

  override def normalizedCypherTypeString(): String = {
    val normalizedType = CypherType.normalizeTypes(this)
    if (normalizedType.isNullable) normalizedType.toCypherTypeString
    else s"${normalizedType.toCypherTypeString} NOT NULL"
  }
  override def sortOrder: Int = CypherTypeOrder.LIST.id
  override def hasValueRepresentation: Boolean = innerType.hasValueRepresentation

  override def simplify: CypherType = innerType match {
    case unionInnerType: ClosedDynamicUnionType =>
      this.copy(unionInnerType.simplify)(position)
    case listInnerType: ListType =>
      this.copy(listInnerType.simplify)(position)
    case _ => this
  }

  // If this list is fully encapsulated by the given list
  override def isSubtypeOf(otherCypherType: CypherType): Boolean = {
    otherCypherType match {
      case otherList: ListType =>
        innerType match {
          case innerDynamicUnion: ClosedDynamicUnionType => otherList.innerType match {
              case otherInnerDynamicUnion: ClosedDynamicUnionType =>
                innerDynamicUnion.innerTypes.forall(innerType =>
                  otherInnerDynamicUnion.innerTypes.exists(otherInnerType => innerType.isSubtypeOf(otherInnerType))
                )
              case _ => false
            }
          case innerList: ListType => otherList match {
              case otherInnerList: ListType => innerList.isSubtypeOf(otherInnerList)
              case _                        => false
            }
          case NothingType()                                => true
          case NullType() if otherList.innerType.isNullable => true
          case _: CypherType => innerType.isSubtypeOf(
              otherList.innerType
            ) && isNullableSubtypeOf(this, otherCypherType)
        }
      case _: AnyType                            => isNullableSubtypeOf(this, otherCypherType)
      case _ @ClosedDynamicUnionType(innerTypes) => innerTypes.exists(isSubtypeOf)
      case _                                     => false
    }
  }

  override def updateIsNullable(isNullable: Boolean): CypherType = this.copy(isNullable = isNullable)(position)

  def withPosition(newPosition: InputPosition): CypherType = this.copy()(position = newPosition)

  override def isAssignableFrom(other: CypherType): Boolean = other match {
    case otherCollection: ListType =>
      innerType isAssignableFrom otherCollection.innerType
    case _ =>
      super.isAssignableFrom(other)
  }

  override def leastUpperBound(other: CypherType): CypherType = other match {
    case otherCollection: ListType =>
      copy(innerType leastUpperBound otherCollection.innerType)(position)
    case _ =>
      super.leastUpperBound(other)
  }

  override def greatestLowerBound(other: CypherType): Option[CypherType] = other match {
    case otherCollection: ListType =>
      (innerType greatestLowerBound otherCollection.innerType).map(f => copy(f)(position))
    case _ =>
      super.greatestLowerBound(other)
  }

  override def rewrite(f: CypherType => CypherType): CypherType = f(copy(innerType.rewrite(f))(position))
}
