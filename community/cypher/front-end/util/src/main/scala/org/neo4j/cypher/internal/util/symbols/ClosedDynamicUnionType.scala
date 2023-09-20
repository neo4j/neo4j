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

case class ClosedDynamicUnionType(innerTypes: Set[CypherType])(val position: InputPosition) extends CypherType {
  val parentType: CypherType = CTAny

  val sortedInnerTypes: List[CypherType] = innerTypes.map(_.simplify).toList.sorted
  override val toString: String = sortedInnerTypes.map(_.toString).mkString(" | ")
  override val toCypherTypeString: String = sortedInnerTypes.map(_.description).mkString(" | ")
  override def normalizedCypherTypeString(): String = CypherType.normalizeTypes(this).toCypherTypeString

  override def sortOrder: Int = CypherTypeOrder.CLOSED_DYNAMIC_UNION.id
  // Dynamic Union Types are not allowed to declare NOT NULL, nullability is based on the inner types nullability
  // e.g ANY<INTEGER NOT NULL | FLOAT NOT NULL> does not contain null
  // ANY<INTEGER | FLOAT> does
  // ANY<INTEGER NOT NULL | FLOAT> is invalid and will produce a semantic error
  override def isNullable: Boolean = sortedInnerTypes.head.isNullable

  override def description: String = toCypherTypeString

  override def simplify: CypherType = {
    val flattenedInner = sortedInnerTypes.flatMap {
      case c: ClosedDynamicUnionType => c.sortedInnerTypes.map(_.simplify)
      case other                     => List(other.simplify)
    }.toSet
    if (flattenedInner.size == 1) flattenedInner.head else this.copy(flattenedInner)(position)
  }

  override def updateIsNullable(isNullable: Boolean): CypherType =
    this.copy(innerTypes.map(_.updateIsNullable(isNullable)))(position)

  def withPosition(newPosition: InputPosition): CypherType = this.copy()(position = newPosition)

  // Don't check the non-simplified types in the equals
  override def equals(other: Any): Boolean = other match {
    case otherUnion: ClosedDynamicUnionType =>
      sortedInnerTypes.equals(otherUnion.sortedInnerTypes)
    case _ => false
  }

  override def isAssignableFrom(other: CypherType): Boolean =
    innerTypes.exists(innerType => innerType isAssignableFrom other)

  override def parents: Seq[CypherType] = innerTypes.flatMap(_.parents).toList

  override lazy val covariant: TypeSpec = innerTypes.tail.foldLeft(innerTypes.head.invariant)(_ union _).covariant
  override lazy val invariant: TypeSpec = TypeSpec.exact(this.innerTypes)

  override def leastUpperBound(other: CypherType): CypherType =
    innerTypes.foldLeft(other)(_ leastUpperBound _)

  override def greatestLowerBound(other: CypherType): Option[CypherType] = other match {
    case otherClosedDynamicUnion: ClosedDynamicUnionType =>
      val lowerBounds: Set[Option[CypherType]] = Set()
      innerTypes.foreach { innerType =>
        otherClosedDynamicUnion.innerTypes.foreach { otherInnerType =>
          lowerBounds ++ Set(innerType greatestLowerBound otherInnerType)
        }
      }

      if (!lowerBounds.exists(_.isDefined)) None
      else Some(ClosedDynamicUnionType(lowerBounds.filter(_.isDefined).map(a => a.get))(position))
    case _ =>
      var lowerBound = innerTypes.head greatestLowerBound other
      innerTypes.tail.foreach { innerType =>
        lowerBound = innerType greatestLowerBound other match {
          case Some(value) if lowerBound.isEmpty => Some(value)
          case Some(value) if (lowerBound.get greatestLowerBound value).isDefined =>
            lowerBound.get greatestLowerBound value
          case Some(_) => Some(other)
          case None    => lowerBound
        }
      }
      lowerBound
  }
}
