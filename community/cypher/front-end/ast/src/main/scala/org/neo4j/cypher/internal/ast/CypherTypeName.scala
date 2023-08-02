/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.semantics.SemanticAnalysisTooling
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckable
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.values.storable.ValueRepresentation

sealed trait CypherTypeName extends SemanticCheckable with SemanticAnalysisTooling with ASTNode {
  protected def typeName: String
  // e.g BOOLEAN set(true, false, null) is nullable, BOOLEAN NOT NULL set(true, false) is not
  def isNullable: Boolean

  def hasValueRepresentation: Boolean = false

  def possibleValueRepresentations: List[ValueRepresentation] =
    throw new UnsupportedOperationException("possibleValueRepresentations not supported on ${getClass.getName}")

  def updateIsNullable(isNullable: Boolean): CypherTypeName
  def description: String = if (isNullable) typeName else s"$typeName NOT NULL"
  override def toString: String = description

  def withPosition(position: InputPosition): CypherTypeName

  override def semanticCheck: SemanticCheck = SemanticCheck.success

  def sortOrder: Int
  def simplify: CypherTypeName = this

  def isSubtypeOf(otherCypherTypeName: CypherTypeName): Boolean = this match {
    case thisDynamicUnion: ClosedDynamicUnionTypeName => otherCypherTypeName match {
        case otherDynamicUnion: ClosedDynamicUnionTypeName =>
          thisDynamicUnion.innerTypes.forall(otherDynamicUnion.innerTypes.contains)
        case _ => false
      }
    case _ => otherCypherTypeName match {
        case otherDynamicUnion: ClosedDynamicUnionTypeName =>
          otherDynamicUnion.innerTypes.contains(this)
        case _: AnyTypeName => isNullableSubtypeOf(this, otherCypherTypeName)
        case _ =>
          typeName.equals(otherCypherTypeName.typeName) && isNullableSubtypeOf(this, otherCypherTypeName)
      }
  }

  protected def isNullableSubtypeOf(cypherType: CypherTypeName, otherCypherType: CypherTypeName): Boolean =
    !cypherType.isNullable || (cypherType.isNullable && otherCypherType.isNullable)
}

object CypherTypeName {

  /**
   * normalizeTypes takes a CypherTypeName and will normalize it to a simpler form.
   * The rules are defined in CIP-100 (https://docs.google.com/document/d/12i09VJaWRneFQkrsdl20Xv2LM1UEndjnRoNiOWObHC4)
   * Normalization Rules:
   *  Type Name Normalization: This is done in parsing e.g. BOOL -> BOOLEAN
   *  Duplicate types are removed (done initially in parsing)
   *  NULL NOT NULL and NOTHING NOT NULL -> NOTHING.
   *  NOTHING is absorbed if any other type is present
   *  NULL is absorbed into types e.g ANY<BOOLEAN | NULL> is BOOLEAN
   *  NULL is propagated through other types e.g ANY<BOOLEAN | INTEGER NOT NULL> is ANY<BOOLEAN | INTEGER>
   *  Dynamic unions are always flattened e.g ANY<ANY<BOOLEAN | FLOAT>> is ANY<BOOLEAN | FLOAT>
   *  If all types are present, then it is simplified to ANY
   *  If ANY is present, then other types may be absorbed e.g ANY<BOOLEAN | ANY > is ANY
   *  PROPERTY VALUE is simplified to the Closed Dynamic Union of all property value types
   *  Encapsulated LISTs are absorbed e.g LIST<BOOLEAN> | LIST<BOOLEAN | INTEGER> = LIST<BOOLEAN | INTEGER>
   *
   * Ordering of Types: Types have a specific order (see sortOrder). NOT NULL variants of types comes before the nullable.
   *  LISTs with fewer inner types are ordered before LISTs with more. If both LISTs have the same size, the ordering is
   *  lexigraphical.
   */
  def normalizeTypes(typeToNormalize: CypherTypeName): CypherTypeName = {
    val simplifiedType = typeToNormalize.simplify
    simplifiedType match {
      case ClosedDynamicUnionTypeName(innerTypes) =>
        val updatedTypes = normalizeInnerTypes(innerTypes)
        ClosedDynamicUnionTypeName(updatedTypes.toSet)(typeToNormalize.position).simplify
      case lt: ListTypeName          => lt.copy(normalizeTypes(lt.innerType))(lt.position)
      case pt: PropertyValueTypeName => ClosedDynamicUnionTypeName(pt.expandToTypes.toSet)(pt.position)
      case _                         => simplifiedType
    }
  }

  private def normalizeInnerTypes(innerTypes: Set[CypherTypeName]): List[CypherTypeName] = {
    var oldTypes = innerTypes.toList
    var updatedTypes = oldTypes

    do {
      val newTypes = normalize(updatedTypes)
      oldTypes = updatedTypes
      updatedTypes = newTypes
    } while (!oldTypes.equals(updatedTypes))

    updatedTypes
  }

  // Remove instances where both `TYPE` and `TYPE NOT NULL` appears, keeping `TYPE`.
  // Sorts according to ordering
  private def normalize(types: List[CypherTypeName]) = {
    // Expand PropertyValueTypeName to all property types instead
    val expandedTypes: List[CypherTypeName] = types.flatMap {
      case propertyValueTypeName: PropertyValueTypeName => propertyValueTypeName.expandToTypes
      case cypherType                                   => List(cypherType)
    }
    // Simplify expressions, (for example flatten dynamic unions: ANY<ANY<BOOLEAN>> -> BOOLEAN)
    // and normalize inner types (normalizes nested dynamic unions and lists)
    val simplifiedTypes = expandedTypes.map(_.simplify).map(normalizeTypes).sorted

    // Check for multiples of the same type or subtype, example:
    //  - BOOLEAN | BOOLEAN NOT NULL -> BOOLEAN
    //  - BOOLEAN | BOOLEAN (after flattening BOOLEAN | ANY<BOOLEAN>) -> BOOLEAN
    //  - LIST<BOOLEAN> | LIST<BOOLEAN | INTEGER> -> LIST<BOOLEAN | INTEGER>
    val combinedTypes = simplifiedTypes.foldLeft(List.empty[CypherTypeName]) {
      // Filter out already added subtypes of this list
      case (filteredList, currentType) =>
        filteredList.filterNot(cypherType => cypherType.isSubtypeOf(currentType)) :+ currentType
    }

    // Keep as is if we either have only NOT NULL or no NOT NULL versions
    // if mixed, remove the NOT NULL
    // Example: remove NOT NULL on INTEGER for BOOLEAN | INTEGER NOT NULL
    var updatedTypes = if (combinedTypes.forall(_.isNullable) || combinedTypes.forall(!_.isNullable)) combinedTypes
    else combinedTypes.map(t => if (!t.isNullable) t.updateIsNullable(true) else t)

    // If all types are present, rewrite to Any
    if (allTypes(updatedTypes.head.isNullable).forall(updatedTypes.map(_.withPosition(InputPosition.NONE)).contains))
      updatedTypes = List(AnyTypeName(updatedTypes.head.isNullable)(updatedTypes.map(_.position).min))

    // Sort types
    updatedTypes.sorted
  }

  private def allTypes(isNullable: Boolean): Set[CypherTypeName] = Set(
    BooleanTypeName(isNullable)(InputPosition.NONE),
    StringTypeName(isNullable)(InputPosition.NONE),
    IntegerTypeName(isNullable)(InputPosition.NONE),
    FloatTypeName(isNullable)(InputPosition.NONE),
    DateTypeName(isNullable)(InputPosition.NONE),
    LocalTimeTypeName(isNullable)(InputPosition.NONE),
    ZonedTimeTypeName(isNullable)(InputPosition.NONE),
    LocalDateTimeTypeName(isNullable)(InputPosition.NONE),
    ZonedDateTimeTypeName(isNullable)(InputPosition.NONE),
    DurationTypeName(isNullable)(InputPosition.NONE),
    PointTypeName(isNullable)(InputPosition.NONE),
    NodeTypeName(isNullable)(InputPosition.NONE),
    RelationshipTypeName(isNullable)(InputPosition.NONE),
    MapTypeName(isNullable)(InputPosition.NONE),
    ListTypeName(AnyTypeName(isNullable = true)(InputPosition.NONE), isNullable = isNullable)(InputPosition.NONE),
    PathTypeName(isNullable)(InputPosition.NONE)
  )

  implicit val order: Ordering[CypherTypeName] = {
    // TYPE NOT NULL should be before TYPE
    // comparing boolean values gives false before true, so isNullable false comes first as wanted
    (x: CypherTypeName, y: CypherTypeName) =>
      (x, y) match {
        case (lx: ListTypeName, ly: ListTypeName)                               => compareListTypes(lx, ly)
        case (cux: ClosedDynamicUnionTypeName, cuy: ClosedDynamicUnionTypeName) =>
          // Sorting multiple closed dynamic unions
          compareInnerLists(cux.sortedInnerTypes, cuy.sortedInnerTypes)
        case (tx, ty) if tx.sortOrder == ty.sortOrder => x.isNullable.compare(y.isNullable)
        case _                                        => x.sortOrder.compare(y.sortOrder)
      }
  }

  private def compareListTypes(x: ListTypeName, y: ListTypeName): Int = {
    val innerComparison = (x.innerType, y.innerType) match {
      case (hx: ListTypeName, hy: ListTypeName) => compareListTypes(hx, hy)
      case (hx: ClosedDynamicUnionTypeName, hy: ClosedDynamicUnionTypeName) =>
        compareInnerLists(hx.sortedInnerTypes, hy.sortedInnerTypes)
      case _ => x.innerType.sortOrder.compare(y.innerType.sortOrder)
    }

    // The inner comparison was equal, first check nullability of them
    val innerComparisonInnerTypeNullCheck = if (innerComparison == 0)
      x.innerType.isNullable.compare(y.innerType.isNullable)
    else innerComparison
    // the inner parts were of same nullability, compare lists nullability
    if (innerComparisonInnerTypeNullCheck == 0) x.isNullable.compare(y.isNullable)
    else innerComparisonInnerTypeNullCheck
  }

  private def compareInnerLists(x: List[CypherTypeName], y: List[CypherTypeName]): Int = {
    if (x.isEmpty && y.isEmpty) 0
    else if (x.size != y.size) x.size.compare(y.size)
    else if (x.head.equals(y.head))
      compareInnerLists(x.tail, y.tail)
    else {
      val hx = x.head
      val hy = y.head

      val sortOrder = hx.sortOrder.compare(hy.sortOrder)
      if (sortOrder == 0) {
        val innerComparison = (hx, hy) match {
          case (
              ListTypeName(lxDynamicUnion: ClosedDynamicUnionTypeName, _),
              ListTypeName(lyDynamicUnion: ClosedDynamicUnionTypeName, _)
            ) =>
            compareInnerLists(lxDynamicUnion.sortedInnerTypes, lyDynamicUnion.sortedInnerTypes)
          case (
              ListTypeName(lxListType: ListTypeName, _),
              ListTypeName(lyListType: ListTypeName, _)
            ) =>
            compareListTypes(lxListType, lyListType)
          case (ListTypeName(lxInnerType: CypherTypeName, _), ListTypeName(lyInnerType: CypherTypeName, _)) =>
            val innerTypeOrder = lxInnerType.sortOrder.compare(lyInnerType.sortOrder)
            if (innerTypeOrder == 0) lxInnerType.isNullable.compare(lyInnerType.isNullable)
            else innerTypeOrder
          case (cux: ClosedDynamicUnionTypeName, cuy: ClosedDynamicUnionTypeName) =>
            compareInnerLists(cux.sortedInnerTypes, cuy.sortedInnerTypes)
          case _ => sortOrder
        }

        if (innerComparison == 0) hx.isNullable.compare(hy.isNullable)
        else innerComparison
      } else sortOrder
    }
  }
}

case class NothingTypeName()(val position: InputPosition) extends CypherTypeName {
  protected val typeName: String = "NOTHING"

  // The NOTHING type never includes `null` but should not have a `NOT NULL`
  override def isNullable: Boolean = false
  override def description: String = typeName

  override def sortOrder: Int = CypherTypeNameOrder.NOTHING.id

  override def updateIsNullable(isNullable: Boolean): CypherTypeName = this

  override def isSubtypeOf(otherCypherTypeName: CypherTypeName): Boolean = true

  def withPosition(newPosition: InputPosition): CypherTypeName = this.copy()(position = newPosition)
}

case class NullTypeName()(val position: InputPosition) extends CypherTypeName {
  protected val typeName: String = "NULL"
  override def isNullable: Boolean = true

  override def sortOrder: Int = CypherTypeNameOrder.NULL.id

  override def updateIsNullable(isNullable: Boolean): CypherTypeName =
    if (isNullable) this
    else NothingTypeName()(position)

  override def isSubtypeOf(otherCypherTypeName: CypherTypeName): Boolean = otherCypherTypeName.isNullable

  def withPosition(newPosition: InputPosition): CypherTypeName = this.copy()(position = newPosition)
}

case class BooleanTypeName(isNullable: Boolean)(val position: InputPosition) extends CypherTypeName {
  protected val typeName: String = "BOOLEAN"

  override def hasValueRepresentation: Boolean = true

  override def possibleValueRepresentations: List[ValueRepresentation] = List(ValueRepresentation.BOOLEAN)

  override def sortOrder: Int = CypherTypeNameOrder.BOOLEAN.id

  override def updateIsNullable(isNullable: Boolean): CypherTypeName = this.copy(isNullable = isNullable)(position)

  def withPosition(newPosition: InputPosition): CypherTypeName = this.copy()(position = newPosition)
}

case class StringTypeName(isNullable: Boolean)(val position: InputPosition) extends CypherTypeName {
  protected val typeName: String = "STRING"

  override def hasValueRepresentation: Boolean = true

  override def possibleValueRepresentations: List[ValueRepresentation] =
    List(ValueRepresentation.UTF8_TEXT, ValueRepresentation.UTF16_TEXT)

  override def sortOrder: Int = CypherTypeNameOrder.STRING.id

  override def updateIsNullable(isNullable: Boolean): CypherTypeName = this.copy(isNullable = isNullable)(position)

  def withPosition(newPosition: InputPosition): CypherTypeName = this.copy()(position = newPosition)
}

case class IntegerTypeName(isNullable: Boolean)(val position: InputPosition) extends CypherTypeName {
  protected val typeName: String = "INTEGER"

  override def hasValueRepresentation: Boolean = true

  override def possibleValueRepresentations: List[ValueRepresentation] = List(
    ValueRepresentation.INT8,
    ValueRepresentation.INT16,
    ValueRepresentation.INT32,
    ValueRepresentation.INT64
  )

  override def sortOrder: Int = CypherTypeNameOrder.INTEGER.id

  override def updateIsNullable(isNullable: Boolean): CypherTypeName = this.copy(isNullable = isNullable)(position)

  def withPosition(newPosition: InputPosition): CypherTypeName = this.copy()(position = newPosition)
}

case class FloatTypeName(isNullable: Boolean)(val position: InputPosition) extends CypherTypeName {
  protected val typeName: String = "FLOAT"

  override def hasValueRepresentation: Boolean = true

  override def possibleValueRepresentations: List[ValueRepresentation] =
    List(ValueRepresentation.FLOAT32, ValueRepresentation.FLOAT64)

  override def sortOrder: Int = CypherTypeNameOrder.FLOAT.id

  override def updateIsNullable(isNullable: Boolean): CypherTypeName = this.copy(isNullable = isNullable)(position)

  def withPosition(newPosition: InputPosition): CypherTypeName = this.copy()(position = newPosition)
}

case class DateTypeName(isNullable: Boolean)(val position: InputPosition) extends CypherTypeName {
  protected val typeName: String = "DATE"

  override def hasValueRepresentation: Boolean = true

  override def possibleValueRepresentations: List[ValueRepresentation] = List(ValueRepresentation.DATE)

  override def sortOrder: Int = CypherTypeNameOrder.DATE.id

  override def updateIsNullable(isNullable: Boolean): CypherTypeName = this.copy(isNullable = isNullable)(position)

  def withPosition(newPosition: InputPosition): CypherTypeName = this.copy()(position = newPosition)
}

case class LocalTimeTypeName(isNullable: Boolean)(val position: InputPosition) extends CypherTypeName {
  protected val typeName: String = "LOCAL TIME"

  override def hasValueRepresentation: Boolean = true

  override def possibleValueRepresentations: List[ValueRepresentation] = List(ValueRepresentation.LOCAL_TIME)

  override def sortOrder: Int = CypherTypeNameOrder.LOCAL_TIME.id

  override def updateIsNullable(isNullable: Boolean): CypherTypeName = this.copy(isNullable = isNullable)(position)

  def withPosition(newPosition: InputPosition): CypherTypeName = this.copy()(position = newPosition)
}

case class ZonedTimeTypeName(isNullable: Boolean)(val position: InputPosition) extends CypherTypeName {
  protected val typeName: String = "ZONED TIME"

  override def hasValueRepresentation: Boolean = true

  override def possibleValueRepresentations: List[ValueRepresentation] = List(ValueRepresentation.ZONED_TIME)

  override def sortOrder: Int = CypherTypeNameOrder.ZONED_TIME.id

  override def updateIsNullable(isNullable: Boolean): CypherTypeName = this.copy(isNullable = isNullable)(position)

  def withPosition(newPosition: InputPosition): CypherTypeName = this.copy()(position = newPosition)
}

case class LocalDateTimeTypeName(isNullable: Boolean)(val position: InputPosition) extends CypherTypeName {
  protected val typeName: String = "LOCAL DATETIME"

  override def hasValueRepresentation: Boolean = true

  override def possibleValueRepresentations: List[ValueRepresentation] = List(ValueRepresentation.LOCAL_DATE_TIME)

  override def sortOrder: Int = CypherTypeNameOrder.LOCAL_DATETIME.id

  override def updateIsNullable(isNullable: Boolean): CypherTypeName = this.copy(isNullable = isNullable)(position)

  def withPosition(newPosition: InputPosition): CypherTypeName = this.copy()(position = newPosition)
}

case class ZonedDateTimeTypeName(isNullable: Boolean)(val position: InputPosition) extends CypherTypeName {
  protected val typeName: String = "ZONED DATETIME"

  override def hasValueRepresentation: Boolean = true

  override def possibleValueRepresentations: List[ValueRepresentation] = List(ValueRepresentation.ZONED_DATE_TIME)

  override def sortOrder: Int = CypherTypeNameOrder.ZONED_DATETIME.id

  override def updateIsNullable(isNullable: Boolean): CypherTypeName = this.copy(isNullable = isNullable)(position)

  def withPosition(newPosition: InputPosition): CypherTypeName = this.copy()(position = newPosition)
}

case class DurationTypeName(isNullable: Boolean)(val position: InputPosition) extends CypherTypeName {
  protected val typeName: String = "DURATION"

  override def hasValueRepresentation: Boolean = true

  override def possibleValueRepresentations: List[ValueRepresentation] = List(ValueRepresentation.DURATION)

  override def sortOrder: Int = CypherTypeNameOrder.DURATION.id

  override def updateIsNullable(isNullable: Boolean): CypherTypeName = this.copy(isNullable = isNullable)(position)

  def withPosition(newPosition: InputPosition): CypherTypeName = this.copy()(position = newPosition)
}

case class PointTypeName(isNullable: Boolean)(val position: InputPosition) extends CypherTypeName {
  protected val typeName: String = "POINT"

  override def hasValueRepresentation: Boolean = true

  override def possibleValueRepresentations: List[ValueRepresentation] = List(ValueRepresentation.GEOMETRY)

  override def sortOrder: Int = CypherTypeNameOrder.POINT.id

  override def updateIsNullable(isNullable: Boolean): CypherTypeName = this.copy(isNullable = isNullable)(position)

  def withPosition(newPosition: InputPosition): CypherTypeName = this.copy()(position = newPosition)
}

case class NodeTypeName(isNullable: Boolean)(val position: InputPosition) extends CypherTypeName {
  protected val typeName: String = "NODE"

  override def sortOrder: Int = CypherTypeNameOrder.NODE.id

  override def updateIsNullable(isNullable: Boolean): CypherTypeName = this.copy(isNullable = isNullable)(position)

  def withPosition(newPosition: InputPosition): CypherTypeName = this.copy()(position = newPosition)
}

case class RelationshipTypeName(isNullable: Boolean)(val position: InputPosition) extends CypherTypeName {
  protected val typeName: String = "RELATIONSHIP"

  override def sortOrder: Int = CypherTypeNameOrder.RELATIONSHIP.id

  override def updateIsNullable(isNullable: Boolean): CypherTypeName = this.copy(isNullable = isNullable)(position)

  def withPosition(newPosition: InputPosition): CypherTypeName = this.copy()(position = newPosition)
}

case class MapTypeName(isNullable: Boolean)(val position: InputPosition) extends CypherTypeName {
  protected val typeName: String = "MAP"

  override def sortOrder: Int = CypherTypeNameOrder.MAP.id

  override def updateIsNullable(isNullable: Boolean): CypherTypeName = this.copy(isNullable = isNullable)(position)

  def withPosition(newPosition: InputPosition): CypherTypeName = this.copy()(position = newPosition)
}

case class ListTypeName(innerType: CypherTypeName, isNullable: Boolean)(val position: InputPosition)
    extends CypherTypeName {
  override def hasValueRepresentation: Boolean = possibleValueRepresentations.nonEmpty

  override def possibleValueRepresentations: List[ValueRepresentation] = innerType match {
    case _: BooleanTypeName => List(ValueRepresentation.BOOLEAN_ARRAY)
    case _: StringTypeName  => List(ValueRepresentation.TEXT_ARRAY)
    case _: IntegerTypeName => List(
        ValueRepresentation.INT8_ARRAY,
        ValueRepresentation.INT16_ARRAY,
        ValueRepresentation.INT32_ARRAY,
        ValueRepresentation.INT64_ARRAY
      )
    case _: FloatTypeName         => List(ValueRepresentation.FLOAT32_ARRAY, ValueRepresentation.FLOAT64_ARRAY)
    case _: DateTypeName          => List(ValueRepresentation.DATE_ARRAY)
    case _: LocalTimeTypeName     => List(ValueRepresentation.LOCAL_TIME_ARRAY)
    case _: ZonedTimeTypeName     => List(ValueRepresentation.ZONED_TIME_ARRAY)
    case _: LocalDateTimeTypeName => List(ValueRepresentation.LOCAL_DATE_TIME_ARRAY)
    case _: ZonedDateTimeTypeName => List(ValueRepresentation.ZONED_DATE_TIME_ARRAY)
    case _: DurationTypeName      => List(ValueRepresentation.DURATION_ARRAY)
    case _: PointTypeName         => List(ValueRepresentation.GEOMETRY_ARRAY)
    case _                        => List.empty
  }

  protected val typeName: String = s"LIST<${innerType.description}>"
  override def sortOrder: Int = CypherTypeNameOrder.LIST.id

  override def simplify: CypherTypeName = innerType match {
    case unionInnerType: ClosedDynamicUnionTypeName =>
      this.copy(unionInnerType.simplify)(position)
    case listInnerType: ListTypeName =>
      this.copy(listInnerType.simplify)(position)
    case _ => this
  }

  override def semanticCheck: SemanticCheck = {
    innerType match {
      case unionInnerType: ClosedDynamicUnionTypeName => unionInnerType.semanticCheck
      case _                                          => SemanticCheck.success
    }
  } chain super.semanticCheck

  // If this list is fully encapsulated by the given list
  override def isSubtypeOf(otherCypherTypeName: CypherTypeName): Boolean = {
    otherCypherTypeName match {
      case otherList: ListTypeName =>
        innerType match {
          case innerDynamicUnion: ClosedDynamicUnionTypeName => otherList.innerType match {
              case otherInnerDynamicUnion: ClosedDynamicUnionTypeName =>
                innerDynamicUnion.innerTypes.forall(innerType =>
                  otherInnerDynamicUnion.innerTypes.exists(otherInnerType => innerType.isSubtypeOf(otherInnerType))
                )
              case _ => false
            }
          case innerList: ListTypeName => otherList match {
              case otherInnerList: ListTypeName => innerList.isSubtypeOf(otherInnerList)
              case _                            => false
            }
          case NothingTypeName()                                => true
          case NullTypeName() if otherList.innerType.isNullable => true
          case _: CypherTypeName => innerType.isSubtypeOf(
              otherList.innerType
            ) && isNullableSubtypeOf(this, otherCypherTypeName)
        }
      case _: AnyTypeName                            => isNullableSubtypeOf(this, otherCypherTypeName)
      case _ @ClosedDynamicUnionTypeName(innerTypes) => innerTypes.exists(isSubtypeOf)
      case _                                         => false
    }
  }

  override def updateIsNullable(isNullable: Boolean): CypherTypeName = this.copy(isNullable = isNullable)(position)

  def withPosition(newPosition: InputPosition): CypherTypeName = this.copy()(position = newPosition)
}

case class PathTypeName(isNullable: Boolean)(val position: InputPosition) extends CypherTypeName {
  protected val typeName: String = "PATH"

  override def sortOrder: Int = CypherTypeNameOrder.PATH.id

  override def updateIsNullable(isNullable: Boolean): CypherTypeName = this.copy(isNullable = isNullable)(position)

  def withPosition(newPosition: InputPosition): CypherTypeName = this.copy()(position = newPosition)
}

case class PropertyValueTypeName(isNullable: Boolean)(val position: InputPosition) extends CypherTypeName {
  protected val typeName: String = "PROPERTY VALUE"

  // This is technically a special case of a closed dynamic union
  override def sortOrder: Int = CypherTypeNameOrder.CLOSED_DYNAMIC_UNION.id

  // Property value expands to a closed dynamic union of all property types
  def expandToTypes: List[CypherTypeName] = List(
    BooleanTypeName(isNullable)(position),
    StringTypeName(isNullable)(position),
    IntegerTypeName(isNullable)(position),
    FloatTypeName(isNullable)(position),
    DateTypeName(isNullable)(position),
    LocalTimeTypeName(isNullable)(position),
    ZonedTimeTypeName(isNullable)(position),
    LocalDateTimeTypeName(isNullable)(position),
    ZonedDateTimeTypeName(isNullable)(position),
    DurationTypeName(isNullable)(position),
    PointTypeName(isNullable)(position),
    ListTypeName(BooleanTypeName(isNullable = false)(position), isNullable = isNullable)(position),
    ListTypeName(StringTypeName(isNullable = false)(position), isNullable = isNullable)(position),
    ListTypeName(IntegerTypeName(isNullable = false)(position), isNullable = isNullable)(position),
    ListTypeName(FloatTypeName(isNullable = false)(position), isNullable = isNullable)(position),
    ListTypeName(DateTypeName(isNullable = false)(position), isNullable = isNullable)(position),
    ListTypeName(LocalTimeTypeName(isNullable = false)(position), isNullable = isNullable)(position),
    ListTypeName(ZonedTimeTypeName(isNullable = false)(position), isNullable = isNullable)(position),
    ListTypeName(LocalDateTimeTypeName(isNullable = false)(position), isNullable = isNullable)(position),
    ListTypeName(ZonedDateTimeTypeName(isNullable = false)(position), isNullable = isNullable)(position),
    ListTypeName(DurationTypeName(isNullable = false)(position), isNullable = isNullable)(position),
    ListTypeName(PointTypeName(isNullable = false)(position), isNullable = isNullable)(position)
  )

  override def updateIsNullable(isNullable: Boolean): CypherTypeName = this.copy(isNullable = isNullable)(position)

  def withPosition(newPosition: InputPosition): CypherTypeName = this.copy()(position = newPosition)
}

case class ClosedDynamicUnionTypeName(innerTypes: Set[CypherTypeName])(val position: InputPosition)
    extends CypherTypeName {
  // Dynamic Union Types are not allowed to declare NOT NULL, nullability is based on the inner types nullability
  // e.g ANY<INTEGER NOT NULL | FLOAT NOT NULL> does not contain null
  // ANY<INTEGER | FLOAT> does
  // ANY<INTEGER NOT NULL | FLOAT> is invalid and will produce a semantic error

  val sortedInnerTypes: List[CypherTypeName] = innerTypes.map(_.simplify).toList.sorted
  override def isNullable: Boolean = sortedInnerTypes.head.isNullable

  override def description: String = typeName

  protected val typeName: String = sortedInnerTypes.map(_.description).mkString(" | ")

  override def semanticCheck: SemanticCheck = {
    // All types are nullable or all are not nullable
    // The following throw semantic errors:
    //    * ANY<INTEGER | FLOAT NOT NULL>
    //    * ANY<INTEGER NOT NULL | FLOAT>
    // NOTE: NOTHING is always NOT NULL, but should work e.g ANY<NOTHING | BOOLEAN> is valid
    if (
      !(sortedInnerTypes.forall(innerType => innerType.isNullable || innerType.isInstanceOf[NothingTypeName]) ||
        sortedInnerTypes.forall(!_.isNullable))
    )
      error("All types in a Closed Dynamic Union must be nullable, or be appended with `NOT NULL`", position)
    else sortedInnerTypes.foldSemanticCheck(_.semanticCheck)
  } chain super.semanticCheck

  override def simplify: CypherTypeName = {
    val flattenedInner = sortedInnerTypes.flatMap {
      case c: ClosedDynamicUnionTypeName => c.sortedInnerTypes.map(_.simplify)
      case other                         => List(other.simplify)
    }.toSet
    if (flattenedInner.size == 1) flattenedInner.head else this.copy(flattenedInner)(position)
  }

  override def sortOrder: Int = CypherTypeNameOrder.CLOSED_DYNAMIC_UNION.id

  override def updateIsNullable(isNullable: Boolean): CypherTypeName = this

  def withPosition(newPosition: InputPosition): CypherTypeName = this.copy()(position = newPosition)

  // Don't check the non-simplified types in the equals
  override def equals(other: Any): Boolean = other match {
    case otherUnion: ClosedDynamicUnionTypeName =>
      sortedInnerTypes.equals(otherUnion.sortedInnerTypes)
    case _ => false
  }
}

case class AnyTypeName(isNullable: Boolean)(val position: InputPosition) extends CypherTypeName {
  protected val typeName: String = "ANY"

  override def sortOrder: Int = CypherTypeNameOrder.ANY.id

  override def updateIsNullable(isNullable: Boolean): CypherTypeName = this.copy(isNullable = isNullable)(position)

  override def isSubtypeOf(otherCypherTypeName: CypherTypeName): Boolean = otherCypherTypeName match {
    case _: AnyTypeName => isNullableSubtypeOf(this, otherCypherTypeName)
    case _              => false
  }

  def withPosition(newPosition: InputPosition): CypherTypeName = this.copy()(position = newPosition)
}
