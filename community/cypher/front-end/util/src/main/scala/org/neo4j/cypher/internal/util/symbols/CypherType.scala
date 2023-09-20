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

import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition

trait CypherType extends ASTNode {
  def parentType: CypherType
  val isAbstract: Boolean = false

  // e.g BOOLEAN set(true, false, null) is nullable, BOOLEAN NOT NULL set(true, false) is not
  def isNullable: Boolean
  def hasCypherParserSupport: Boolean = true

  def hasValueRepresentation: Boolean = false

  def updateIsNullable(isNullable: Boolean): CypherType

  def description: String = if (isNullable) toCypherTypeString else s"$toCypherTypeString NOT NULL"

  def withPosition(position: InputPosition): CypherType

  def simplify: CypherType = this

  // A helper method for calling normalize and get string on an individual type, only types that may be normalized should override
  // e.g ClosedDynamicUnionType, ListType etc
  def normalizedCypherTypeString(): String = description

  def isSubtypeOf(otherCypherType: CypherType): Boolean = this match {
    case thisDynamicUnion: ClosedDynamicUnionType => otherCypherType match {
        case otherDynamicUnion: ClosedDynamicUnionType =>
          thisDynamicUnion.innerTypes.forall(otherDynamicUnion.innerTypes.contains)
        case _ => false
      }
    case _ => otherCypherType match {
        case otherDynamicUnion: ClosedDynamicUnionType =>
          otherDynamicUnion.innerTypes.contains(this)
        case _: AnyType => isNullableSubtypeOf(this, otherCypherType)
        case _ =>
          toCypherTypeString.equals(otherCypherType.toCypherTypeString) && isNullableSubtypeOf(
            this,
            otherCypherType
          )
      }
  }

  protected def isNullableSubtypeOf(cypherType: CypherType, otherCypherType: CypherType): Boolean =
    !cypherType.isNullable || (cypherType.isNullable && otherCypherType.isNullable)

  def sortOrder: Int

  def coercibleTo: Set[CypherType] = Set.empty

  def parents: Seq[CypherType] = parents(Vector.empty)

  private def parents(accumulator: Seq[CypherType]): Seq[CypherType] =
    if (this.parentType == this)
      accumulator
    else
      this.parentType.parents(accumulator :+ this.parentType)

  /*
  Determines if the class or interface represented by this
  {@code CypherType} object is either the same as, or is a
  supertype of, the class or interface represented by the
  specified {@code CypherType} parameter.
   */
  def isAssignableFrom(other: CypherType): Boolean =
    if (other == this)
      true
    else if (other.parentType == other)
      false
    else
      isAssignableFrom(other.parentType)

  def legacyIteratedType: CypherType = this

  def leastUpperBound(other: CypherType): CypherType =
    if (this.isAssignableFrom(other)) this
    else if (other.isAssignableFrom(this)) other
    else parentType leastUpperBound other.parentType

  def greatestLowerBound(other: CypherType): Option[CypherType] =
    if (this.isAssignableFrom(other)) Some(other)
    else Some(this).filter(other.isAssignableFrom)

  lazy val covariant: TypeSpec = TypeSpec.all constrain this
  lazy val invariant: TypeSpec = TypeSpec.exact(this)
  lazy val contravariant: TypeSpec = TypeSpec.all leastUpperBounds this

  def rewrite(f: CypherType => CypherType): CypherType = f(this)

  def toCypherTypeString: String
}

object CypherType {

  /**
   * normalizeTypes takes a CypherType and will normalize it to a simpler form.
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
  def normalizeTypes(typeToNormalize: CypherType): CypherType = {
    val simplifiedType = typeToNormalize.simplify
    simplifiedType match {
      case ClosedDynamicUnionType(innerTypes) =>
        val updatedTypes = normalizeInnerTypes(innerTypes)
        ClosedDynamicUnionType(updatedTypes.toSet)(typeToNormalize.position).simplify
      case lt: ListType          => lt.copy(normalizeTypes(lt.innerType))(lt.position)
      case pt: PropertyValueType => ClosedDynamicUnionType(pt.expandToTypes.toSet)(pt.position)
      case numberType: NumberType => ClosedDynamicUnionType(Set(
          IntegerType(numberType.isNullable)(numberType.position),
          FloatType(numberType.isNullable)(numberType.position)
        ))(numberType.position)
      case geometryType: GeometryType => PointType(geometryType.isNullable)(geometryType.position)
      case _                          => simplifiedType
    }
  }

  private def normalizeInnerTypes(innerTypes: Set[CypherType]): List[CypherType] = {
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
  private def normalize(types: List[CypherType]) = {
    // Expand PropertyValueTypeName to all property types instead
    val expandedTypes: List[CypherType] = types.flatMap {
      case propertyValueTypeName: PropertyValueType => propertyValueTypeName.expandToTypes
      case numberType: NumberType => List(
          IntegerType(numberType.isNullable)(numberType.position),
          FloatType(numberType.isNullable)(numberType.position)
        )
      case geometryType: GeometryType => List(PointType(geometryType.isNullable)(geometryType.position))
      case cypherType                 => List(cypherType)
    }
    // Simplify expressions, (for example flatten dynamic unions: ANY<ANY<BOOLEAN>> -> BOOLEAN)
    // and normalize inner types (normalizes nested dynamic unions and lists)
    val simplifiedTypes = expandedTypes.map(_.simplify).map(normalizeTypes).sorted

    // Check for multiples of the same type or subtype, example:
    //  - BOOLEAN | BOOLEAN NOT NULL -> BOOLEAN
    //  - BOOLEAN | BOOLEAN (after flattening BOOLEAN | ANY<BOOLEAN>) -> BOOLEAN
    //  - LIST<BOOLEAN> | LIST<BOOLEAN | INTEGER> -> LIST<BOOLEAN | INTEGER>
    val combinedTypes = simplifiedTypes.foldLeft(List.empty[CypherType]) {
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
      updatedTypes = List(AnyType(updatedTypes.head.isNullable)(updatedTypes.map(_.position).min))

    // Sort types
    updatedTypes.sorted
  }

  private def allTypes(isNullable: Boolean): Set[CypherType] = Set(
    BooleanType(isNullable)(InputPosition.NONE),
    StringType(isNullable)(InputPosition.NONE),
    IntegerType(isNullable)(InputPosition.NONE),
    FloatType(isNullable)(InputPosition.NONE),
    DateType(isNullable)(InputPosition.NONE),
    LocalTimeType(isNullable)(InputPosition.NONE),
    ZonedTimeType(isNullable)(InputPosition.NONE),
    LocalDateTimeType(isNullable)(InputPosition.NONE),
    ZonedDateTimeType(isNullable)(InputPosition.NONE),
    DurationType(isNullable)(InputPosition.NONE),
    PointType(isNullable)(InputPosition.NONE),
    NodeType(isNullable)(InputPosition.NONE),
    RelationshipType(isNullable)(InputPosition.NONE),
    MapType(isNullable)(InputPosition.NONE),
    ListType(AnyType(isNullable = true)(InputPosition.NONE), isNullable = isNullable)(InputPosition.NONE),
    PathType(isNullable)(InputPosition.NONE)
  )

  implicit val order: Ordering[CypherType] = {
    // TYPE NOT NULL should be before TYPE
    // comparing boolean values gives false before true, so isNullable false comes first as wanted
    (x: CypherType, y: CypherType) =>
      (x, y) match {
        case (lx: ListType, ly: ListType)                               => compareListTypes(lx, ly)
        case (cux: ClosedDynamicUnionType, cuy: ClosedDynamicUnionType) =>
          // Sorting multiple closed dynamic unions
          compareInnerLists(cux.sortedInnerTypes, cuy.sortedInnerTypes)
        case (tx, ty) if tx.sortOrder == ty.sortOrder => x.isNullable.compare(y.isNullable)
        case _                                        => x.sortOrder.compare(y.sortOrder)
      }
  }

  private def compareListTypes(x: ListType, y: ListType): Int = {
    val innerComparison = (x.innerType, y.innerType) match {
      case (hx: ListType, hy: ListType) => compareListTypes(hx, hy)
      case (hx: ClosedDynamicUnionType, hy: ClosedDynamicUnionType) =>
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

  private def compareInnerLists(x: List[CypherType], y: List[CypherType]): Int = {
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
              ListType(lxDynamicUnion: ClosedDynamicUnionType, _),
              ListType(lyDynamicUnion: ClosedDynamicUnionType, _)
            ) =>
            compareInnerLists(lxDynamicUnion.sortedInnerTypes, lyDynamicUnion.sortedInnerTypes)
          case (
              ListType(lxListType: ListType, _),
              ListType(lyListType: ListType, _)
            ) =>
            compareListTypes(lxListType, lyListType)
          case (ListType(lxInnerType: CypherType, _), ListType(lyInnerType: CypherType, _)) =>
            val innerTypeOrder = lxInnerType.sortOrder.compare(lyInnerType.sortOrder)
            if (innerTypeOrder == 0) lxInnerType.isNullable.compare(lyInnerType.isNullable)
            else innerTypeOrder
          case (cux: ClosedDynamicUnionType, cuy: ClosedDynamicUnionType) =>
            compareInnerLists(cux.sortedInnerTypes, cuy.sortedInnerTypes)
          case _ => sortOrder
        }

        if (innerComparison == 0) hx.isNullable.compare(hy.isNullable)
        else innerComparison
      } else sortOrder
    }
  }
}
