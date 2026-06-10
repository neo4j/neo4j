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
package org.neo4j.cypher.internal.ast.semantics

import org.neo4j.cypher.internal.ast.CypherTypeName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.AnyType
import org.neo4j.cypher.internal.util.symbols.BooleanType
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.DateType
import org.neo4j.cypher.internal.util.symbols.DurationType
import org.neo4j.cypher.internal.util.symbols.FloatType
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.cypher.internal.util.symbols.LocalDateTimeType
import org.neo4j.cypher.internal.util.symbols.LocalTimeType
import org.neo4j.cypher.internal.util.symbols.PointType
import org.neo4j.cypher.internal.util.symbols.PropertyValueCypher5Type
import org.neo4j.cypher.internal.util.symbols.PropertyValueType
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.cypher.internal.util.symbols.UUIDType
import org.neo4j.cypher.internal.util.symbols.VectorType
import org.neo4j.cypher.internal.util.symbols.ZonedDateTimeType
import org.neo4j.cypher.internal.util.symbols.ZonedTimeType
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.values.storable.VectorValue
import org.neo4j.values.storable.VectorValue.MAX_VECTOR_DIMENSIONS
import org.neo4j.values.storable.VectorValue.MIN_VECTOR_DIMENSIONS

import scala.jdk.OptionConverters.RichOptional

object CypherTypeChecking extends SemanticAnalysisTooling {

  // Note: vectors are handled separately below, as there is one CypherType for each dimension and coordinate combo
  private val allowedPropertyTypes = List(
    BooleanType(isNullable = true)(InputPosition.NONE),
    StringType(isNullable = true)(InputPosition.NONE),
    UUIDType(isNullable = true)(InputPosition.NONE),
    IntegerType(isNullable = true)(InputPosition.NONE),
    FloatType(isNullable = true)(InputPosition.NONE),
    DateType(isNullable = true)(InputPosition.NONE),
    LocalTimeType(isNullable = true)(InputPosition.NONE),
    ZonedTimeType(isNullable = true)(InputPosition.NONE),
    LocalDateTimeType(isNullable = true)(InputPosition.NONE),
    ZonedDateTimeType(isNullable = true)(InputPosition.NONE),
    DurationType(isNullable = true)(InputPosition.NONE),
    PointType(isNullable = true)(InputPosition.NONE),
    ListType(BooleanType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE),
    ListType(StringType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE),
    ListType(UUIDType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE),
    ListType(IntegerType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE),
    ListType(FloatType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE),
    ListType(DateType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE),
    ListType(LocalTimeType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE),
    ListType(ZonedTimeType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE),
    ListType(LocalDateTimeType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE),
    ListType(ZonedDateTimeType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE),
    ListType(DurationType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE),
    ListType(PointType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE)
  )

  private val allowedPropertyTypesInGraphType = List(
    AnyType(isNullable = false)(InputPosition.NONE),
    BooleanType(isNullable = false)(InputPosition.NONE),
    StringType(isNullable = false)(InputPosition.NONE),
    UUIDType(isNullable = false)(InputPosition.NONE),
    IntegerType(isNullable = false)(InputPosition.NONE),
    FloatType(isNullable = false)(InputPosition.NONE),
    DateType(isNullable = false)(InputPosition.NONE),
    LocalTimeType(isNullable = false)(InputPosition.NONE),
    ZonedTimeType(isNullable = false)(InputPosition.NONE),
    LocalDateTimeType(isNullable = false)(InputPosition.NONE),
    ZonedDateTimeType(isNullable = false)(InputPosition.NONE),
    DurationType(isNullable = false)(InputPosition.NONE),
    PointType(isNullable = false)(InputPosition.NONE),
    ListType(BooleanType(isNullable = false)(InputPosition.NONE), isNullable = false)(InputPosition.NONE),
    ListType(StringType(isNullable = false)(InputPosition.NONE), isNullable = false)(InputPosition.NONE),
    ListType(UUIDType(isNullable = false)(InputPosition.NONE), isNullable = false)(InputPosition.NONE),
    ListType(IntegerType(isNullable = false)(InputPosition.NONE), isNullable = false)(InputPosition.NONE),
    ListType(FloatType(isNullable = false)(InputPosition.NONE), isNullable = false)(InputPosition.NONE),
    ListType(DateType(isNullable = false)(InputPosition.NONE), isNullable = false)(InputPosition.NONE),
    ListType(LocalTimeType(isNullable = false)(InputPosition.NONE), isNullable = false)(InputPosition.NONE),
    ListType(ZonedTimeType(isNullable = false)(InputPosition.NONE), isNullable = false)(InputPosition.NONE),
    ListType(LocalDateTimeType(isNullable = false)(InputPosition.NONE), isNullable = false)(InputPosition.NONE),
    ListType(ZonedDateTimeType(isNullable = false)(InputPosition.NONE), isNullable = false)(InputPosition.NONE),
    ListType(DurationType(isNullable = false)(InputPosition.NONE), isNullable = false)(InputPosition.NONE),
    ListType(PointType(isNullable = false)(InputPosition.NONE), isNullable = false)(InputPosition.NONE)
  ) ++ allowedPropertyTypes

  def checkPropertyTypeForConstraint(
    originalPropertyType: CypherType,
    normalizedPropertyType: CypherType,
    errorFn: (CypherType, String, Option[ErrorGqlStatusObject]) => SemanticError
  ): SemanticCheck = checkPropertyTypes(originalPropertyType, normalizedPropertyType, errorFn, allowedPropertyTypes)

  /**
   * Graph Types support NOT NULL variants, which will translate into a property type constraint and a property existence constraint
   * Graph Types support ANY NOT NULL as an alias for a property existence constraint
   * */
  def checkPropertyTypeForGraphType(
    originalPropertyType: CypherType,
    normalizedPropertyType: CypherType,
    errorFn: (CypherType, String, Option[ErrorGqlStatusObject]) => SemanticError
  ): SemanticCheck =
    checkPropertyTypes(originalPropertyType, normalizedPropertyType, errorFn, allowedPropertyTypesInGraphType)

  private def checkPropertyTypes(
    originalPropertyType: CypherType,
    normalizedPropertyType: CypherType,
    errorFn: (CypherType, String, Option[ErrorGqlStatusObject]) => SemanticError,
    allowedTypes: List[CypherType]
  ): SemanticCheck = {

    def allowedTypesCheck = {
      def anyPropertyValueType(pt: CypherType): Boolean = pt match {
        case _: PropertyValueType        => true
        case _: PropertyValueCypher5Type => true
        case l: ListType                 => anyPropertyValueType(l.innerType)
        case c: ClosedDynamicUnionType   => c.sortedInnerTypes.map(anyPropertyValueType).exists(b => b)
        case _                           => false
      }
      val containsPropertyValueType = anyPropertyValueType(originalPropertyType)

      val onlyAllowedTypes = normalizedPropertyType match {
        case v: VectorType => checkVectorAllowed(v)
        case c: ClosedDynamicUnionType =>
          c.sortedInnerTypes.forall(p =>
            allowedTypes.contains(p.withPosition(InputPosition.NONE)) ||
              p.isInstanceOf[VectorType] && checkVectorAllowed(p.asInstanceOf[VectorType])
          )
        case _ =>
          allowedTypes.contains(normalizedPropertyType.withPosition(InputPosition.NONE))
      }

      if (containsPropertyValueType || !onlyAllowedTypes) {
        def additionalErrorInfo(pt: CypherType): (String, Option[ErrorGqlStatusObject]) = pt match {
          case ListType(_: ListType, _) =>
            (
              " Lists cannot have lists as an inner type.",
              Some(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB9)
                .withParam(GqlParams.StringParam.typeDescription, "a list")
                .build())
            )
          case ListType(_: ClosedDynamicUnionType, _) =>
            (
              " Lists cannot have a union of types as an inner type.",
              Some(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB9)
                .withParam(GqlParams.StringParam.typeDescription, "a union of types")
                .build())
            )
          case ListType(_: VectorType, _) =>
            (
              " Lists cannot have a vector as an inner type.",
              Some(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB9)
                .withParam(GqlParams.StringParam.typeDescription, "a vector")
                .build())
            )
          case ListType(inner, _) if inner.isNullable =>
            (
              " Lists cannot have nullable inner types.",
              Some(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB9)
                .withParam(GqlParams.StringParam.typeDescription, "a nullable type")
                .build())
            )
          case VectorType(inner, dim, _) if inner.isEmpty || dim.isEmpty =>
            (
              " Property type constraints for vectors need to define both coordinate type and dimension.",
              Some(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NBA).build())
            )
          case VectorType(_, Some(dim), _) if dim < MIN_VECTOR_DIMENSIONS || dim > MAX_VECTOR_DIMENSIONS =>
            (
              s" The dimension of property type constraints for vectors needs to be between $MIN_VECTOR_DIMENSIONS and $MAX_VECTOR_DIMENSIONS.",
              Some(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N31)
                .withParam(GqlParams.StringParam.component, "DIMENSION")
                .withParam(GqlParams.StringParam.valueType, "INTEGER NOT NULL")
                .withParam(GqlParams.NumberParam.lower, VectorValue.MIN_VECTOR_DIMENSIONS)
                .withParam(GqlParams.NumberParam.upper, VectorValue.MAX_VECTOR_DIMENSIONS)
                .withParam(GqlParams.StringParam.value, dim.toString).build())
            )
          case c: ClosedDynamicUnionType
            if c.sortedInnerTypes.exists(inner => inner.isInstanceOf[ListType] || inner.isInstanceOf[VectorType]) =>
            // If we have lists we want to check them for the above cases as well
            // Unions within unions should have been flattened in parsing so won't be handled here
            c.sortedInnerTypes.filter(inner => inner.isInstanceOf[ListType] || inner.isInstanceOf[VectorType])
              .map(additionalErrorInfo)
              .find(inner => inner._1.nonEmpty)
              .getOrElse(("", None))
          case _ => ("", None)
        }

        // Don't expand the PROPERTY VALUE in error message as that makes it confusing as to why it's not allowed.
        // Similarly, it shouldn't get any additional error messages for being a union in a list,
        // in case of LIST<PROPERTY VALUE>, as that isn't the main reason for failure.
        val (propertyType, additionalError) =
          if (containsPropertyValueType) (originalPropertyType, additionalErrorInfo(originalPropertyType))
          else (normalizedPropertyType, additionalErrorInfo(normalizedPropertyType))

        error(errorFn(propertyType, additionalError._1, additionalError._2))
      } else SemanticCheck.success
    }

    // We want run the semantic checks for the types themselves, but the error messages might not make sense for independent constraints,
    // there isn't much point telling users to make all their union types NOT NULL if that is not accepted.
    // Since property types in element types accept NOT NULL as well, they should keep the error as a cause for their failure.
    // Can't use `isNotNullContaining` on the CypherType as that looks at the inner types of the lists
    // which, since they have NOT NULL, makes the check always evaluate to true
    val keepTypeErrorCause = allowedTypes.exists(!_.isNullable)
    CypherTypeName(originalPropertyType).semanticCheck.map {
      case r @ SemanticCheckResult(_, Nil) => r
      // Catch the case where we have a single cause and set that as cause for why we failed
      // If we have multiple errors we won't know which is the true cause so then we skip adding the cause
      case SemanticCheckResult(state, cause :: Nil) if keepTypeErrorCause =>
        val innerCause =
          if (cause.gqlStatusObject.gqlStatus().equals(GqlStatusInfoCodes.STATUS_42001.getStatusString))
            cause.gqlStatusObject.cause().toScala
          else Some(cause.gqlStatusObject)

        SemanticCheckResult(
          state,
          Seq(errorFn(originalPropertyType, "", innerCause))
        )
      case SemanticCheckResult(state, _) => SemanticCheckResult(
          state,
          Seq(errorFn(originalPropertyType, "", None))
        )
    } chain allowedTypesCheck
  }

  private def checkVectorAllowed(vectorType: VectorType): Boolean = {
    vectorType match {
      case VectorType(Some(_), Some(dim), true) =>
        dim >= MIN_VECTOR_DIMENSIONS && dim <= MAX_VECTOR_DIMENSIONS
      case _ => false
    }
  }

}
