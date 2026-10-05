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
package org.neo4j.cypher.internal.util

import org.neo4j.exceptions.Neo4jException
import org.neo4j.exceptions.SyntaxException
import org.neo4j.gqlstatus.CommonGqlStatusObjectImplementation
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlHelper
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes

import scala.jdk.CollectionConverters.SeqHasAsJava

trait CypherExceptionFactory {
  def syntaxException(gqlStatusObject: ErrorGqlStatusObject, message: String, pos: InputPosition): RuntimeException
  def syntaxException(gqlStatusObject: ErrorGqlStatusObject, pos: InputPosition, cause: Throwable): RuntimeException

  def internalError(message: String, pos: InputPosition): RuntimeException = {
    syntaxException(GqlHelper.get50N00(this.getClass.getSimpleName, message), message, pos)
  }

  def insertExistsInOtherLanguageVersion(
    unsupportedVersion: Int,
    supportedVersion: Int,
    ex: SyntaxException
  ): RuntimeException = {
    val exceptionCause = ex.gqlStatusObject()
    val (offset, line, col) = exceptionCause match {
      case c: ErrorGqlStatusObjectImplementation =>
        val pos = c.getDiagnosticPosition
        (
          pos.get("offset").intValue(),
          pos.get("line").intValue(),
          pos.get("column").intValue()
        )
      case _ => (0, 1, 1)
    }
    val gql_42I67 =
      ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I67)
        .atPosition(offset, line, col)
        .withParam(GqlParams.NumberParam.version1, unsupportedVersion)
        .withParam(GqlParams.NumberParam.version2, supportedVersion).buildImpl()

    val gql = exceptionCause match {
      case c: ErrorGqlStatusObjectImplementation =>
        c.insertCause(gql_42I67)
      case _ =>
        // It is possible we will never end up in this case, but in that case we make sure that all codes are preserved.
        gql_42I67.setCause(exceptionCause)
        gql_42I67
    }
    new SyntaxException(gql, ex.getMessage)
  }

  def unsupportedRequestOnSystemDatabaseException(
    invalidInput: String,
    legacyMessage: String,
    pos: InputPosition
  ): RuntimeException = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(pos.offset, pos.line, pos.column)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N17)
          .withParam(GqlParams.StringParam.input, invalidInput)
          .atPosition(pos.offset, pos.line, pos.column)
          .build()
      )
      .build()
    syntaxException(gql, legacyMessage, pos)
  }

  def invalidInputException(
    wrongInput: String,
    forField: String,
    expectedInput: List[String],
    legacyMessage: String,
    pos: InputPosition
  ): RuntimeException = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(pos.offset, pos.line, pos.column)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N04)
          .withParam(GqlParams.StringParam.input, wrongInput)
          .withParam(GqlParams.StringParam.context, forField)
          .withParam(GqlParams.ListParam.inputList, expectedInput.asJava)
          .atPosition(pos.offset, pos.line, pos.column)
          .build()
      )
      .build()
    syntaxException(gql, legacyMessage, pos)
  }

  def invalidInputException(
    wrongInput: String,
    expectedInput: List[String],
    legacyMessage: String,
    pos: InputPosition
  ): RuntimeException = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(pos.offset, pos.line, pos.column)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I06)
          .atPosition(pos.offset, pos.line, pos.column)
          .withParam(GqlParams.StringParam.input, wrongInput)
          .withParam(GqlParams.ListParam.valueList, expectedInput.asJava)
          .build()
      ).build()
    syntaxException(gql, legacyMessage, pos)
  }

  def invalidGlobEscaping(pos: InputPosition): RuntimeException = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(pos.offset, pos.line, pos.column)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I60)
          .atPosition(pos.offset, pos.line, pos.column)
          .build()
      ).build()

    syntaxException(
      gql,
      "Each part of the glob (a block of text up until a dot) must either be fully escaped or not escaped at all.",
      pos
    )
  }

  def missingLookupIndexFunctionName(pos: InputPosition): RuntimeException = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(pos.offset, pos.line, pos.column)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I61)
          .atPosition(pos.offset, pos.line, pos.column)
          .build()
      ).build()

    syntaxException(
      gql,
      "Missing function name for the LOOKUP INDEX",
      pos
    )

  }

  def invalidGraphReferenceFormat(
    wrongInput: String,
    pos: InputPosition
  ): RuntimeException = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(pos.offset, pos.line, pos.column)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NAA)
          .withParam(GqlParams.StringParam.input, wrongInput)
          .atPosition(pos.offset, pos.line, pos.column)
          .build()
      )
      .build()
    syntaxException(
      gql,
      "Incorrectly formatted graph reference '%s'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
        .formatted(wrongInput),
      pos
    )
  }

  def invalidUseOfAggregationInOrderBy(
    clause: String,
    position: InputPosition
  ): RuntimeException = {
    syntaxException(
      GqlHelper.getGql42001_42N23(clause, position.offset, position.line, position.column),
      s"Cannot use aggregation in ORDER BY if there are no aggregate expressions in the preceding $clause",
      position
    )
  }

  def unsupportedPathSelectorInPathPattern(
    selector: String,
    pathPatternKind: String,
    position: InputPosition
  ): RuntimeException = {
    unsupportedPathSelectorOrPathModeInPathPattern(
      selector,
      position,
      s"Path selectors such as `$selector` are not supported within $pathPatternKind path patterns."
    )
  }

  def unsupportedPathModeInPathPattern(
    explicitPathMode: String,
    pathPatternKind: String,
    position: InputPosition
  ): RuntimeException = {
    unsupportedPathSelectorOrPathModeInPathPattern(
      explicitPathMode,
      position,
      s"Explicit path modes such as `$explicitPathMode` are not supported within $pathPatternKind path patterns."
    )
  }

  def unsupportedPathSelectorOrPathModeInPathPattern(
    selectorOrExplicitPathMode: String,
    position: InputPosition,
    legacyErrorMessage: String
  ): RuntimeException = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N35)
        .atPosition(position.offset, position.line, position.column)
        .withParam(GqlParams.StringParam.selectorOrPathMode, selectorOrExplicitPathMode)
        .build())
      .build()

    syntaxException(
      gql,
      legacyErrorMessage,
      position
    )
  }

  def unsupportedMultiplePropertiesInConstraint(
    constraintType: String,
    position: InputPosition
  ): RuntimeException = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N16)
        .atPosition(position.offset, position.line, position.column)
        .withParam(GqlParams.StringParam.idxType, s"'$constraintType' constraints")
        .build())
      .build()
    syntaxException(gql, s"Constraint type '$constraintType' does not allow multiple properties", position)
  }

  def invalidNormalForm(normalForm: ASTNode): RuntimeException = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(normalForm.position.offset, normalForm.position.line, normalForm.position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N49)
        .atPosition(normalForm.position.offset, normalForm.position.line, normalForm.position.column)
        .withParam(GqlParams.StringParam.input, normalForm.asCanonicalStringVal)
        .build)
      .build

    syntaxException(
      gql,
      "Invalid normal form, expected NFC, NFD, NFKC, NFKD",
      normalForm.position
    )
  }

  def invalidVectorType(vectorType: ASTNode): RuntimeException = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(vectorType.position.offset, vectorType.position.line, vectorType.position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I53)
        .atPosition(vectorType.position.offset, vectorType.position.line, vectorType.position.column)
        .withParam(GqlParams.StringParam.input, vectorType.asCanonicalStringVal)
        .build)
      .build

    syntaxException(
      gql,
      "Invalid vector inner type, expected INTEGER64, INTEGER32, INTEGER16, INTEGER8, FLOAT64, FLOAT32, FLOAT16 or BFLOAT16",
      vectorType.position
    )
  }

  def invalidVectorDistanceMetric(distanceMetric: ASTNode, normFunction: Boolean): RuntimeException = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(distanceMetric.position.offset, distanceMetric.position.line, distanceMetric.position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I62)
        .atPosition(distanceMetric.position.offset, distanceMetric.position.line, distanceMetric.position.column)
        .withParam(GqlParams.StringParam.input, distanceMetric.asCanonicalStringVal)
        .build)
      .build

    if (normFunction) {
      syntaxException(
        gql,
        "Invalid vector distance metric, expected EUCLIDEAN or MANHATTAN",
        distanceMetric.position
      )
    } else {
      syntaxException(
        gql,
        "Invalid vector distance metric, expected EUCLIDEAN, EUCLIDEAN_SQUARED, MANHATTAN, COSINE, DOT or HAMMING",
        distanceMetric.position
      )
    }
  }

  def invalidNotNullClosedDynamicUnion(position: InputPosition): RuntimeException = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I33)
        .atPosition(position.offset, position.line, position.column)
        .build())
      .build()

    syntaxException(
      gql,
      "Closed Dynamic Union Types can not be appended with `NOT NULL`, specify `NOT NULL` on all inner types instead.",
      position
    )
  }

  def duplicateClauseParameter(description: String, position: InputPosition): RuntimeException = {
    val gql = GqlHelper.getGql42001_42N19(description, position.offset, position.line, position.column)
    syntaxException(
      gql,
      s"Duplicated $description parameters",
      position
    )
  }

  def duplicateClause(description: String, position: InputPosition): RuntimeException = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N19)
          .withParam(GqlParams.StringParam.syntax, description)
          .atPosition(position.offset, position.line, position.column)
          .build()
      ).build()

    syntaxException(
      gql,
      s"Duplicate $description clause",
      position
    )
  }

  def stringLiteralWithInvalidQuotes(position: InputPosition): RuntimeException = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I19)
          .atPosition(position.offset, position.line, position.column)
          .build()
      ).build()

    syntaxException(
      gql,
      SyntaxException.QUOTE_MISMATCH_ERROR_MESSAGE,
      position
    )
  }

  def invalidNameTooManyComponents(
    errorTemplate: String,
    context: String,
    maxComponents: Int,
    prettyName: String,
    position: InputPosition
  ): RuntimeException = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
          .atPosition(position.offset, position.line, position.column)
          .withParam(GqlParams.StringParam.input, prettyName)
          .withParam(GqlParams.StringParam.context, context)
          .withCause(
            ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N83)
              .atPosition(position.offset, position.line, position.column)
              .withParam(GqlParams.NumberParam.upper, maxComponents)
              .build()
          )
          .build()
      )
      .build()

    syntaxException(
      gql,
      errorTemplate.formatted(prettyName),
      position
    )
  }

  def invalidCharacterForRemoteAlias(aliasName: String, position: InputPosition): RuntimeException = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
          .atPosition(position.offset, position.line, position.column)
          .withParam(GqlParams.StringParam.input, aliasName)
          .withParam(GqlParams.StringParam.context, "remote alias name")
          .withCause(
            ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N82)
              .atPosition(position.offset, position.line, position.column)
              .withParam(GqlParams.StringParam.input, aliasName)
              .withParam(GqlParams.StringParam.context, "remote alias name")
              .build()
          )
          .build(
          )
      ).build()
    syntaxException(
      gql,
      s"'.' is not a valid character in the remote alias name '$aliasName'. Remote alias names using '.' must be quoted with backticks e.g. `remote.alias`.",
      position
    )
  }

  def invalidUseOfInsert(
    cause: String,
    replacement: String,
    legacyMessage: String,
    position: InputPosition
  ): RuntimeException = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I54)
          .withParam(GqlParams.StringParam.cause, cause)
          .withParam(GqlParams.StringParam.replacement, replacement)
          .atPosition(position.offset, position.line, position.column)
          .build()
      ).build()
    syntaxException(
      gql,
      legacyMessage,
      position
    )
  }

  def duplicatePropertyTypeInGraphTypeElement(
    propertyKey: String,
    legacyMessage: String,
    position: InputPosition
  ): RuntimeException = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NC1)
          .withParam(GqlParams.StringParam.propKey, propertyKey)
          .atPosition(position.offset, position.line, position.column)
          .build()
      ).build()
    syntaxException(
      gql,
      legacyMessage,
      position
    )
  }

}

case class Neo4jCypherExceptionFactory(queryText: String, preParserOffset: Option[InputPosition])
    extends CypherExceptionFactory {

  override def syntaxException(
    gqlStatusObject: ErrorGqlStatusObject,
    message: String,
    pos: InputPosition
  ): Neo4jException =
    syntaxException(gqlStatusObject, message, pos, None)

  override def syntaxException(
    gqlStatusObject: ErrorGqlStatusObject,
    pos: InputPosition,
    cause: Throwable
  ): Neo4jException =
    syntaxException(gqlStatusObject, cause.getMessage, pos, Some(cause))

  private def syntaxException(
    gqlStatusObject: ErrorGqlStatusObject,
    message: String,
    pos: InputPosition,
    cause: Option[Throwable]
  ): Neo4jException = {
    val adjustedPosition = pos.withOffset(preParserOffset)

    // Adjust the position in the GQL object
    val gqlWithAdjustedPosition =
      if (gqlStatusObject != null) {
        val gqlImpl = gqlStatusObject.asInstanceOf[CommonGqlStatusObjectImplementation]
        gqlImpl.adjustPosition(
          pos.offset,
          pos.line,
          pos.column,
          adjustedPosition.offset,
          adjustedPosition.line,
          adjustedPosition.column
        )
        gqlImpl.asInstanceOf[ErrorGqlStatusObject]
      } else {
        gqlStatusObject
      }

    new SyntaxException(
      gqlWithAdjustedPosition,
      message,
      queryText,
      adjustedPosition.toString,
      adjustedPosition.offset,
      cause.orNull
    )
  }

}
