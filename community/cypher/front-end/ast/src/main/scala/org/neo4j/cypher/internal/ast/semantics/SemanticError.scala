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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.LocalFieldSignature
import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.StaticElementTypeName
import org.neo4j.cypher.internal.expressions.functions.AllReduce
import org.neo4j.cypher.internal.util.CallableName
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.ProcedureName
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlHelper
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes

import scala.jdk.CollectionConverters.SeqHasAsJava

sealed trait SemanticErrorDef {
  def msg: String
  def position: InputPosition
  def gqlStatusObject: ErrorGqlStatusObject
  def withMsg(error: SemanticError): SemanticErrorDef
}

final case class SemanticError(
  override val gqlStatusObject: ErrorGqlStatusObject,
  override val msg: String,
  override val position: InputPosition
) extends SemanticErrorDef {
  override def withMsg(error: SemanticError): SemanticError = error
}

object SemanticError {

  def unapply(errorDef: SemanticErrorDef): Option[(ErrorGqlStatusObject, String, InputPosition)] =
    Some((errorDef.gqlStatusObject, errorDef.msg, errorDef.position))

  def internalError(msgTitle: String, msg: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.get50N00(msgTitle, msg, position.offset, position.line, position.column)
    SemanticError(gql, msg, position)
  }

  def invalidOption(
    invalidOptionsString: String,
    validOptions: Seq[String],
    errorMessageOverride: Option[String],
    position: InputPosition
  ): SemanticError = {
    val gql = GqlHelper.getGql42001_22N04(
      invalidOptionsString,
      GqlParams.StringParam.input.process("OPTIONS"),
      validOptions.asJava,
      position.offset,
      position.line,
      position.column
    )
    SemanticError(
      gql,
      errorMessageOverride.getOrElse(GqlHelper.getCompleteMessage(gql)),
      position
    )
  }

  def authForbidsClauseError(
    provider: String,
    unsupportedClause: String,
    expected: java.util.List[String],
    position: InputPosition
  ): SemanticError = {
    val gql = GqlHelper.getGql42001_22N04(
      unsupportedClause,
      "auth provider " + GqlParams.StringParam.input.process(provider) + " attribute",
      expected,
      position.offset,
      position.line,
      position.column
    )
    SemanticError(
      gql,
      s"Auth provider `$provider` does not allow `$unsupportedClause` clause.",
      position
    )
  }

  def unsupportedActionAccess(
    actionName: String,
    expectedActions: java.util.List[String],
    position: InputPosition
  ): SemanticError = {
    val gql = GqlHelper.getGql42001_22N04(
      actionName,
      "property value access rules",
      expectedActions,
      position.offset,
      position.line,
      position.column
    )
    SemanticError(gql, s"$actionName is not supported for property value access rules.", position)
  }

  def mixedListInPBAC(expression: String, position: InputPosition): SemanticError = {

    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA0)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NAB)
        .withParam(GqlParams.StringParam.expr, expression)
        .atPosition(position.offset, position.line, position.column)
        .build()).build()

    val legacyMsg =
      s"Failed to administer property rule. The expression: `$expression` is not supported. All elements in a list must be literals of the same type for property-based access control."
    SemanticError(gql, legacyMsg, position)

  }

  def yieldMissingColumn(
    originalName: String,
    expectedColumns: java.util.List[String],
    position: InputPosition
  ): SemanticError = {
    val gql = GqlHelper.getGql42001_22N04(
      originalName,
      "column name",
      expectedColumns,
      position.offset,
      position.line,
      position.column
    )
    SemanticError(gql, s"Trying to YIELD non-existing column: `$originalName`", position)
  }

  def invalidFunctionForIndex(
    entityIndexDescription: String,
    name: String,
    validFunction: String,
    position: InputPosition
  ): SemanticError = {
    val gql = GqlHelper.getGql42001_22N04(
      s"$name()",
      s"function in $entityIndexDescription",
      java.util.List.of(s"$validFunction()"),
      position.offset,
      position.line,
      position.column
    )
    SemanticError(
      gql,
      s"Failed to create $entityIndexDescription: Function '$name' is not allowed, valid function is '$validFunction'.",
      position
    )
  }

  def invalidUseOfGraphFunction(graphFunction: String, pos: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(pos.offset, pos.line, pos.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N75)
        .atPosition(pos.offset, pos.line, pos.column)
        .withParam(GqlParams.StringParam.fun, graphFunction)
        .build())
      .build()
    SemanticError(
      gql,
      s"`$graphFunction` is only allowed at the first position of a USE clause.",
      pos
    )
  }

  private val existsErrorMessage = "The EXISTS expression is not valid in driver settings."
  private val countErrorMessage = "The COUNT expression is not valid in driver settings."
  private val collectErrorMessage = "The COLLECT expression is not valid in driver settings."
  private val patternExpressionErrorMessage = "Pattern expressions are not valid in driver settings."
  private val patternComprehensionErrorMessage = "Pattern comprehensions are not valid in driver settings."
  private val genericErrorMessage = "This expression is not valid in driver settings."

  def existsInDriverSettings(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql22N81(
      GqlParams.StringParam.cmd.process("EXISTS"),
      "driver settings",
      position.offset,
      position.line,
      position.column
    )
    SemanticError(gql, existsErrorMessage, position)
  }

  def countInDriverSettings(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql22N81(
      GqlParams.StringParam.cmd.process("COUNT"),
      "driver settings",
      position.offset,
      position.line,
      position.column
    )
    SemanticError(gql, countErrorMessage, position)
  }

  def collectInDriverSettings(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql22N81(
      GqlParams.StringParam.cmd.process("COLLECT"),
      "driver settings",
      position.offset,
      position.line,
      position.column
    )
    SemanticError(gql, collectErrorMessage, position)
  }

  def patternExpressionInDriverSettings(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql22N81(
      GqlParams.StringParam.cmd.process("pattern expression"),
      "driver settings",
      position.offset,
      position.line,
      position.column
    )
    SemanticError(gql, patternExpressionErrorMessage, position)
  }

  def patternComprehensionInDriverSettings(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql22N81(
      GqlParams.StringParam.cmd.process("pattern comprehension"),
      "driver settings",
      position.offset,
      position.line,
      position.column
    )
    SemanticError(gql, patternComprehensionErrorMessage, position)
  }

  def genericDriverSettingsFail(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql22N81(
      GqlParams.StringParam.cmd.process("subquery expression"),
      "driver settings",
      position.offset,
      position.line,
      position.column
    )
    SemanticError(gql, genericErrorMessage, position)
  }

  def cannotUseJoinHint(hint: UsingJoinHint, prettifiedHint: String): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N76)
      .withParam(GqlParams.ListParam.hintList, Seq(prettifiedHint).asJava)
      .atPosition(hint.position.offset, hint.position.line, hint.position.column)
      .build()
    SemanticError(gql, "Cannot use join hint for single node pattern.", hint.position)
  }

  def variableAlreadyDeclaredInOuterScope(name: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N07(name, position.offset, position.line, position.column)
    SemanticError(gql, s"Variable `$name` already declared in outer scope", position)
  }

  def variableShadowingOuterScope(name: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N07(name, position.offset, position.line, position.column)
    SemanticError(
      gql,
      s"The variable `$name` is shadowing a variable with the same name from the outer scope and needs to be renamed",
      position
    )
  }

  def legacyDisjunction(
    sanitizedLabelExpression: String,
    containsIs: Boolean,
    isNode: Boolean = false,
    position: InputPosition
  ): SemanticError = {
    val isOrColon = if (containsIs) "IS " else ":"
    val msg = if (isNode) {
      s"""Label expressions are not allowed to contain '|:'.
         |If you want to express a disjunction of labels, please use `$isOrColon$sanitizedLabelExpression` instead""".stripMargin
    } else {
      s"""The semantics of using colon in the separation of alternative relationship types in conjunction with
         |the use of variable binding, inlined property predicates, or variable length is no longer supported.
         |Please separate the relationships types using `$isOrColon$sanitizedLabelExpression` instead.""".stripMargin
    }
    val gql = GqlHelper.getGql42001_42I20(
      "|:",
      "|",
      position.offset,
      position.line,
      position.column
    )
    SemanticError(gql, msg, position)
  }

  def invalidDisjunction(isNode: Boolean, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42I20(
      if (isNode) "|:" else ":",
      "|",
      position.offset,
      position.line,
      position.column
    )
    SemanticError(
      gql,
      if (isNode) "Label expressions are not allowed to contain '|:'."
      else "Relationship types in a relationship type expressions may not be combined using ':'",
      position
    )
  }

  def subPathAssignmentNotSupported(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N42(position.offset, position.line, position.column)
    SemanticError(gql, "Sub-path assignment is currently not supported.", position)
  }

  def unsupportedRequestOnSystemDatabase(
    invalidInput: String,
    legacyMessage: String,
    pos: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N17)
          .withParam(GqlParams.StringParam.input, invalidInput)
          .build()
      )
      .build()
    SemanticError(
      gql,
      legacyMessage,
      pos
    )
  }

  def useClauseWithAdministrationCommand(pos: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N79).build()
    SemanticError(
      gql,
      "The `USE` clause is not required for administration commands. Retry your query without the `USE` clause, and it will be routed automatically.",
      pos
    )
  }

  def invalidInput(
    wrongInput: String,
    forField: String,
    expectedInput: List[String],
    legacyMessage: String,
    pos: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(pos.offset, pos.line, pos.column)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N04)
          .atPosition(pos.offset, pos.line, pos.column)
          .withParam(GqlParams.StringParam.input, wrongInput)
          .withParam(GqlParams.StringParam.context, forField)
          .withParam(GqlParams.ListParam.inputList, java.util.List.of(expectedInput))
          .build()
      )
      .build()
    SemanticError(
      gql,
      legacyMessage,
      pos
    )
  }

  def invalidEntityType(
    invalidInput: String,
    variable: String,
    expectedValueList: Seq[String],
    legacyMessage: String,
    pos: InputPosition
  ): SemanticError = {
    val gql = GqlHelper.getGql22G03_22N27(
      invalidInput,
      variable,
      expectedValueList.asJava,
      pos.offset,
      pos.line,
      pos.column
    )

    SemanticError(
      gql,
      legacyMessage,
      pos
    )
  }

  def invalidEntityTypeWithPropertiesHint(
    invalidInput: String,
    variable: String,
    expectedValueList: Seq[String],
    legacyMessage: String,
    pos: InputPosition
  ): SemanticError = {
    val gql = GqlHelper.getGql22G03_22N27WithHint(
      invalidInput,
      variable,
      expectedValueList.asJava,
      " Hint: use properties(...) on the right-hand side.",
      pos.offset,
      pos.line,
      pos.column
    )

    SemanticError(
      gql,
      legacyMessage,
      pos
    )
  }

  def typeMismatch(
    expectedValueList: List[String],
    wrongType: String,
    legacyMessage: String,
    pos: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(pos.offset, pos.line, pos.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB1)
        .atPosition(pos.offset, pos.line, pos.column)
        .withParam(GqlParams.ListParam.valueTypeList, expectedValueList.asJava)
        .withParam(GqlParams.StringParam.input, wrongType)
        .build())
      .build()

    SemanticError(
      gql,
      legacyMessage,
      pos
    )
  }

  def invalidCoercion(
    cannotCoerceFrom: String,
    cannotCoerceTo: String,
    legacyMessage: String,
    pos: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(pos.offset, pos.line, pos.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N37)
        .atPosition(pos.offset, pos.line, pos.column)
        .withParam(GqlParams.StringParam.value, cannotCoerceFrom)
        .withParam(GqlParams.StringParam.valueType, cannotCoerceTo)
        .build())
      .build()

    SemanticError(
      gql,
      legacyMessage,
      pos
    )
  }

  def specifiedNumberOutOfRange(
    component: String,
    valueType: String,
    lower: Number,
    upper: Number,
    inputValue: String,
    legacyMessage: String,
    pos: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(pos.offset, pos.line, pos.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N31)
        .atPosition(pos.offset, pos.line, pos.column)
        .withParam(GqlParams.StringParam.component, component)
        .withParam(GqlParams.StringParam.valueType, valueType)
        .withParam(GqlParams.NumberParam.lower, lower)
        .withParam(GqlParams.NumberParam.upper, upper)
        .withParam(GqlParams.StringParam.value, inputValue).build())
      .build()

    SemanticError(
      gql,
      legacyMessage,
      pos
    )
  }

  def propertyTypeUnsupportedInConstraint(
    constraintTypeDescription: String,
    originalPropertyType: CypherType,
    additionalError: String,
    maybeCause: Option[ErrorGqlStatusObject]
  ): SemanticError = {

    val gqlInnerBuilder = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N90)
      .withParam(GqlParams.StringParam.valueType, originalPropertyType.description)

    if (maybeCause.isDefined) gqlInnerBuilder.withCause(maybeCause.get)

    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N11)
      .withParam(GqlParams.StringParam.constrDescrOrName, constraintTypeDescription + " constraint")
      .withCause(gqlInnerBuilder.build())
      .build()
    new SemanticError(
      gql,
      s"Failed to create ${constraintTypeDescription} constraint: " +
        s"Invalid property type `${originalPropertyType.description}`.$additionalError",
      originalPropertyType.position
    )
  }

  def missingMandatoryAuthClause(
    clause: String,
    authProvider: String,
    legacyMessage: String,
    pos: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N97)
      .withParam(GqlParams.StringParam.clause, clause)
      .withParam(GqlParams.StringParam.auth, authProvider)
      .build()
    SemanticError(
      gql,
      legacyMessage,
      pos
    )
  }

  def duplicateClause(clause: String, legacyMessage: String, pos: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N19(clause, pos.offset, pos.line, pos.column)
    SemanticError(
      gql,
      legacyMessage,
      pos
    )
  }

  def missingHintPredicate(
    legacyMessage: String,
    hint: String,
    entity: String,
    variable: String,
    position: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N76)
      .withParam(GqlParams.ListParam.hintList, Seq(hint).asJava)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N77)
        .withParam(GqlParams.StringParam.hint, hint)
        .withParam(GqlParams.StringParam.entityType, entity)
        .withParam(GqlParams.StringParam.variable, variable)
        .atPosition(position.offset, position.line, position.column)
        .build())
      .build()
    SemanticError(gql, legacyMessage, position)
  }

  def functionRequiresWhereClause(func: String, position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N70)
        .atPosition(position.offset, position.line, position.column)
        .withParam(GqlParams.StringParam.fun, func)
        .build())
      .build()
    SemanticError(gql, s"$func(...) requires a WHERE predicate", position)
  }

  def aExpressionCannotContainUpdates(expr: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N57(expr, position.offset, position.line, position.column)
    SemanticError(gql, s"A $expr Expression cannot contain any updates", position)
  }

  def anExpressionCannotContainUpdates(expr: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N57(expr, position.offset, position.line, position.column)
    SemanticError(gql, s"An $expr Expression cannot contain any updates", position)
  }

  def localFunctionCannotContainUpdates(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N57("Local function", position.offset, position.line, position.column)
    SemanticError(gql, s"Local function cannot contain any updates", position)
  }

  def singleReturnColumnRequired(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N22(position.offset, position.line, position.column)
    SemanticError(gql, "A Collect Expression must end with a single return column.", position)
  }

  def emptyListRangeOperator(position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N20)
        .atPosition(position.offset, position.line, position.column)
        .build())
      .build()
    SemanticError(gql, "The start or end (or both) is required for a collection slice", position)
  }

  def unboundVariablesInPatternExpression(name: String, position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N29)
        .atPosition(position.offset, position.line, position.column)
        .withParam(GqlParams.StringParam.variable, name)
        .build())
      .build()
    SemanticError(
      gql,
      s"PatternExpressions are not allowed to introduce new variables: '$name'.",
      position
    )
  }

  def invalidUseOfDynamicLabelOrType(
    entityType: String,
    clause: String,
    pos: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(pos.offset, pos.line, pos.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I55)
        .atPosition(pos.offset, pos.line, pos.column)
        .withParam(GqlParams.StringParam.entityType, entityType)
        .withParam(GqlParams.StringParam.clause, clause)
        .build())
      .build()

    new SemanticError(
      gql,
      s"Dynamic $entityType using `$$any()` are not allowed in CREATE or MERGE.",
      pos
    )
  }

  def dynamicEntityTypeNotAllowed(pos: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(pos.offset, pos.line, pos.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I59)
        .atPosition(pos.offset, pos.line, pos.column)
        .withParam(GqlParams.ListParam.clauseList, List("MATCH", "CREATE", "MERGE", "SET", "REMOVE").asJava)
        .build())
      .build()

    new SemanticError(
      gql,
      "Dynamic Label and Types are only allowed in MATCH, CREATE, MERGE, SET and REMOVE clauses.",
      pos
    )
  }

  def onlyDirectedRelationshipAllowed(clause: String, pos: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(pos.offset, pos.line, pos.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I56)
        .atPosition(pos.offset, pos.line, pos.column)
        .withParam(GqlParams.StringParam.clause, clause)
        .build())
      .build()

    new SemanticError(
      gql,
      s"Only directed relationships are supported in $clause",
      pos
    )
  }

  def invalidEndOfQuery(exprType: String, pos: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(pos.offset, pos.line, pos.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I57)
        .atPosition(pos.offset, pos.line, pos.column)
        .withParam(GqlParams.StringParam.exprType, exprType)
        .withParam(GqlParams.StringParam.clause, "FINISH")
        .build())
      .build()

    new SemanticError(
      gql,
      s"$exprType cannot contain a query ending with FINISH.",
      pos
    )
  }

  def invalidEntityReference(entity: String, pos: InputPosition, msg: String): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(pos.offset, pos.line, pos.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I58)
        .atPosition(pos.offset, pos.line, pos.column)
        .withParam(GqlParams.StringParam.expr, entity)
        .build())
      .build()

    new SemanticError(gql, msg, pos)
  }

  def invalidEntityReference(entity: String, clause: String, pos: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(pos.offset, pos.line, pos.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I58)
        .atPosition(pos.offset, pos.line, pos.column)
        .withParam(GqlParams.StringParam.expr, entity)
        .build())
      .build()

    new SemanticError(
      gql,
      s"Creating an entity ($entity) and referencing that entity in a property definition in the same $clause is not allowed. Only reference variables created in earlier clauses.",
      pos
    )
  }

  def incompatibleWhenReturnColumns(context: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N39(context, position.offset, position.line, position.column)
    SemanticError(gql, gql.cause().get().gqlStatusObject().getMessage, position)
  }

  def incompatibleReturnColumns(context: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N39(context, position.offset, position.line, position.column)
    SemanticError(gql, "All sub queries in an UNION must have the same return column names", position)
  }

  def incompatibleSubqueryType(context: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N3A(context, position.offset, position.line, position.column)
    SemanticError(gql, gql.cause().get().gqlStatusObject().getMessage, position)
  }

  def incompatibleNumberOfReturnColumns(context: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N3B(context, position.offset, position.line, position.column)
    SemanticError(gql, gql.cause().get().gqlStatusObject().getMessage, position)
  }

  def invalidUseOfOldCall(clause: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N3C(clause, position.offset, position.line, position.column)
    SemanticError(gql, gql.cause().get().gqlStatusObject().getMessage, position)
  }

  def invalidUseOfUnion(position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I40)
        .atPosition(position.offset, position.line, position.column)
        .build())
      .build()

    SemanticError(gql, "Invalid combination of UNION and UNION ALL", position)
  }

  def invalidUseOfCIT(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42I25(position.offset, position.line, position.column)
    SemanticError(
      gql,
      "CALL { ... } IN TRANSACTIONS after a write clause is not supported",
      position
    )
  }

  def invalidPositionOfClause(name: String, position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I38)
        .withParam(GqlParams.StringParam.clause, name)
        .atPosition(position.offset, position.line, position.column)
        .build())
      .build()

    SemanticError(gql, s"$name can only be used at the end of the query.", position)
  }

  def invalidSubclauseOrder(name: String, position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I63)
        .withParam(GqlParams.StringParam.clause, name)
        .atPosition(position.offset, position.line, position.column)
        .build())
      .build()

    SemanticError(gql, gql.cause().get().gqlStatusObject().getMessage, position)
  }

  def invalidUseOfReturnStar(position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I37)
        .atPosition(position.offset, position.line, position.column)
        .build())
      .build()
    SemanticError(gql, "RETURN * is not allowed when there are no variables in scope", position)
  }

  def invalidReferenceToNonGroupingExpression(variables: Seq[String], position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I18)
        .atPosition(position.offset, position.line, position.column)
        .withParam(GqlParams.ListParam.variableList, variables.asJava)
        .build())
      .build()

    val errorMsg = implicitGroupingExpressionInAggregationColumnErrorMessage(variables)
    SemanticError(gql, errorMsg, position)
  }

  def invalidGroupingElement(
    element: String,
    alias: String,
    referencesAggregation: Boolean,
    position: InputPosition
  ): SemanticError = {
    val gql =
      GqlHelper.getGql42001_42I80(
        element,
        alias,
        referencesAggregation,
        position.offset,
        position.line,
        position.column
      )
    SemanticError(gql, gql.getMessage, position)
  }

  def invalidReferenceInSubclauseExpression(variables: Seq[String], position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I79)
        .atPosition(position.offset, position.line, position.column)
        .withParam(GqlParams.ListParam.variableList, variables.asJava)
        .build())
      .build()

    val errorMsg = gql.getMessage
    SemanticError(gql, errorMsg, position)
  }

  def invalidForeach(clause: String, position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I01)
        .atPosition(position.offset, position.line, position.column)
        .withParam(GqlParams.StringParam.clause, clause)
        .build())
      .build()

    SemanticError(gql, s"Invalid use of $clause inside FOREACH", position)
  }

  def unaliasedReturnItem(context: String, position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N21)
        .atPosition(position.offset, position.line, position.column)
        .withParam(GqlParams.StringParam.context, context)
        .build())
      .build()

    SemanticError(gql, s"Expression in $context must be aliased (use AS)", position)
  }

  def implicitGroupingExpressionInAggregationColumnErrorMessage(variables: Seq[String]): String =
    "Aggregation column contains implicit grouping expressions. " +
      "For example, in 'RETURN n.a, n.a + n.b + count(*)' the aggregation expression 'n.a + n.b + count(*)' includes the implicit grouping key 'n.b'. " +
      "It may be possible to rewrite the query by extracting these grouping/aggregation expressions into a preceding WITH clause. " +
      s"Illegal expression(s): ${variables.mkString(", ")}"

  def aggregateExpressionsNotAllowedInSimpleExpressions(
    function: String,
    functionName: String,
    position: InputPosition
  ): SemanticError =
    aggregateExpressionsNotAllowed(
      function,
      s"Invalid use of aggregating function $functionName(...) in this context",
      position
    )

  def aggregateExpressionsNotAllowedInAggregationFunctions(function: String, position: InputPosition): SemanticError =
    aggregateExpressionsNotAllowed(
      function,
      "Can't use aggregate functions inside of aggregate functions.",
      position
    )

  def aggregateExpressionsNotAllowedInProcedureCallArgument(arg: String, position: InputPosition): SemanticError =
    aggregateExpressionsNotAllowed(
      arg,
      """Procedure call cannot take an aggregating function as argument, please add a 'WITH' to your statement.
        |For example:
        |    MATCH (n:Person) WITH collect(n.name) AS names CALL proc(names) YIELD value RETURN value""".stripMargin,
      position
    )

  def aggregateExpressionsInOrderBy(aggregateExpressions: Seq[String], position: InputPosition): SemanticError =
    aggregateExpressionsNotAllowed(
      aggregateExpressions.head,
      s"Illegal aggregation expression(s) in order by: ${aggregateExpressions.mkString(", ")}. " +
        "If an aggregation expression is used in order by, it also needs to be a projection item on it's own. " +
        "For example, in 'RETURN n.a, 1 + count(*) ORDER BY count(*) + 1' the aggregation expression 'count(*) + 1' is not a projection " +
        "item on its own, but it could be rewritten to 'RETURN n.a, 1 + count(*) AS cnt ORDER BY 1 + count(*)'.",
      position
    )

  def aggregateExpressionInSubclauseExpression(aggregateExpressions: Seq[String], position: InputPosition)
    : SemanticError =
    aggregateExpressionsNotAllowed(
      aggregateExpressions.head,
      s"Illegal aggregation expression(s) in subclause expression: ${aggregateExpressions.mkString(", ")}. " +
        "If an aggregation expression is used in a subclause (e.g. ORDER BY or WHERE), the clause must " +
        "already be aggregating.",
      position
    )

  private def aggregateExpressionsNotAllowed(
    function: String,
    legacyMessage: String,
    position: InputPosition
  ): SemanticError = {
    val gql = GqlHelper.getGql42001_42I24(function, position.offset, position.line, position.column)
    SemanticError(
      gql,
      legacyMessage,
      position
    )
  }

  def accessingMultipleGraphsError(legacyMessage: String, position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NA5)
        .atPosition(position.offset, position.line, position.column)
        .build())
      .build()
    SemanticError(
      gql,
      legacyMessage,
      position
    )
  }

  def numberTooLarge(numberType: String, value: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql22003(value, position.offset, position.line, position.column)
    SemanticError(gql, s"$numberType is too large", position)
  }

  def notSupported(pos: InputPosition): SemanticError = {
    val msg = "Not supported."
    val gql = GqlHelper.get50N00(SemanticError.getClass.getSimpleName, msg, pos.offset, pos.line, pos.column)
    SemanticError(gql, msg, pos)
  }

  def useClauseInLocalProcedureDefinitionNotSupported(pos: InputPosition): SemanticError = {
    val msg = "USE clause is not supported in local procedure definitions"
    val gql = GqlHelper.getGql42001_42NAF()
    SemanticError(gql, msg, pos)
  }

  def useClauseInLocalFunctionDefinitionNotSupported(pos: InputPosition): SemanticError = {
    val msg = "USE clause is not supported in local function definitions"
    val gql = GqlHelper.getGql42001_42NAG()
    SemanticError(gql, msg, pos)
  }

  def returnColumnDoesNotMatchOutputSignatureOfLocalProcedure(
    column: String,
    procedureName: ProcedureName,
    pos: InputPosition
  ): SemanticError = {
    val msg = s"Return column `$column` does not match output signature of local procedure ${procedureName.fullName}"
    val gql = GqlHelper.getGql42001_42NAH(column, procedureName.fullName, pos.offset, pos.line, pos.column)
    SemanticError(gql, msg, pos)
  }

  def missingReturnColumnInLocalProcedure(
    column: String,
    procedureName: ProcedureName,
    pos: InputPosition
  ): SemanticError = {
    val msg =
      s"Return column `$column` is missing to match output signature of local procedure ${procedureName.fullName}"
    val gql = GqlHelper.getGql42001_42NAI(column, procedureName.fullName, pos.offset, pos.line, pos.column)
    SemanticError(gql, msg, pos)
  }

  def notSupportedLocalProcedureOutputType(
    outputFieldSignature: LocalFieldSignature,
    procedureName: ProcedureName
  ): SemanticError = {
    val typeName = outputFieldSignature.getType.normalizedCypherTypeString()
    val column = outputFieldSignature.name
    val pos = outputFieldSignature.position
    val msg =
      s"`$typeName` is not supported as local procedure output type. Adjust the type of output field `$column` of local procedure ${procedureName.fullName}"
    val gql = GqlHelper.getGql42001_42NAJ(typeName, column, procedureName.fullName, pos.offset, pos.line, pos.column)
    SemanticError(gql, msg, pos)
  }

  def notSupportedLocalFunctionReturnType(
    typ: CypherType,
    functionName: FunctionName
  ): SemanticError = {
    val typeName = typ.normalizedCypherTypeString()
    val pos = functionName.namespace.position
    val msg =
      s"`$typeName` is not supported as local function return type. Adjust the return type of local function ${functionName.fullName}"
    val gql = GqlHelper.getGql42001_42NAK(typeName, functionName.fullName, pos.offset, pos.line, pos.column)
    SemanticError(gql, msg, pos)
  }

  def notSupportedLocalCallableParameterType(
    inputFieldSignature: LocalFieldSignature,
    callableName: CallableName
  ): SemanticError = {
    val typeName = inputFieldSignature.getType.normalizedCypherTypeString()
    val column = inputFieldSignature.name
    val pos = inputFieldSignature.position
    val msg =
      s"`$typeName` is not supported as local callable parameter type. Adjust the type of parameter `$column` of local callable ${callableName.fullName}"
    val gql = GqlHelper.getGql42001_42NAL(typeName, column, callableName.fullName, pos.offset, pos.line, pos.column)
    SemanticError(gql, msg, pos)
  }

  def notSupportedQueryResultInLocalFunction(functionName: FunctionName, position: InputPosition): SemanticError = {
    val msg =
      s"Non-scalar query result not supported in local function definitions. " +
        s"Query in local function definitions ${functionName.fullName} requires a `RETURN` clause with a single column and computing a total aggregate or containing `LIMIT 1`."
    val gql = GqlHelper.getGql42001_42NAN(functionName.fullName, position.offset, position.line, position.column)
    SemanticError(gql, msg, position)
  }

  def invalidUseOfDefineAndCIT(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42NAO(position.offset, position.line, position.column)
    SemanticError(gql, "'CALL { ... } IN TRANSACTIONS' is not supported in combination with 'DEFINE'", position)
  }

  def invalidClauseCombination(clause1: String, clause2: String, position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N14)
        .atPosition(position.offset, position.line, position.column)
        .withParam(GqlParams.StringParam.clause, clause1)
        .withParam(GqlParams.StringParam.cmd, clause2)
        .build())
      .build()
    SemanticError(
      gql,
      "%s cannot be used together with %s.".formatted(clause1, clause2),
      position
    )
  }

  def bothOrReplaceAndIfNotExists(entity: String, userAsString: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N14(
      "OR REPLACE",
      "IF NOT EXISTS",
      position.offset,
      position.line,
      position.column
    )
    SemanticError(
      gql,
      s"Failed to create the specified $entity '$userAsString': cannot have both `OR REPLACE` and `IF NOT EXISTS`.",
      position
    )
  }

  def badCommandWithOrReplace(cmd: String, cypherCmd: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N14("OR REPLACE", cypherCmd, position.offset, position.line, position.column)
    SemanticError(gql, s"Failed to $cmd: `OR REPLACE` cannot be used together with this command.", position)
  }

  def authRuleMustHaveACondition(position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N06)
        .atPosition(position.offset, position.line, position.column)
        .withParam(GqlParams.ListParam.inputList, List("SET CONDITION").asJava)
        .build())
      .build();
    SemanticError(gql, GqlHelper.getCompleteMessage(gql), position)
  }

  def authRuleConditionCannotSubqueryExpression(
    position: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N04)
          .atPosition(position.offset, position.line, position.column)
          .withParam(GqlParams.StringParam.input, "subquery expression")
          .withParam(GqlParams.StringParam.context, "auth rule condition")
          .withParam(GqlParams.ListParam.inputList, java.util.List.of("boolean expression"))
          .build()
      ).build()

    SemanticError(gql, GqlHelper.getCompleteMessage(gql), position)
  }

  def authRuleConditionHaveInvalidFunctionInCondition(
    functionName: String,
    position: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
          .atPosition(position.offset, position.line, position.column)
          .withParam(GqlParams.StringParam.input, functionName)
          .withParam(GqlParams.StringParam.context, "function in auth rule condition")
          .build()
      ).build()

    SemanticError(gql, GqlHelper.getCompleteMessage(gql), position)
  }

  def authRuleConditionTemporalFunctionRetrievesCurrentTime(
    functionCall: String,
    suggestion: String,
    position: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NAM)
          .atPosition(position.offset, position.line, position.column)
          .withParam(GqlParams.StringParam.input, functionCall)
          .withParam(GqlParams.StringParam.input1, suggestion)
          .build()
      ).build()

    SemanticError(gql, GqlHelper.getCompleteMessage(gql), position)
  }

  def authRuleConditionCannotContainParameter(parameter: Parameter): SemanticError = {
    val position = parameter.position
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
          .withParam(GqlParams.StringParam.input, s"$$${parameter.name}")
          .withParam(GqlParams.StringParam.context, "auth rule condition")
          .atPosition(position.offset, position.line, position.column)
          .build()
      )
      .build()

    SemanticError(gql, GqlHelper.getCompleteMessage(gql), position)
  }

  def denyMergeUnsupported(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N14("DENY", "MERGE", position.offset, position.line, position.column)
    SemanticError(gql, "`DENY MERGE` is not supported. Use `DENY SET PROPERTY` and `DENY CREATE` instead.", position)
  }

  def grantDenyRevokeUnsupported(cmd: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N14(
      "GRANT, DENY and REVOKE",
      cmd,
      position.offset,
      position.line,
      position.column
    )
    SemanticError(gql, s"`GRANT`, `DENY` and `REVOKE` are not supported for `$cmd`", position)
  }

  def defaultLanguageForConstituentAliases(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N14(
      "DEFAULT LANGUAGE",
      "constituent aliases",
      position.offset,
      position.line,
      position.column
    )
    SemanticError(gql, GqlHelper.getCompleteMessage(gql), position)
  }

  def unableToRouteUseClauseError(legacyMessage: String, position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N04)
      .atPosition(position.offset, position.line, position.column)
      .withParam(GqlParams.StringParam.clause, "`USE` clause")
      .build()
    SemanticError(
      gql,
      legacyMessage,
      position
    )
  }

  def invalidNumberOfProcedureOrFunctionArguments(
    expectedNumberOfArgs: Int,
    obtainedNumberOfArgs: Int,
    procedureFunction: String,
    signature: String,
    legacyMessage: String,
    position: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I13)
        .atPosition(position.offset, position.line, position.column)
        .withParam(GqlParams.NumberParam.count1, expectedNumberOfArgs)
        .withParam(GqlParams.NumberParam.count2, obtainedNumberOfArgs)
        .withParam(GqlParams.StringParam.procFun, procedureFunction)
        .withParam(GqlParams.StringParam.sig, signature)
        .build())
      .build()

    SemanticError(
      gql,
      legacyMessage,
      position
    )
  }

  def invalidYieldStar(
    commandName: String,
    position: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N94)
      .build()
    SemanticError(
      gql,
      s"When combining `${commandName}` with other show and/or terminate commands, `YIELD *` isn't permitted.",
      position
    )
  }

  def missingYield(
    commandName: String,
    position: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N93)
      .build()
    SemanticError(
      gql,
      s"When combining `${commandName}` with other show and/or terminate commands, `YIELD` is mandatory.",
      position
    )
  }

  def missingReturn(position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N92)
      .build()
    SemanticError(
      gql,
      "When combining show and/or terminate commands, `RETURN` isn't optional.",
      position
    )
  }

  def queryMustConcludeWithClause(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N71(position.offset, position.line, position.column)
    SemanticError(gql, s"Query must conclude with $validLastClauses.", position)
  }

  def queryCannotConcludeWithCall(callName: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N71(position.offset, position.line, position.column)
    SemanticError(gql, s"Query cannot conclude with $callName together with YIELD", position)
  }

  def queryCannotConcludeWithClause(clause: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N71(position.offset, position.line, position.column)
    SemanticError(
      gql,
      s"Query cannot conclude with $clause (must be $validLastClauses).",
      position
    )
  }

  def invalidPropertyBasedAccessControlRuleInvolvingNontrivialPredicates(
    unsupportedExpression: String,
    legacyMessage: String,
    position: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA0)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA7)
        .atPosition(position.offset, position.line, position.column)
        .withParam(GqlParams.StringParam.expr, unsupportedExpression)
        .build())
      .build()
    SemanticError(
      gql,
      legacyMessage,
      position
    )
  }

  private val validLastClauses =
    "a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD"

  def withIsRequiredBetween(clause1: String, clause2: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N24(clause1, clause2, position.offset, position.line, position.column)
    SemanticError(gql, s"WITH is required between $clause1 and $clause2", position)
  }

  def invalidType(
    value: String,
    correctTypes: List[String],
    actualType: String,
    legacyMessage: String,
    pos: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G03)
      .atPosition(pos.offset, pos.line, pos.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N01)
        .atPosition(pos.offset, pos.line, pos.column)
        .withParam(GqlParams.StringParam.value, value)
        .withParam(GqlParams.ListParam.valueTypeList, correctTypes.asJava)
        .withParam(GqlParams.StringParam.valueType, actualType)
        .build())
      .build()

    new SemanticError(
      gql,
      legacyMessage,
      pos
    )
  }

  def invalidPlacementOfUseClause(pos: InputPosition): SemanticError = {
    new SemanticError(
      GqlHelper.getGql42001_42N73(pos.offset, pos.line, pos.column),
      "USE clause must be the first clause in a (sub-)query.",
      pos
    )
  }

  def invalidPlacementOfUseClauseVerboseLegacyMsg(pos: InputPosition): SemanticError = {
    new SemanticError(
      GqlHelper.getGql42001_42N73(pos.offset, pos.line, pos.column),
      "USE clause must be either the first clause in a (sub-)query or preceded by an importing WITH clause in a sub-query.",
      pos
    )
  }

  def invalidSubqueryInMerge(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42I48(position.offset, position.line, position.column)
    SemanticError(gql, "Subquery expressions are not allowed in a MERGE clause.", position)
  }

  def invalidUseOfMultiplePathPatterns(position: InputPosition, explicitMatchModesSupported: Boolean): SemanticError = {
    val baseMessage =
      "Multiple path patterns cannot be used in the same clause in combination with a selective path selector."

    val action = if (explicitMatchModesSupported) {
      " You may want to use multiple MATCH clauses, or you might want to consider using the REPEATABLE ELEMENTS match mode."
    } else {
      ""
    }

    val gql = GqlHelper.getGql42001_42I45(action, position.offset, position.line, position.column)
    SemanticError(gql, baseMessage + action, position)
  }

  def invalidFieldTerminator(position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I05)
        .atPosition(position.offset, position.line, position.column)
        .build())
      .build()
    SemanticError(gql, "CSV field terminator can only be one character wide", position)
  }

  def singleRelationshipPatternRequired(name: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N40(name, position.offset, position.line, position.column)
    SemanticError(gql, s"$name(...) requires a pattern containing a single relationship", position)
  }

  def inputContainsInvalidCharacters(
    invalidInput: String,
    context: String,
    legacyMessage: String,
    position: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
      .atPosition(position.offset, position.line, position.column)
      .withParam(GqlParams.StringParam.input, invalidInput)
      .withParam(GqlParams.StringParam.context, context)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N82)
        .atPosition(position.offset, position.line, position.column)
        .withParam(GqlParams.StringParam.input, invalidInput)
        .withParam(GqlParams.StringParam.context, context)
        .build())
      .build()
    SemanticError(gql, legacyMessage, position)
  }

  def numPrimariesOutOfRange(
    count: Int,
    command: String,
    action: String,
    topologyString: String,
    position: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22003)
      .withParam(GqlParams.StringParam.value, String.valueOf(count))
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N52)
        .atPosition(position.offset, position.line, position.column)
        .withParam(GqlParams.StringParam.action, action)
        .withParam(GqlParams.NumberParam.count, count)
        .withParam(GqlParams.NumberParam.upper, 11)
        .build())
      .build()

    SemanticError(
      gql,
      s"Failed to $command with `$topologyString`, PRIMARY must be greater than 0.",
      position
    )
  }

  def numSecondariesOutOfRange(
    count: Int,
    command: String,
    action: String,
    topologyString: String,
    position: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22003)
      .withParam(GqlParams.StringParam.value, String.valueOf(count))
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N53)
        .atPosition(position.offset, position.line, position.column)
        .withParam(GqlParams.StringParam.action, action)
        .withParam(GqlParams.NumberParam.count, count)
        .withParam(GqlParams.NumberParam.upper, 20)
        .build())
      .build()

    SemanticError(
      gql,
      s"Failed to $command with `$topologyString`, SECONDARY must be a positive value",
      position
    )
  }

  def numReplicasOutOfRange(
    count: Int,
    command: String,
    topologyString: String,
    position: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22003)
      .withParam(GqlParams.StringParam.value, String.valueOf(count))
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N58)
        .atPosition(position.offset, position.line, position.column)
        .withParam(GqlParams.NumberParam.count, count)
        .withParam(GqlParams.NumberParam.upper, 20)
        .withParam(GqlParams.StringParam.context, "replicas")
        .build())
      .build()

    SemanticError(
      gql,
      s"Failed to $command with `$topologyString`, REPLICA must be between 1 and 20.",
      position
    )
  }

  def numShardsOutOfRange(
    count: Int,
    command: String,
    topologyString: String,
    position: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22003)
      .withParam(GqlParams.StringParam.value, String.valueOf(count))
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N58)
        .atPosition(position.offset, position.line, position.column)
        .withParam(GqlParams.NumberParam.count, count)
        .withParam(GqlParams.NumberParam.upper, 1000)
        .withParam(GqlParams.StringParam.context, "shards")
        .build())
      .build()

    SemanticError(
      gql,
      s"Failed to $command with `$topologyString`, COUNT must be between 1 and 1000.",
      position
    )
  }

  def unsupportedUseOfProperties(props: Expression, funcName: String, position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N56)
        .atPosition(position.offset, position.line, position.column)
        .withParam(GqlParams.StringParam.fun, funcName)
        .build())
      .build()
    SemanticError(
      gql,
      s"$funcName(...) contains properties $props. This is currently not supported.",
      position
    )
  }

  def nodeVariableNotBound(funcName: String, position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N65)
        .atPosition(position.offset, position.line, position.column)
        .withParam(GqlParams.StringParam.fun, funcName)
        .build())
      .build()
    SemanticError(gql, s"A $funcName(...) requires bound nodes when not part of a MATCH clause.", position)
  }

  def relationshipVariableAlreadyBound(funcName: String, position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N66)
        .atPosition(position.offset, position.line, position.column)
        .withParam(GqlParams.StringParam.fun, funcName)
        .build())
      .build()
    SemanticError(gql, s"Bound relationships not allowed in $funcName(...)", position)
  }

  def qppInShortestPath(funcName: String, position: InputPosition): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42I23(funcName, position.offset, position.line, position.column),
      s"$funcName(...) contains quantified pattern. This is currently not supported.",
      position
    )
  }

  def invalidLowerBound(funcName: String, position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I08)
        .atPosition(position.offset, position.line, position.column)
        .withParam(GqlParams.StringParam.fun, funcName)
        .build())
      .build()

    SemanticError(
      gql,
      s"$funcName(...) does not support a minimal length different from 0 or 1",
      position
    )
  }

  def invalidUseOfParameterMap(keyword: String, param: String, position: InputPosition): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42N32(keyword, position.offset, position.line, position.column),
      s"Parameter maps cannot be used in `$keyword` patterns (use a literal map instead, e.g. `{id: $$$param.id}`)",
      position
    )
  }

  def invalidUseOfPatternExpression(position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I34)
        .atPosition(position.offset, position.line, position.column)
        .build())
      .build()

    SemanticError(gql, invalidUseOfPatternExpressionMessage, position)
  }

  val invalidUseOfPatternExpressionMessage: String =
    "A pattern expression should only be used in order to test the existence of a pattern. " +
      "It should therefore only be used in contexts that evaluate to a boolean, e.g. inside the function exists() or in a WHERE-clause. " +
      "No other uses are allowed, instead they should be replaced by a pattern comprehension."

  def invalidUseOfUnionAndCIT(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N47(position.offset, position.line, position.column)
    SemanticError(gql, "CALL { ... } IN TRANSACTIONS in a UNION is not supported", position)
  }

  def unknownFunction(functionName: String, position: InputPosition): SemanticError = {
    unknownFunction(functionName, s"Unknown function '$functionName'", position)
  }

  def unknownFunctionWithVersionHint(
    functionName: String,
    otherVersion: CypherVersion,
    position: InputPosition
  ): SemanticError = {
    val versionNumber = otherVersion.versionName.toInt
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N48)
          .atPosition(position.offset, position.line, position.column)
          .withParam(GqlParams.StringParam.fun, functionName)
          .withCause(
            ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I78)
              .atPosition(position.offset, position.line, position.column)
              .withParam(GqlParams.NumberParam.version1, versionNumber)
              .build()
          )
          .build()
      )
      .build()
    SemanticError(gql, s"Unknown function '$functionName'", position)
  }

  def unknownFunctionNamedNot(position: InputPosition): SemanticError = {
    unknownFunction(
      "not",
      "Unknown function 'not'. If you intended to use the negation expression, surround it with parentheses.",
      position
    )
  }

  private def unknownFunction(functionName: String, legacyMessage: String, position: InputPosition): SemanticError = {
    val gql =
      GqlHelper.getGql42001_42N48(functionName, position.offset, position.line, position.column)
    SemanticError(gql, legacyMessage, position)
  }

  def invalidToken(tokenType: String, input: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42I11(tokenType, input, position.offset, position.line, position.column)
    val inputChecked =
      if (input == null) "Null"
      else if (input.isEmpty) "''"
      else input
    SemanticError(
      gql,
      s"$inputChecked is not a valid token name. Token names cannot be empty or contain any null-bytes.",
      position
    )
  }

  def invalidDelete(position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I26)
        .atPosition(position.offset, position.line, position.column)
        .build())
      .build()

    SemanticError(gql, "DELETE doesn't support removing labels from a node. Try REMOVE.", position)
  }

  def unsafeUsageOfRepeatableElements(position: InputPosition): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42N53(position.offset, position.line, position.column),
      "The quantified path pattern may yield an infinite number of rows under match mode 'REPEATABLE ELEMENTS'. " +
        "Add an upper bound to the quantified path pattern.",
      position
    )
  }

  def variableAlreadyDeclared(variableName: String, position: InputPosition): SemanticError = {
    variableAlreadyDeclared(variableName, s"Variable `${variableName}` already declared", position)
  }

  def variableAlreadyDeclared(variableName: String, legacyMessage: String, position: InputPosition): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42N59(variableName, position.offset, position.line, position.column),
      legacyMessage,
      position
    )
  }

  def invalidUseOfVariableLengthRelationship(
    expr: String,
    legacyMessage: String,
    position: InputPosition
  ): SemanticError = {
    SemanticError(
      ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
        .atPosition(position.offset, position.line, position.column)
        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I41)
          .atPosition(position.offset, position.line, position.column)
          .withParam(GqlParams.StringParam.value, expr)
          .build())
        .build(),
      legacyMessage,
      position
    )
  }

  def variableNotDefined(variableName: String, legacyMessage: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N62(variableName, position.offset, position.line, position.column)
    SemanticError(gql, legacyMessage, position)
  }

  def variableNotDefined(variableName: String, position: InputPosition): SemanticError = {
    variableNotDefined(variableName, s"Variable `$variableName` not defined", position)
  }

  def wrongInequalityOperator(position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I49)
        .atPosition(position.offset, position.line, position.column)
        .build())
      .build()
    SemanticError(
      gql,
      "Unknown operation '!=' (you probably meant to use '<>', which is the operator for inequality testing)",
      position
    )
  }

  def multipleReturnColumnsWithSameName(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N38(position.offset, position.line, position.column)
    SemanticError(gql, "Multiple result columns with the same name are not supported", position)
  }

  def multipleJoinHintsForSameVariable(variable: String, position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N26)
        .atPosition(position.offset, position.line, position.column)
        .withParam(GqlParams.StringParam.variable, variable)
        .build())
      .build()
    SemanticError(gql, "Multiple join hints for same variable are not supported", position)
  }

  def innerTypeWithDifferentNullability(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N63(position.offset, position.line, position.column)
    SemanticError(gql, "All types in a Closed Dynamic Union must be nullable, or be appended with `NOT NULL`", position)
  }

  def expressionCanOnlyBeUsedInMatch(
    startOfLegacyMessage: String,
    expr: String,
    clause: String,
    position: InputPosition
  ): SemanticError = {
    val gql = GqlHelper.getGql42001_42I04(expr, clause, position.offset, position.line, position.column)
    SemanticError(
      gql,
      s"$startOfLegacyMessage cannot be used in a $clause clause, but only in a MATCH clause.",
      position
    )
  }

  def patternPredicateInVarLengthRel(position: InputPosition): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42N37(position.offset, position.line, position.column),
      "Relationship pattern predicates are not supported for variable-length relationships.",
      position
    )
  }

  def procedureCallWithoutParentheses(name: String, position: InputPosition): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42N36(position.offset, position.line, position.column),
      "Procedure call is missing parentheses: " + name,
      position
    )
  }

  def procedureCallWithParenthesesWithArgs(name: String, position: InputPosition): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42N36(position.offset, position.line, position.column),
      "Procedure call inside a query does not support passing arguments implicitly. " +
        "Please pass arguments explicitly in parentheses after procedure name for " + name,
      position
    )
  }

  def procedureCallWithImplicitNaming(
    availableColumns: List[String],
    position: InputPosition
  ): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42N25(availableColumns.asJava, position.offset, position.line, position.column),
      s"Procedure call inside a query does not support naming results implicitly (name explicitly using `YIELD` instead)",
      position
    )
  }

  def nestedQPP(position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I12)
        .atPosition(position.offset, position.line, position.column)
        .build())
      .build()

    SemanticError(
      gql,
      "Quantified path patterns are not allowed to be nested.",
      position
    )
  }

  def shortestPathInsideQPP(position: InputPosition): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42N69(
        "shortestPath",
        "quantified path pattern",
        position.offset,
        position.line,
        position.column
      ),
      "shortestPath(...) is only allowed as a top-level element and not inside a quantified path pattern",
      position
    )
  }

  def shortestPathInsideParenthesizedPathPattern(shortestPathFunc: String, position: InputPosition): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42N69(
        shortestPathFunc,
        "parenthesized path pattern",
        position.offset,
        position.line,
        position.column
      ),
      s"$shortestPathFunc(...) is only allowed as a top-level element and not inside a parenthesized path pattern",
      position
    )
  }

  def pathPatternNeedsAtLeastOnePattern(patternPart: String, position: InputPosition): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42N64(position.offset, position.line, position.column),
      s"""A top-level path pattern in a `MATCH` clause must be written such that it always evaluates to at least one node pattern.
         |In this case, `$patternPart` would result in an empty pattern.""".stripMargin,
      position
    )
  }

  def qppNeedsAtLeastOneRelationship(
    pattern: String,
    nodeCountDescription: String,
    position: InputPosition
  ): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42N64(position.offset, position.line, position.column),
      s"""A quantified path pattern needs to have at least one relationship.
         |In this case, the quantified path pattern $pattern consists of only $nodeCountDescription.""".stripMargin,
      position
    )
  }

  def invalidNodePatternPair(inThisCase: String, position: InputPosition): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42I46(position.offset, position.line, position.column),
      s"""Juxtaposition is currently only supported for quantified path patterns.
         |$inThisCase
         |That is, neither of these is a quantified path pattern.""".stripMargin,
      position
    )
  }

  def invalidQuantifier(lower: Long, upper: Long, position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I17)
        .atPosition(position.offset, position.line, position.column)
        .build()).build()
    SemanticError(
      gql,
      s"""A quantifier for a path pattern must not have a lower bound which exceeds its upper bound.
         |In this case, the lower bound $lower is greater than the upper bound $upper.""".stripMargin,
      position
    )
  }

  def cannotYieldFromVoidProcedure(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42I42(position.offset, position.line, position.column)
    SemanticError(gql, "Cannot yield value from void procedure.", position)
  }

  def pathBoundInQPP(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N34(position.offset, position.line, position.column)
    SemanticError(gql, "Assigning a path in a quantified path pattern is not yet supported.", position)
  }

  def notStaticallyInferrableVariable(name: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N28(name, position.offset, position.line, position.column)
    SemanticError(
      gql,
      s"It is not allowed to refer to variables in $name, so that the value for $name can be statically calculated.",
      position
    )
  }

  def notStaticallyInferrablePattern(name: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N28(name, position.offset, position.line, position.column)
    SemanticError(
      gql,
      s"It is not allowed to use patterns in the expression for $name, so that the value for $name can be statically calculated.",
      position
    )
  }

  def invalidUseOfShortestPath(fun: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42I39(fun, position.offset, position.line, position.column)
    SemanticError(
      gql,
      "Mixing shortestPath/allShortestPaths with path selectors (e.g. `ANY SHORTEST`), " +
        "explicit match modes (e.g. `DIFFERENT RELATIONSHIPS`) or explicit path modes (e.g. `ACYCLIC`) is not allowed.",
      position
    )
  }

  def invalidReportStatus(position: InputPosition): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42I36(position.offset, position.line, position.column),
      "REPORT STATUS can only be used when specifying ON ERROR CONTINUE or ON ERROR BREAK",
      position
    )
  }

  def disjointByRequiresConcurrent(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N7A(position.offset, position.line, position.column)
    SemanticError(gql, "DISJOINT BY can only be used in CALL { ... } IN CONCURRENT TRANSACTIONS", position)
  }

  def disjointByExpressionNotDeterministic(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N7B(position.offset, position.line, position.column)
    SemanticError(gql, "DISJOINT BY expressions must be deterministic", position)
  }

  def disjointByExpressionContainsSubquery(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N7C(position.offset, position.line, position.column)
    SemanticError(gql, "DISJOINT BY expressions must not contain subquery expressions", position)
  }

  def matchModesNotSupportedInCypher5(matchMode: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N54(matchMode, position.offset, position.line, position.column)
    SemanticError(
      gql,
      s"Match modes such as `$matchMode` are not supported in Cypher 5.",
      position
    )
  }

  def unsupportedMatchModePathModeCombination(pathMode: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N60(pathMode, position.offset, position.line, position.column)
    SemanticError(
      gql,
      s"REPEATABLE ELEMENTS with $pathMode path mode is not supported.",
      position
    )
  }

  def unsupportedPathModeMixing(pathModes: Set[String], position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N61(pathModes.toList.asJava, position.offset, position.line, position.column)
    SemanticError(
      gql,
      s"Mixing path modes ${pathModes.mkString(", ")} in the same graph pattern is not supported.",
      position
    )
  }

  def uuidTypeNotSupported(item: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_51N26(item, "UUID types", position.offset, position.line, position.column)
    SemanticError(
      gql,
      "The UUID type is not supported.",
      position
    )
  }

  def groupByNotSupported(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_51N26(
      "The `GROUP BY` clause",
      "the `GROUP BY` clause",
      position.offset,
      position.line,
      position.column
    )
    SemanticError(
      gql,
      "The GROUP BY clause is not supported.",
      position
    )
  }

  def unsupportedPathModeWithVarLength(
    varLengthRel: String,
    pathMode: String,
    position: InputPosition
  ): SemanticError = {
    val gql = GqlHelper.getGql42001_51N26(
      s"Using explicit path modes on a pattern containing a variable-length relationship",
      s"`$pathMode` on variable-length relationships",
      position.offset,
      position.line,
      position.column
    )
    SemanticError(
      gql,
      s"Using a variable-length relationship such as `$varLengthRel` together with explicit path mode `$pathMode` is not available.",
      position
    )
  }

  def invalidImportingWithKeyword(keyword: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42I28(keyword, position.offset, position.line, position.column)
    SemanticError(
      gql,
      s"Importing WITH should consist only of simple references to outside variables. $keyword is not allowed.",
      position
    )
  }

  def invalidImportingWithAliasOrExpression(input: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42I28(input, position.offset, position.line, position.column)
    SemanticError(
      gql,
      "Importing WITH should consist only of simple references to outside variables. Aliasing or expressions are not supported.",
      position
    )
  }

  def invalidYieldStar(position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I43)
          .atPosition(position.offset, position.line, position.column)
          .build()
      )
      .build()

    SemanticError(gql, "Cannot use `YIELD *` outside standalone call", position)
  }

  def unsupportedNestingCIT(position: InputPosition): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42N58(position.offset, position.line, position.column),
      "Nested CALL { ... } IN TRANSACTIONS is not supported",
      position
    )
  }

  def unsupportedNestingCITInCall(position: InputPosition): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42N58(position.offset, position.line, position.column),
      "CALL { ... } IN TRANSACTIONS nested in a regular CALL is not supported",
      position
    )
  }

  def invalidReduceAccumulator(position: InputPosition): SemanticError = {
    val gql = missingPipeExpression(position)
    SemanticError(gql, "reduce(...) requires '| expression' (an accumulation expression)", position)
  }

  private def missingPipeExpression(position: InputPosition) = {
    ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N41)
          .atPosition(position.offset, position.line, position.column)
          .build()
      )
      .build()
  }

  def invalidAllReduceSyntax(position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation
        .from(GqlStatusInfoCodes.STATUS_42I51)
        .withParam(GqlParams.StringParam.procFun, AllReduce.name)
        .withParam(GqlParams.StringParam.sig, AllReduce.signatures.head.getSignatureAsString)
        .atPosition(position.offset, position.line, position.column)
        .build)
      .build
    SemanticError(
      gql,
      s"Invalid syntax for the `allReduce` function. The function allReduce must have the signature ${AllReduce.signatures.head.getSignatureAsString}",
      position
    )
  }

  def invalidDistinct(fun: String, position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I27)
          .atPosition(position.offset, position.line, position.column)
          .withParam(GqlParams.StringParam.fun, fun)
          .build()
      )
      .build()
    SemanticError(gql, s"Invalid use of DISTINCT with function '$fun'", position)
  }

  def invalidPoint(keys: Seq[String], position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I16)
          .atPosition(position.offset, position.line, position.column)
          .withParam(GqlParams.ListParam.mapKeyList, keys.asJava)
          .build()
      )
      .build()

    val legacyMessage =
      s"A map with keys ${keys.map(key => s"'$key'").mkString(", ")} is not describing a valid point, " +
        s"a point is described either by using cartesian coordinates e.g. {x: 2.3, y: 4.5, crs: 'cartesian'} or using " +
        s"geographic coordinates e.g. {latitude: 12.78, longitude: 56.7, crs: 'WGS-84'}."

    SemanticError(gql, legacyMessage, position)
  }

  def invalidRelTypeExpression(clause: String, position: InputPosition): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42I35(position.offset, position.line, position.column),
      s"Relationship type expressions in patterns are not allowed in $clause, but only in a MATCH clause",
      position
    )
  }

  def invalidPatternPredicate(entity: String, description: String, position: InputPosition): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42I32(description, position.offset, position.line, position.column),
      s"$entity pattern predicates are not allowed in $description, but only in a MATCH clause or inside a pattern comprehension",
      position
    )
  }

  def mixingColonAndIs(
    labelExpressions: Set[String],
    replacements: Set[String],
    position: InputPosition
  ): SemanticError = {
    val gql = GqlHelper.getGql42001_42I29(
      labelExpressions.mkString(", "),
      replacements.mkString(", "),
      position.offset,
      position.line,
      position.column
    )
    val exprText = if (replacements.size > 1) "These expressions" else "This expression"
    SemanticError(
      gql,
      s"Mixing the IS keyword with colon (':') between labels is not allowed. $exprText could be expressed as ${replacements.mkString(", ")}.",
      position
    )
  }

  def mixingIsWithMultipleLabels(statement: String, replacement: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42I29(
      statement,
      replacement,
      position.offset,
      position.line,
      position.column
    )
    SemanticError(
      gql,
      s"It is not supported to use the `IS` keyword together with multiple labels in `$statement`. Rewrite the expression as `$replacement`.",
      position
    )
  }

  def invalidLabelExpression(replacements: Set[String], position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42I10(replacements.mkString(", "), position.offset, position.line, position.column)
    val exprText = if (replacements.size > 1) "These expressions" else "This expression"
    SemanticError(
      gql,
      s"Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. $exprText could be expressed as ${replacements.mkString(", ")}.",
      position
    )
  }

  def invalidLabelExpressionInShortestPath(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42I30("shortestPath", position.offset, position.line, position.column)
    SemanticError(
      gql,
      "Label expressions in shortestPath are not allowed in an expression",
      position
    )
  }

  def invalidLabelExpressionInPattern(clause: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42I30(clause, position.offset, position.line, position.column)
    val legacyMsg =
      s"Label expressions in patterns are not allowed in $clause, but only in a MATCH clause and in expressions"
    SemanticError(gql, legacyMsg, position)
  }

  def functionCallWrongNumberOfArguments(
    expectedNumArgs: Int,
    actualNumArgs: Int,
    name: String,
    signature: String,
    argumentMsg: String,
    position: InputPosition,
    legacyMsg: Option[String] = None
  ): SemanticError = {
    val gql = GqlHelper.getGql42001_42I13(
      expectedNumArgs,
      actualNumArgs,
      name,
      signature,
      position.offset,
      position.line,
      position.column
    )
    val msg = legacyMsg match {
      case Some(m) => m
      case None =>
        s"""Function call does not provide the required number of arguments: expected $expectedNumArgs got $actualNumArgs.
           |
           |Function $name has signature: $signature
           |meaning that it expects $expectedNumArgs $argumentMsg""".stripMargin
    }
    SemanticError(
      gql,
      msg,
      position
    )
  }

  def procedureCallTooFewArguments(
    actualNumArgs: Int,
    minNumArgs: Int,
    totalNumArgs: Int,
    numArgsWithDefaults: Int,
    name: String,
    signature: String,
    sigDesc: String,
    description: String,
    position: InputPosition
  ): SemanticError = {
    val gql = GqlHelper.getGql42001_42I13(
      minNumArgs,
      actualNumArgs,
      name,
      signature,
      position.offset,
      position.line,
      position.column
    )
    SemanticError(
      gql,
      s"""Procedure call does not provide the required number of arguments: got $actualNumArgs expected at least $minNumArgs (total: $totalNumArgs, $numArgsWithDefaults of which have default values).
         |
         |$sigDesc
         |$description""".stripMargin,
      position
    )
  }

  def procedureCallTooManyArguments(
    expectedNumArgs: Int,
    actualNumArgs: Int,
    name: String,
    signature: String,
    maxExpectedMsg: String,
    sigDesc: String,
    description: String,
    position: InputPosition
  ): SemanticError = {
    val gql = GqlHelper.getGql42001_42I13(
      expectedNumArgs,
      actualNumArgs,
      name,
      signature,
      position.offset,
      position.line,
      position.column
    )
    SemanticError(
      gql,
      s"""Procedure call provides too many arguments: got $actualNumArgs expected $maxExpectedMsg.
         |
         |$sigDesc
         |$description""".stripMargin,
      position
    )
  }

  def invalidReferenceInParenthesizedPathPatternPredicate(
    stringifiedPattern: String,
    invalidReferences: Set[String],
    position: InputPosition,
    legacyErrorMessage: String
  ): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42I21(
        invalidReferences.toList.asJava,
        stringifiedPattern,
        position.offset,
        position.line,
        position.column
      ),
      legacyErrorMessage,
      position
    )
  }

  def invalidLiteralNumber(value: String, input: String, position: InputPosition): SemanticError = {
    invalidIntegerSyntax(value, input, position, "invalid literal number")
  }

  def invalidOctalIntegerSyntax(input: String, position: InputPosition): SemanticError = {
    val newInput = input.patch(input.indexOf('0') + 1, "o", 0)
    invalidIntegerSyntax(
      "octal integer",
      input,
      position,
      s"The octal integer literal syntax `$input` is no longer supported, please use `$newInput` instead"
    )
  }

  def invalidHexIntegerSyntax(input: String, position: InputPosition): SemanticError =
    invalidIntegerSyntax(
      "hex integer",
      input,
      position,
      s"The hex integer literal syntax `$input` is no longer supported, please use `${input.replace('X', 'x')}` instead"
    )

  private def invalidIntegerSyntax(
    valueType: String,
    input: String,
    position: InputPosition,
    legacyErrorMessage: String
  ): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42I07(
        valueType,
        input,
        position.offset,
        position.line,
        position.column
      ),
      legacyErrorMessage,
      position
    )
  }

  def invalidNumberOfRelationshipTypes(
    variable: String,
    position: InputPosition,
    legacyErrorMessage: String
  ): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42I14(variable, position.offset, position.line, position.column),
      legacyErrorMessage,
      position
    )
  }

  def inaccessibleVariable(
    variable: String,
    clause: String,
    position: InputPosition,
    groupBySupported: Boolean = false
  ): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42N44(variable, clause, groupBySupported, position.offset, position.line, position.column),
      s"In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: $variable",
      position
    )
  }

  def patternExpressionInSize(position: InputPosition): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42I52(errorMessageForSizeFunction, position.offset, position.line, position.column),
      errorMessageForSizeFunction,
      position
    )
  }

  val errorMessageForSizeFunction: String =
    "A pattern expression should only be used in order to test the existence of a pattern. " +
      "It can no longer be used inside the function size(), an alternative is to replace size() with COUNT {}."

  def labelIdentifyingAndImplied(labels: List[String], position: InputPosition): SemanticError = SemanticError(
    ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NC4)
          .withParam(GqlParams.ListParam.labelList, labels.asJava)
          .atPosition(position.offset, position.line, position.column)
          .build()
      ).build(),
    s"The label(s) ${labels.map(l => s"`$l`").mkString(", ")} are defined as both identifying and implied.",
    position
  )

  def independentConstraintOnDependentElement(
    description: String,
    elementTypeName: StaticElementTypeName,
    position: InputPosition
  ): SemanticError = {
    val (statusCode, param, nameValue, error) = elementTypeName match {
      case LabelName(name) =>
        (
          GqlStatusInfoCodes.STATUS_22NC6,
          GqlParams.StringParam.label,
          name,
          s"Independent constraints cannot be defined on label `$name`"
        )
      case RelTypeName(name) =>
        (
          GqlStatusInfoCodes.STATUS_22NC7,
          GqlParams.StringParam.relType,
          name,
          s"Independent constraints cannot be defined on relationship type `$name`"
        )
    }

    SemanticError(
      ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
        .atPosition(position.offset, position.line, position.column)
        .withCause(
          ErrorGqlStatusObjectImplementation.from(statusCode)
            .withParam(GqlParams.StringParam.constrDescrOrName, description)
            .withParam(param, nameValue)
            .atPosition(position.offset, position.line, position.column)
            .build()
        ).build(),
      error,
      position
    )
  }

  def duplicateTokensInGraphType(token: String, position: InputPosition): SemanticError = {
    SemanticError(
      ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
        .atPosition(position.offset, position.line, position.column)
        .withCause(
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NC8)
            .withParam(GqlParams.StringParam.token, token)
            .atPosition(position.offset, position.line, position.column)
            .build()
        ).build(),
      s"graph type contains duplicated tokens '$token'",
      position
    )
  }

  def invalidIndexParameter(position: InputPosition): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42I04("Parameter", "VECTOR INDEX", position.offset, position.line, position.column),
      "Parameter cannot be used in a VECTOR INDEX clause.",
      position
    )
  }

  def searchWithVariableFromOutsideMatch(bindingVariable: LogicalVariable): SemanticError = {
    val pos = bindingVariable.position
    SemanticError(
      GqlHelper.getGql42001_42I69(bindingVariable.name, pos.offset, pos.line, pos.column),
      s"The variable `${bindingVariable.name}` in SEARCH must reference a variable from the same MATCH statement.",
      pos
    )
  }

  def searchWithMultipleBoundVariables(position: InputPosition): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42I70(position.offset, position.line, position.column),
      "In order to have a search clause, a MATCH statement can only have one bound variable.",
      position
    )
  }

  def searchWithInvalidPredicates(position: InputPosition): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42I71(position.offset, position.line, position.column),
      "In order to have a search clause, a MATCH statement can only have predicates on the bound variable.",
      position
    )
  }

  def searchWithTooComplexMatch(position: InputPosition): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42I72(position.offset, position.line, position.column),
      "In order to have a search clause, a MATCH statement can only have a single node or relationship pattern and no selectors.",
      position
    )
  }

  def singleStageWithInvalidPredicate(expr: Expression, position: InputPosition): SemanticError = {
    val stringifier = ExpressionStringifier()
    val exprString = And.flatten(expr).map(stringifier.apply).mkString(" AND ")
    SemanticError(
      GqlHelper.getGql42001_42I73(exprString, position.offset, position.line, position.column),
      s"The vector search filter predicate '$exprString' must consist of one or more property predicates joined by AND, and the combined property predicates for each property must specify either an exact value (e.g. x.prop = 1), a half-bounded range (e.g. x.prop >= 1), or a bounded range (e.g. x.prop > 1 AND x.prop < 100). Note that this is not an exhaustive list of valid predicates, see documentation for all rules.",
      position
    )
  }

  def singleStageWithInvalidVariable(variable1: String, variable2: String, position: InputPosition): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42I74(variable1, variable2, position.offset, position.line, position.column),
      s"The variable `$variable1` in a vector search filter property predicate must be the same as the search clause binding variable `$variable2`.",
      position
    )
  }

  def singleStageWithPredicateReferencingEntity(
    expression: String,
    variable: String,
    position: InputPosition
  ): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42I75(expression, variable, position.offset, position.line, position.column),
      s"Vector search filter predicate referencing the search binding variable",
      position
    )
  }

  def singleStageWithEmbeddingReferencingEntity(
    expression: String,
    variable: String,
    position: InputPosition
  ): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42I75(expression, variable, position.offset, position.line, position.column),
      s"Vector search query vector referencing the search binding variable",
      position
    )
  }

  def localCallableAlreadyDefined(
    name: String,
    position: InputPosition
  ): SemanticError = {
    SemanticError(
      GqlHelper.getGql42001_42I77(name, position.offset, position.line, position.column),
      s"Local procedure $name is already defined",
      position
    )
  }

  def duplicateParameter(parameter: String, pos: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N67(parameter, pos.offset, pos.line, pos.column)
    SemanticError(
      gql,
      s"Duplicate parameter $parameter in local callable signature",
      pos
    )
  }
}

final case class FeatureError(
  override val gqlStatusObject: ErrorGqlStatusObject,
  override val msg: String,
  feature: SemanticFeature,
  override val position: InputPosition
) extends SemanticErrorDef {

  override def withMsg(error: SemanticError): FeatureError = copy(msg = error.msg)
}

object FeatureError {

  def unapply(errorDef: FeatureError): Option[(ErrorGqlStatusObject, String, SemanticFeature, InputPosition)] =
    Some((errorDef.gqlStatusObject, errorDef.msg, errorDef.feature, errorDef.position))

  def notAvailableInThisImplementation(feature: SemanticFeature, msg: String, position: InputPosition): FeatureError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N26)
      .withParam(GqlParams.StringParam.item, msg)
      .withParam(GqlParams.StringParam.feat, feature.toString)
      .atPosition(position.offset, position.line, position.column)
      .build()
    FeatureError(
      gql,
      s"$msg is not available in this implementation of Cypher " +
        s"due to lack of support for $feature.",
      feature,
      position
    )
  }
}
